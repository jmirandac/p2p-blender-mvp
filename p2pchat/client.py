"""Persistent signaling client and terminal chat backed by aiortc."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import secrets
from collections.abc import Callable
from typing import Any

from aiortc import (
    RTCConfiguration,
    RTCDataChannel,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
)
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed


DEFAULT_STUN_URL = "stun:stun.l.google.com:19302"
DEFAULT_ICE_GATHER_TIMEOUT = 0.5


class WebRTCChat:
    """Own a peer connection and expose a small text-chat interface."""

    def __init__(
        self,
        peer_id: str,
        remote_id: str,
        *,
        stun_url: str = DEFAULT_STUN_URL,
        ice_gather_timeout: float | None = DEFAULT_ICE_GATHER_TIMEOUT,
        on_message: Callable[[str], None] | None = None,
    ) -> None:
        if ice_gather_timeout is not None and ice_gather_timeout <= 0:
            raise ValueError("el timeout de recolección ICE debe ser positivo")
        configuration = RTCConfiguration(
            iceServers=[RTCIceServer(urls=stun_url)] if stun_url else []
        )
        self.peer_id = peer_id
        self.remote_id = remote_id
        self.connection = RTCPeerConnection(configuration=configuration)
        self.channel: RTCDataChannel | None = None
        self.ready = asyncio.Event()
        self.closed = asyncio.Event()
        self.ice_gather_timeout = ice_gather_timeout
        self._uses_ice_server = bool(stun_url)
        self._ice_gathering_limited = False
        self._on_message = on_message or self._print_message

        @self.connection.on("datachannel")
        def on_datachannel(channel: RTCDataChannel) -> None:
            self._attach_channel(channel)

        @self.connection.on("connectionstatechange")
        async def on_connectionstatechange() -> None:
            if self.connection.connectionState in ("closed", "failed"):
                self.closed.set()

    def _print_message(self, text: str) -> None:
        print(f"\r{self.remote_id}> {text}\nyo> ", end="", flush=True)

    def _attach_channel(self, channel: RTCDataChannel) -> None:
        self.channel = channel

        @channel.on("open")
        def on_open() -> None:
            self.ready.set()

        @channel.on("close")
        def on_close() -> None:
            self.closed.set()

        @channel.on("message")
        def on_message(payload: str | bytes) -> None:
            try:
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8")
                message = json.loads(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
                return
            if not isinstance(message, dict) or message.get("peer_id") != self.remote_id:
                return
            if message.get("type") == "chat":
                self._on_message(str(message.get("text", "")))
            elif message.get("type") == "bye":
                self.closed.set()

        if channel.readyState == "open":
            self.ready.set()

    async def create_offer(self) -> RTCSessionDescription:
        self._attach_channel(self.connection.createDataChannel("chat"))
        self._limit_ice_gathering()
        await self.connection.setLocalDescription(await self.connection.createOffer())
        assert self.connection.localDescription is not None
        return self.connection.localDescription

    async def accept_offer(
        self, description: RTCSessionDescription
    ) -> RTCSessionDescription:
        await self.connection.setRemoteDescription(description)
        self._limit_ice_gathering()
        await self.connection.setLocalDescription(await self.connection.createAnswer())
        assert self.connection.localDescription is not None
        return self.connection.localDescription

    async def accept_answer(self, description: RTCSessionDescription) -> None:
        await self.connection.setRemoteDescription(description)

    def _limit_ice_gathering(self) -> None:
        """Cap aioice's fixed five-second wait across local interfaces.

        aiortc doesn't expose aioice's candidate-gathering timeout. Applying the
        limit to this peer connection keeps host candidates and any STUN response
        received within the configured window, while avoiding a five-second wait
        for unreachable VPN or virtual interfaces.
        """
        if (
            self._ice_gathering_limited
            or self.ice_gather_timeout is None
            or not self._uses_ice_server
        ):
            return
        sctp = self.connection.sctp
        if sctp is None:
            raise RuntimeError("transporte SCTP no inicializado")
        ice_connection = sctp.transport.transport.iceGatherer._connection
        original = ice_connection.get_component_candidates
        configured_timeout = self.ice_gather_timeout

        async def get_component_candidates(
            component: int, addresses: list[str], timeout: int = 5
        ) -> Any:
            del timeout
            return await original(
                component, addresses, timeout=configured_timeout
            )

        ice_connection.get_component_candidates = get_component_candidates
        self._ice_gathering_limited = True

    def send_chat(self, text: str) -> None:
        if self.channel is None or self.channel.readyState != "open":
            raise RuntimeError("la conexión P2P aún no está lista")
        self.channel.send(
            json.dumps(
                {"type": "chat", "peer_id": self.peer_id, "text": text},
                separators=(",", ":"),
            )
        )

    async def close(self, *, notify: bool = True) -> None:
        if notify and self.channel is not None and self.channel.readyState == "open":
            self.channel.send(
                json.dumps(
                    {"type": "bye", "peer_id": self.peer_id}, separators=(",", ":")
                )
            )
            await asyncio.sleep(0)
        await self.connection.close()
        self.closed.set()


def _description_from_message(message: dict[str, Any]) -> RTCSessionDescription:
    value = message.get("description")
    if not isinstance(value, dict):
        raise RuntimeError("descripción WebRTC inválida")
    sdp = value.get("sdp")
    kind = value.get("type")
    if not isinstance(sdp, str) or kind not in ("offer", "answer"):
        raise RuntimeError("descripción WebRTC inválida")
    return RTCSessionDescription(sdp=sdp, type=kind)


class SignalingClient:
    """Maintain one server connection across invitations and P2P chats."""

    def __init__(
        self,
        websocket: ClientConnection,
        name: str,
        *,
        stun_url: str = DEFAULT_STUN_URL,
        ice_gather_timeout: float | None = DEFAULT_ICE_GATHER_TIMEOUT,
        on_output: Callable[[str], None] | None = None,
        on_chat_message: Callable[[str], None] | None = None,
    ) -> None:
        self.websocket = websocket
        self.name = name
        self.stun_url = stun_url
        self.ice_gather_timeout = ice_gather_timeout
        self.peer_id: str | None = None
        self.state = "registering"
        self.session_id: str | None = None
        self.pending_request_id: str | None = None
        self.remote_peer: dict[str, str] | None = None
        self.chat: WebRTCChat | None = None
        self.events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.disconnected = asyncio.Event()
        self._receiver_task: asyncio.Task[None] | None = None
        self._session_tasks: set[asyncio.Task[None]] = set()
        self._requests: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._send_lock = asyncio.Lock()
        self._output = on_output or print
        self._on_chat_message = on_chat_message

    async def start(self) -> str:
        await self._send({"type": "register", "name": self.name})
        message = await self._receive_json()
        if message.get("type") == "error":
            raise RuntimeError(str(message.get("message", "registro rechazado")))
        value = message.get("peer")
        if message.get("type") != "registered" or not isinstance(value, dict):
            raise RuntimeError("respuesta de registro inválida")
        peer_id = value.get("id")
        if not isinstance(peer_id, str):
            raise RuntimeError("respuesta de registro inválida")
        self.peer_id = peer_id
        self.state = "waiting"
        self._receiver_task = asyncio.create_task(self._receive_loop())
        return peer_id

    async def _send(self, message: dict[str, Any]) -> None:
        async with self._send_lock:
            await self.websocket.send(json.dumps(message, separators=(",", ":")))

    async def _receive_json(self) -> dict[str, Any]:
        raw = await self.websocket.recv()
        if not isinstance(raw, str):
            raise RuntimeError("respuesta de señalización no textual")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError("respuesta de señalización inválida")
        return value

    async def _receive_loop(self) -> None:
        try:
            async for raw in self.websocket:
                if not isinstance(raw, str):
                    raise RuntimeError("respuesta de señalización no textual")
                message = json.loads(raw)
                if not isinstance(message, dict):
                    raise RuntimeError("respuesta de señalización inválida")
                await self._handle_message(message)
                await self.events.put(message)
        except (ConnectionClosed, json.JSONDecodeError, RuntimeError) as exc:
            if not self.disconnected.is_set():
                self._output(f"Conexión de signaling cerrada: {exc}")
        finally:
            self.state = "disconnected"
            self.disconnected.set()
            await self._close_local_chat()

    async def _handle_message(self, message: dict[str, Any]) -> None:
        message_type = message.get("type")
        request_id = message.get("request_id")
        if isinstance(request_id, str):
            future = self._requests.pop(request_id, None)
            if future is not None and not future.done():
                future.set_result(message)

        if message_type == "peer-list":
            return
        if message_type == "connect-pending":
            self.state = "inviting"
            self.pending_request_id = self._message_token(message, "request_id")
            self.session_id = self._message_token(message, "session_id")
            self._output("Invitación enviada; esperando respuesta.")
        elif message_type == "connection-request":
            peer = self._message_peer(message)
            self.state = "deciding"
            self.session_id = self._message_token(message, "session_id")
            self.remote_peer = peer
            self._output(
                f"Invitación de {peer['name']} ({peer['id']}). "
                "Usa /aceptar o /rechazar."
            )
        elif message_type == "connection-result":
            status = str(message.get("status", "unknown"))
            if status != "accepted":
                belongs_to_session = (
                    isinstance(message.get("session_id"), str)
                    and message.get("session_id") == self.session_id
                )
                belongs_to_request = (
                    isinstance(request_id, str)
                    and request_id == self.pending_request_id
                )
                if belongs_to_request:
                    self.pending_request_id = None
                if belongs_to_session or (
                    belongs_to_request and self.state == "inviting"
                ):
                    self.state = "waiting"
                    self.session_id = None
                    self.remote_peer = None
                self._output(f"Invitación finalizada: {status}.")
        elif message_type == "invitation-expired":
            if message.get("session_id") == self.session_id:
                self.state = "waiting"
                self.session_id = None
                self.pending_request_id = None
                self.remote_peer = None
                self._output("La invitación ha expirado.")
        elif message_type == "matched":
            await self._start_negotiation(message)
        elif message_type == "description":
            await self._handle_description(message)
        elif message_type == "chat-started":
            if message.get("session_id") == self.session_id:
                self.state = "chatting"
                assert self.remote_peer is not None
                self._output(
                    f"Chat P2P con {self.remote_peer['name']} establecido. "
                    "Usa /salir para terminarlo."
                )
        elif message_type == "session-ended":
            if message.get("session_id") == self.session_id:
                reason = str(message.get("reason", "closed"))
                self.session_id = None
                self.pending_request_id = None
                self.remote_peer = None
                self.state = "waiting"
                await self._close_local_chat()
                self._output(f"Sesión finalizada ({reason}). De nuevo en espera.")
        elif message_type == "error":
            self._output(
                f"Error [{message.get('code', 'unknown')}]: "
                f"{message.get('message', '')}"
            )
        elif message_type == "disconnected":
            self.state = "disconnected"
            self.disconnected.set()

    @staticmethod
    def _message_token(message: dict[str, Any], field_name: str) -> str:
        value = message.get(field_name)
        if not isinstance(value, str):
            raise RuntimeError(f"respuesta sin {field_name}")
        return value

    @classmethod
    def _message_peer(cls, message: dict[str, Any]) -> dict[str, str]:
        value = message.get("peer")
        if not isinstance(value, dict):
            raise RuntimeError("respuesta sin peer")
        peer_id = value.get("id")
        name = value.get("name")
        if not isinstance(peer_id, str) or not isinstance(name, str):
            raise RuntimeError("peer inválido")
        return {"id": peer_id, "name": name}

    async def _start_negotiation(self, message: dict[str, Any]) -> None:
        if self.peer_id is None:
            raise RuntimeError("cliente no registrado")
        session_id = self._message_token(message, "session_id")
        peer = self._message_peer(message)
        initiator = message.get("initiator") is True
        self.state = "negotiating"
        self.session_id = session_id
        self.pending_request_id = None
        self.remote_peer = peer
        self.chat = WebRTCChat(
            self.peer_id,
            peer["id"],
            stun_url=self.stun_url,
            ice_gather_timeout=self.ice_gather_timeout,
            on_message=self._on_chat_message,
        )
        self._track_session_task(self._watch_chat(self.chat, session_id))
        if initiator:
            await self._send_description(session_id, await self.chat.create_offer())

    async def _handle_description(self, message: dict[str, Any]) -> None:
        session_id = self._message_token(message, "session_id")
        if session_id != self.session_id or self.chat is None:
            raise RuntimeError("descripción para una sesión desconocida")
        description = _description_from_message(message)
        if description.type == "offer":
            answer = await self.chat.accept_offer(description)
            await self._send_description(session_id, answer)
        else:
            await self.chat.accept_answer(description)

    async def _send_description(
        self, session_id: str, description: RTCSessionDescription
    ) -> None:
        await self._send(
            {
                "type": "description",
                "session_id": session_id,
                "description": {"sdp": description.sdp, "type": description.type},
            }
        )

    def _track_session_task(self, coroutine: Any) -> None:
        task = asyncio.create_task(coroutine)
        self._session_tasks.add(task)
        task.add_done_callback(self._session_tasks.discard)

    async def _watch_chat(self, chat: WebRTCChat, session_id: str) -> None:
        ready_task = asyncio.create_task(chat.ready.wait())
        closed_task = asyncio.create_task(chat.closed.wait())
        try:
            done, pending = await asyncio.wait(
                {ready_task, closed_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            if ready_task in done and self.session_id == session_id:
                await self._send({"type": "chat-ready", "session_id": session_id})
                await chat.closed.wait()
            if chat.closed.is_set() and self.session_id == session_id:
                await self._send(
                    {
                        "type": "chat-end",
                        "session_id": session_id,
                        "reason": "channel-closed",
                    }
                )
        except (ConnectionClosed, asyncio.CancelledError):
            pass
        finally:
            for task in (ready_task, closed_task):
                if not task.done():
                    task.cancel()

    async def list_peers(self) -> list[dict[str, str]]:
        request_id = secrets.token_urlsafe(9)
        future = asyncio.get_running_loop().create_future()
        self._requests[request_id] = future
        await self._send({"type": "list-peers", "request_id": request_id})
        message = await future
        peers = message.get("peers")
        if not isinstance(peers, list):
            raise RuntimeError("listado de peers inválido")
        return peers

    async def request_connection(self, target_id: str) -> str:
        if self.state != "waiting":
            raise RuntimeError("el peer no está en espera")
        request_id = secrets.token_urlsafe(9)
        self.state = "inviting"
        self.pending_request_id = request_id
        try:
            await self._send(
                {
                    "type": "connect-request",
                    "request_id": request_id,
                    "target_id": target_id,
                }
            )
        except BaseException:
            self.state = "waiting"
            self.pending_request_id = None
            raise
        return request_id

    async def respond_to_invitation(self, accepted: bool) -> None:
        if self.state != "deciding" or self.session_id is None:
            raise RuntimeError("no hay una invitación pendiente")
        await self._send(
            {
                "type": "connection-response",
                "session_id": self.session_id,
                "accepted": accepted,
            }
        )

    async def end_chat(self, reason: str = "left") -> None:
        if self.session_id is None or self.state not in ("negotiating", "chatting"):
            raise RuntimeError("no hay un chat activo")
        session_id = self.session_id
        await self._send(
            {"type": "chat-end", "session_id": session_id, "reason": reason}
        )
        await self._close_local_chat()

    async def disconnect(self) -> None:
        if self.disconnected.is_set():
            return
        await self._send({"type": "disconnect"})
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.disconnected.wait(), timeout=2)
        await self._close_local_chat()

    async def _close_local_chat(self) -> None:
        chat, self.chat = self.chat, None
        if chat is not None:
            await chat.close(notify=False)

    async def wait_for_event(
        self, event_type: str, *, timeout: float = 5
    ) -> dict[str, Any]:
        while True:
            event = await asyncio.wait_for(self.events.get(), timeout=timeout)
            if event.get("type") == event_type:
                return event

    async def aclose(self) -> None:
        if not self.disconnected.is_set():
            with contextlib.suppress(ConnectionClosed):
                await self.disconnect()
        if self._receiver_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._receiver_task


async def run(args: argparse.Namespace) -> int:
    scheme = "wss" if args.secure else "ws"
    uri = f"{scheme}://{args.server}:{args.port}"
    client: SignalingClient | None = None
    try:
        async with connect(uri, open_timeout=10) as websocket:
            client = SignalingClient(
                websocket,
                args.name,
                stun_url=args.stun_server,
                ice_gather_timeout=args.ice_gather_timeout,
            )
            peer_id = await client.start()
            print(f"Registrado como {args.name!r} con ID {peer_id}.")
            print(
                "Comandos: /peers, /conectar ID, /aceptar, /rechazar, "
                "/salir, /desconectar"
            )
            while not client.disconnected.is_set():
                text = await asyncio.to_thread(input, "yo> ")
                command = text.strip()
                try:
                    if command == "/peers":
                        peers = await client.list_peers()
                        if not peers:
                            print("No hay otros peers conectados.")
                        for peer in peers:
                            print(
                                f"{peer['id']}  {peer['name']}  "
                                f"[{peer['availability']}]"
                            )
                    elif command.startswith("/conectar "):
                        await client.request_connection(command.split(maxsplit=1)[1])
                    elif command == "/aceptar":
                        await client.respond_to_invitation(True)
                    elif command == "/rechazar":
                        await client.respond_to_invitation(False)
                    elif command == "/salir":
                        await client.end_chat()
                    elif command == "/desconectar":
                        await client.disconnect()
                        break
                    elif command.startswith("/"):
                        print("Comando desconocido.")
                    elif command:
                        if client.state != "chatting" or client.chat is None:
                            print("No hay un chat activo.")
                        else:
                            client.chat.send_chat(text)
                except (RuntimeError, KeyError) as exc:
                    print(f"Error: {exc}")
    except (ConnectionClosed, OSError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"Error de conexión: {exc}")
        return 1
    except (EOFError, KeyboardInterrupt):
        if client is not None:
            with contextlib.suppress(ConnectionClosed):
                await client.disconnect()
    finally:
        if client is not None:
            await client._close_local_chat()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Chat P2P WebRTC de terminal")
    parser.add_argument("--server", default="127.0.0.1", help="host de señalización")
    parser.add_argument("--port", type=int, default=9000, help="puerto WebSocket")
    parser.add_argument(
        "--name", default=f"peer-{secrets.token_hex(3)}", help="nombre visible"
    )
    parser.add_argument(
        "--stun-server", default=DEFAULT_STUN_URL, help="URL del servidor STUN para ICE"
    )
    parser.add_argument(
        "--ice-gather-timeout",
        type=float,
        default=DEFAULT_ICE_GATHER_TIMEOUT,
        help="espera máxima de candidatos STUN en segundos",
    )
    parser.add_argument("--secure", action="store_true", help="usar WSS para señalización")
    raise SystemExit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
