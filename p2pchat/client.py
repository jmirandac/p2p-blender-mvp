"""Terminal chat backed by an aiortc WebRTC data channel."""

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


class WebRTCChat:
    """Own a peer connection and expose a small text-chat interface."""

    def __init__(
        self,
        peer_id: str,
        remote_id: str,
        *,
        stun_url: str = DEFAULT_STUN_URL,
        on_message: Callable[[str], None] | None = None,
    ) -> None:
        configuration = RTCConfiguration(
            iceServers=[RTCIceServer(urls=stun_url)] if stun_url else []
        )
        self.peer_id = peer_id
        self.remote_id = remote_id
        self.connection = RTCPeerConnection(configuration=configuration)
        self.channel: RTCDataChannel | None = None
        self.ready = asyncio.Event()
        self.closed = asyncio.Event()
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
                print(f"\n{self.remote_id} ha cerrado el chat.")
                self.closed.set()

        # aiortc emits "datachannel" once the remotely-created channel is open,
        # so the answerer may attach its handlers after the "open" event.
        if channel.readyState == "open":
            self.ready.set()

    async def create_offer(self) -> RTCSessionDescription:
        self._attach_channel(self.connection.createDataChannel("chat"))
        await self.connection.setLocalDescription(await self.connection.createOffer())
        assert self.connection.localDescription is not None
        return self.connection.localDescription

    async def accept_offer(
        self, description: RTCSessionDescription
    ) -> RTCSessionDescription:
        await self.connection.setRemoteDescription(description)
        await self.connection.setLocalDescription(await self.connection.createAnswer())
        assert self.connection.localDescription is not None
        return self.connection.localDescription

    async def accept_answer(self, description: RTCSessionDescription) -> None:
        await self.connection.setRemoteDescription(description)

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


async def _send_description(
    websocket: ClientConnection, description: RTCSessionDescription
) -> None:
    await websocket.send(
        json.dumps(
            {
                "type": "description",
                "description": {"sdp": description.sdp, "type": description.type},
            },
            separators=(",", ":"),
        )
    )


async def _receive_json(websocket: ClientConnection) -> dict[str, Any]:
    raw = await websocket.recv()
    if not isinstance(raw, str):
        raise RuntimeError("respuesta de señalización no textual")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise RuntimeError("respuesta de señalización inválida")
    return value


async def _receive_until_ready(
    websocket: ClientConnection, chat: WebRTCChat
) -> dict[str, Any] | None:
    receive_task = asyncio.create_task(_receive_json(websocket))
    ready_task = asyncio.create_task(chat.ready.wait())
    closed_task = asyncio.create_task(chat.closed.wait())
    tasks = {receive_task, ready_task, closed_task}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    for task in pending:
        with contextlib.suppress(asyncio.CancelledError):
            await task

    if receive_task in done:
        return receive_task.result()
    if ready_task in done and chat.ready.is_set():
        return None
    raise RuntimeError("la conexión WebRTC se cerró durante la negociación")


async def match_and_connect(
    websocket: ClientConnection,
    room: str,
    peer_id: str,
    stun_url: str,
) -> WebRTCChat:
    await websocket.send(
        json.dumps(
            {"type": "join", "room": room, "peer_id": peer_id},
            separators=(",", ":"),
        )
    )

    while True:
        message = await _receive_json(websocket)
        message_type = message.get("type")
        if message_type == "waiting":
            print("Esperando al segundo participante…")
        elif message_type == "matched":
            remote_id = message.get("peer_id")
            if not isinstance(remote_id, str):
                raise RuntimeError("respuesta matched inválida")
            initiator = message.get("initiator") is True
            break
        elif message_type == "error":
            raise RuntimeError(str(message.get("message", "error de señalización")))

    chat = WebRTCChat(peer_id, remote_id, stun_url=stun_url)
    try:
        if initiator:
            await _send_description(websocket, await chat.create_offer())

        while not chat.ready.is_set():
            message = await _receive_until_ready(websocket, chat)
            if message is None:
                break
            message_type = message.get("type")
            if message_type == "description":
                description = _description_from_message(message)
                if description.type == "offer":
                    await _send_description(
                        websocket, await chat.accept_offer(description)
                    )
                elif description.type == "answer":
                    await chat.accept_answer(description)
            elif message_type == "peer-left":
                raise RuntimeError("el otro peer se desconectó durante la negociación")
            elif message_type == "error":
                raise RuntimeError(
                    str(message.get("message", "error de señalización"))
                )

            if chat.connection.connectionState == "failed":
                raise RuntimeError("falló la negociación ICE")
    except BaseException:
        await chat.close(notify=False)
        raise

    return chat


async def run(args: argparse.Namespace) -> int:
    scheme = "wss" if args.secure else "ws"
    uri = f"{scheme}://{args.server}:{args.port}"
    print(f"Uniéndose a la sala {args.room!r} como {args.name!r}…")

    chat: WebRTCChat | None = None
    try:
        async with connect(uri, open_timeout=10) as websocket:
            chat = await asyncio.wait_for(
                match_and_connect(websocket, args.room, args.name, args.stun_server),
                timeout=args.connect_timeout,
            )
            print(
                f"Conexión WebRTC P2P con {chat.remote_id} establecida. "
                "Escribe /salir para terminar."
            )
            while not chat.closed.is_set():
                text = await asyncio.to_thread(input, "yo> ")
                if text.strip() == "/salir":
                    break
                if text:
                    chat.send_chat(text)
    except asyncio.TimeoutError:
        print("No se pudo establecer la conexión WebRTC dentro del tiempo límite.")
        return 2
    except (ConnectionClosed, OSError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"Error de conexión: {exc}")
        return 1
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        if chat is not None:
            await chat.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Chat P2P WebRTC de terminal")
    parser.add_argument("--server", default="127.0.0.1", help="host de señalización")
    parser.add_argument("--port", type=int, default=9000, help="puerto WebSocket")
    parser.add_argument("--room", required=True, help="código compartido de la sala")
    parser.add_argument("--name", default=f"peer-{secrets.token_hex(3)}", help="nombre visible")
    parser.add_argument(
        "--stun-server", default=DEFAULT_STUN_URL, help="URL del servidor STUN para ICE"
    )
    parser.add_argument(
        "--connect-timeout", type=float, default=30, help="límite de negociación en segundos"
    )
    parser.add_argument("--secure", action="store_true", help="usar WSS para señalización")
    raise SystemExit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
