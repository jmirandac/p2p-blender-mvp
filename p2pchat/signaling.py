"""Persistent WebSocket registry and signaling for aiortc peers."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Literal

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed


NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
ConnectionStatus = Literal["connected", "disconnected"]
ActivityState = Literal[
    "waiting", "inviting", "deciding", "negotiating", "chatting"
]
SessionPhase = Literal["pending", "negotiating", "chatting"]


class ProtocolError(ValueError):
    """A recoverable error caused by a client protocol message."""

    def __init__(
        self, code: str, message: str, *, request_id: str | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = request_id


@dataclass
class Peer:
    peer_id: str
    name: str
    websocket: ServerConnection | None
    connection_status: ConnectionStatus = "connected"
    activity_state: ActivityState | None = "waiting"
    session_id: str | None = None
    connected_at: float = field(default_factory=time.time)
    disconnected_at: float | None = None
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class Session:
    session_id: str
    requester_id: str
    target_id: str
    request_id: str
    phase: SessionPhase = "pending"
    ready_peers: set[str] = field(default_factory=set)
    description_stage: Literal["offer", "answer", "done"] = "offer"
    timeout_task: asyncio.Task[None] | None = None

    def other_id(self, peer_id: str) -> str:
        if peer_id == self.requester_id:
            return self.target_id
        if peer_id == self.target_id:
            return self.requester_id
        raise ProtocolError("not-in-session", "el peer no pertenece a esta sesión")


class SignalingServer:
    """Register peers, broker consent, and relay WebRTC descriptions."""

    def __init__(
        self,
        *,
        invite_timeout: float = 15,
        negotiation_timeout: float = 30,
        registration_timeout: float = 10,
    ) -> None:
        if min(invite_timeout, negotiation_timeout, registration_timeout) <= 0:
            raise ValueError("los timeouts deben ser positivos")
        self.invite_timeout = invite_timeout
        self.negotiation_timeout = negotiation_timeout
        self.registration_timeout = registration_timeout
        self.peers: dict[str, Peer] = {}
        self.peer_by_websocket: dict[ServerConnection, str] = {}
        self.sessions: dict[str, Session] = {}
        self.lock = asyncio.Lock()

    @staticmethod
    def _decode(raw: str | bytes) -> dict[str, Any]:
        if not isinstance(raw, str):
            raise ProtocolError("invalid-message", "se esperaba un mensaje de texto")
        if len(raw.encode("utf-8")) > 1_000_000:
            raise ProtocolError("message-too-large", "mensaje demasiado grande")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProtocolError("invalid-json", "JSON inválido") from exc
        if not isinstance(value, dict):
            raise ProtocolError("invalid-message", "se esperaba un objeto JSON")
        return value

    @staticmethod
    def _required_token(message: dict[str, Any], field_name: str) -> str:
        value = message.get(field_name)
        if not isinstance(value, str) or not TOKEN_PATTERN.fullmatch(value):
            raise ProtocolError("invalid-field", f"campo {field_name!r} inválido")
        return value

    @staticmethod
    async def _send(peer: Peer, message: dict[str, Any]) -> None:
        websocket = peer.websocket
        if websocket is None:
            raise RuntimeError("peer desconectado")
        async with peer.send_lock:
            await websocket.send(json.dumps(message, separators=(",", ":")))

    async def _send_many(
        self, deliveries: list[tuple[Peer, dict[str, Any]]]
    ) -> None:
        if deliveries:
            await asyncio.gather(
                *(self._send(peer, message) for peer, message in deliveries),
                return_exceptions=True,
            )

    async def _send_error(self, peer: Peer, error: ProtocolError) -> None:
        message: dict[str, Any] = {
            "type": "error",
            "code": error.code,
            "message": str(error),
        }
        if error.request_id is not None:
            message["request_id"] = error.request_id
        await self._send(peer, message)

    def _new_id(self, prefix: str) -> str:
        while True:
            value = f"{prefix}_{secrets.token_urlsafe(9)}"
            if value not in self.peers and value not in self.sessions:
                return value

    async def _register(self, websocket: ServerConnection) -> Peer:
        raw = await asyncio.wait_for(websocket.recv(), timeout=self.registration_timeout)
        request = self._decode(raw)
        if request.get("type") != "register":
            raise ProtocolError(
                "registration-required", "el primer mensaje debe ser register"
            )
        name = request.get("name")
        if not isinstance(name, str) or not NAME_PATTERN.fullmatch(name):
            raise ProtocolError(
                "invalid-name",
                "el nombre debe tener entre 1 y 64 caracteres alfanuméricos, '_' o '-'",
            )

        async with self.lock:
            peer_id = self._new_id("peer")
            peer = Peer(peer_id=peer_id, name=name, websocket=websocket)
            self.peers[peer_id] = peer
            self.peer_by_websocket[websocket] = peer_id
        await self._send(
            peer,
            {"type": "registered", "peer": {"id": peer.peer_id, "name": peer.name}},
        )
        return peer

    async def handle(self, websocket: ServerConnection) -> None:
        peer: Peer | None = None
        try:
            try:
                peer = await self._register(websocket)
            except (ProtocolError, asyncio.TimeoutError) as exc:
                error = (
                    ProtocolError("registration-timeout", "tiempo de registro agotado")
                    if isinstance(exc, asyncio.TimeoutError)
                    else exc
                )
                temporary = Peer("", "", websocket)
                try:
                    await self._send_error(temporary, error)
                except ConnectionClosed:
                    pass
                await websocket.close(code=1008, reason=error.code)
                return

            async for raw in websocket:
                try:
                    message = self._decode(raw)
                    should_close = await self._dispatch(peer, message)
                    if should_close:
                        await websocket.close(code=1000, reason="disconnect")
                        break
                except ProtocolError as exc:
                    try:
                        await self._send_error(peer, exc)
                    except ConnectionClosed:
                        break
        except ConnectionClosed:
            pass
        finally:
            if peer is not None:
                await self._remove_peer(peer)

    async def _dispatch(self, peer: Peer, message: dict[str, Any]) -> bool:
        message_type = message.get("type")
        if message_type == "list-peers":
            await self._list_peers(peer, message)
        elif message_type == "connect-request":
            await self._connect_request(peer, message)
        elif message_type == "connection-response":
            await self._connection_response(peer, message)
        elif message_type == "description":
            await self._relay_description(peer, message)
        elif message_type == "chat-ready":
            await self._chat_ready(peer, message)
        elif message_type == "chat-end":
            await self._chat_end(peer, message)
        elif message_type == "disconnect":
            await self._send(peer, {"type": "disconnected"})
            return True
        else:
            raise ProtocolError("unknown-command", "comando desconocido")
        return False

    async def _list_peers(self, peer: Peer, message: dict[str, Any]) -> None:
        request_id = self._required_token(message, "request_id")
        async with self.lock:
            peers = [
                {
                    "id": candidate.peer_id,
                    "name": candidate.name,
                    "availability": (
                        "waiting" if candidate.activity_state == "waiting" else "busy"
                    ),
                }
                for candidate in self.peers.values()
                if candidate.peer_id != peer.peer_id
                and candidate.connection_status == "connected"
            ]
        peers.sort(key=lambda item: (item["name"].lower(), item["id"]))
        await self._send(
            peer,
            {"type": "peer-list", "request_id": request_id, "peers": peers},
        )

    async def _connect_request(self, peer: Peer, message: dict[str, Any]) -> None:
        request_id = self._required_token(message, "request_id")
        target_id = self._required_token(message, "target_id")
        failure: str | None = None
        target: Peer | None = None
        session: Session | None = None

        async with self.lock:
            if peer.activity_state != "waiting":
                failure = "busy"
            elif target_id == peer.peer_id:
                failure = "self"
            else:
                target = self.peers.get(target_id)
                if target is None or target.connection_status != "connected":
                    failure = "unavailable"
                elif target.activity_state != "waiting":
                    failure = "busy"
                else:
                    session_id = self._new_id("session")
                    session = Session(
                        session_id=session_id,
                        requester_id=peer.peer_id,
                        target_id=target.peer_id,
                        request_id=request_id,
                    )
                    self.sessions[session_id] = session
                    peer.activity_state = "inviting"
                    peer.session_id = session_id
                    target.activity_state = "deciding"
                    target.session_id = session_id
                    session.timeout_task = asyncio.create_task(
                        self._expire_invitation(session_id)
                    )

        if failure is not None:
            await self._send(
                peer,
                {
                    "type": "connection-result",
                    "request_id": request_id,
                    "status": failure,
                },
            )
            return
        assert target is not None and session is not None
        await self._send_many(
            [
                (
                    peer,
                    {
                        "type": "connect-pending",
                        "request_id": request_id,
                        "session_id": session.session_id,
                    },
                ),
                (
                    target,
                    {
                        "type": "connection-request",
                        "session_id": session.session_id,
                        "peer": {"id": peer.peer_id, "name": peer.name},
                        "expires_in": self.invite_timeout,
                    },
                ),
            ]
        )

    async def _connection_response(self, peer: Peer, message: dict[str, Any]) -> None:
        session_id = self._required_token(message, "session_id")
        accepted = message.get("accepted")
        if not isinstance(accepted, bool):
            raise ProtocolError("invalid-field", "campo 'accepted' inválido")

        deliveries: list[tuple[Peer, dict[str, Any]]] = []
        async with self.lock:
            session = self.sessions.get(session_id)
            if (
                session is None
                or session.phase != "pending"
                or session.target_id != peer.peer_id
            ):
                raise ProtocolError("stale-session", "invitación inexistente o expirada")
            requester = self.peers[session.requester_id]
            target = self.peers[session.target_id]
            self._cancel_timeout_locked(session)
            if not accepted:
                self._detach_session_locked(session)
                result = {
                    "type": "connection-result",
                    "request_id": session.request_id,
                    "session_id": session_id,
                    "status": "rejected",
                }
                deliveries = [(requester, result), (target, result)]
            else:
                session.phase = "negotiating"
                requester.activity_state = "negotiating"
                target.activity_state = "negotiating"
                session.timeout_task = asyncio.create_task(
                    self._expire_negotiation(session_id)
                )
                deliveries = [
                    (
                        requester,
                        {
                            "type": "matched",
                            "session_id": session_id,
                            "peer": {"id": target.peer_id, "name": target.name},
                            "initiator": True,
                        },
                    ),
                    (
                        target,
                        {
                            "type": "matched",
                            "session_id": session_id,
                            "peer": {"id": requester.peer_id, "name": requester.name},
                            "initiator": False,
                        },
                    ),
                ]
        await self._send_many(deliveries)

    @staticmethod
    def _validated_description(message: dict[str, Any]) -> dict[str, str]:
        description = message.get("description")
        if not isinstance(description, dict):
            raise ProtocolError("invalid-description", "descripción WebRTC inválida")
        kind = description.get("type")
        sdp = description.get("sdp")
        if kind not in ("offer", "answer") or not isinstance(sdp, str) or not sdp:
            raise ProtocolError("invalid-description", "descripción WebRTC inválida")
        return {"type": kind, "sdp": sdp}

    async def _relay_description(self, peer: Peer, message: dict[str, Any]) -> None:
        session_id = self._required_token(message, "session_id")
        description = self._validated_description(message)
        async with self.lock:
            session = self.sessions.get(session_id)
            if session is None or session.phase != "negotiating":
                raise ProtocolError("stale-session", "negociación inexistente o finalizada")
            if session.description_stage == "done":
                raise ProtocolError("invalid-description-state", "la señalización ya terminó")
            expected_sender = (
                session.requester_id
                if session.description_stage == "offer"
                else session.target_id
            )
            expected_type = (
                "offer" if session.description_stage == "offer" else "answer"
            )
            if peer.peer_id != expected_sender or description["type"] != expected_type:
                raise ProtocolError(
                    "invalid-description-state", "descripción fuera de secuencia"
                )
            remote = self.peers[session.other_id(peer.peer_id)]
            session.description_stage = (
                "answer" if session.description_stage == "offer" else "done"
            )
        await self._send(
            remote,
            {
                "type": "description",
                "session_id": session_id,
                "description": description,
            },
        )

    async def _chat_ready(self, peer: Peer, message: dict[str, Any]) -> None:
        session_id = self._required_token(message, "session_id")
        deliveries: list[tuple[Peer, dict[str, Any]]] = []
        async with self.lock:
            session = self.sessions.get(session_id)
            if session is None:
                raise ProtocolError("stale-session", "sesión inexistente o finalizada")
            session.other_id(peer.peer_id)
            if session.phase == "chatting":
                return
            if session.phase != "negotiating" or session.description_stage != "done":
                raise ProtocolError("invalid-state", "la negociación aún no está completa")
            session.ready_peers.add(peer.peer_id)
            if session.ready_peers == {session.requester_id, session.target_id}:
                session.phase = "chatting"
                self._cancel_timeout_locked(session)
                requester = self.peers[session.requester_id]
                target = self.peers[session.target_id]
                requester.activity_state = "chatting"
                target.activity_state = "chatting"
                event = {"type": "chat-started", "session_id": session_id}
                deliveries = [(requester, event), (target, event)]
        await self._send_many(deliveries)

    async def _chat_end(self, peer: Peer, message: dict[str, Any]) -> None:
        session_id = self._required_token(message, "session_id")
        reason = message.get("reason", "left")
        if not isinstance(reason, str) or not 1 <= len(reason) <= 64:
            raise ProtocolError("invalid-field", "campo 'reason' inválido")
        async with self.lock:
            session = self.sessions.get(session_id)
            if session is None:
                return
            session.other_id(peer.peer_id)
            participants = self._detach_session_locked(session)
        event = {
            "type": "session-ended",
            "session_id": session_id,
            "reason": reason,
        }
        await self._send_many([(participant, event) for participant in participants])

    async def _expire_invitation(self, session_id: str) -> None:
        try:
            await asyncio.sleep(self.invite_timeout)
            async with self.lock:
                session = self.sessions.get(session_id)
                if session is None or session.phase != "pending":
                    return
                requester = self.peers[session.requester_id]
                target = self.peers[session.target_id]
                self._detach_session_locked(session, cancel_timeout=False)
            await self._send_many(
                [
                    (
                        requester,
                        {
                            "type": "connection-result",
                            "request_id": session.request_id,
                            "session_id": session_id,
                            "status": "timeout",
                        },
                    ),
                    (
                        target,
                        {"type": "invitation-expired", "session_id": session_id},
                    ),
                ]
            )
        except asyncio.CancelledError:
            pass

    async def _expire_negotiation(self, session_id: str) -> None:
        try:
            await asyncio.sleep(self.negotiation_timeout)
            async with self.lock:
                session = self.sessions.get(session_id)
                if session is None or session.phase != "negotiating":
                    return
                participants = self._detach_session_locked(
                    session, cancel_timeout=False
                )
            event = {
                "type": "session-ended",
                "session_id": session_id,
                "reason": "negotiation-timeout",
            }
            await self._send_many(
                [(participant, event) for participant in participants]
            )
        except asyncio.CancelledError:
            pass

    def _cancel_timeout_locked(self, session: Session) -> None:
        task = session.timeout_task
        session.timeout_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    def _detach_session_locked(
        self, session: Session, *, cancel_timeout: bool = True
    ) -> list[Peer]:
        if self.sessions.pop(session.session_id, None) is None:
            return []
        if cancel_timeout:
            self._cancel_timeout_locked(session)
        participants: list[Peer] = []
        for peer_id in (session.requester_id, session.target_id):
            participant = self.peers.get(peer_id)
            if participant is None:
                continue
            if participant.session_id == session.session_id:
                participant.session_id = None
                if participant.connection_status == "connected":
                    participant.activity_state = "waiting"
            if participant.connection_status == "connected":
                participants.append(participant)
        return participants

    async def _remove_peer(self, peer: Peer) -> None:
        deliveries: list[tuple[Peer, dict[str, Any]]] = []
        async with self.lock:
            if peer.connection_status == "disconnected":
                return
            self.peer_by_websocket.pop(peer.websocket, None)
            peer.websocket = None
            peer.connection_status = "disconnected"
            peer.activity_state = None
            peer.disconnected_at = time.time()
            session = self.sessions.get(peer.session_id or "")
            if session is not None:
                session_id = session.session_id
                participants = self._detach_session_locked(session)
                event = {
                    "type": "session-ended",
                    "session_id": session_id,
                    "reason": "peer-disconnected",
                }
                deliveries = [
                    (participant, event)
                    for participant in participants
                    if participant.peer_id != peer.peer_id
                ]
            peer.session_id = None
        await self._send_many(deliveries)


async def run_server(
    host: str,
    port: int,
    *,
    heartbeat_interval: float = 10,
    heartbeat_timeout: float = 20,
    invite_timeout: float = 15,
    negotiation_timeout: float = 30,
) -> None:
    app = SignalingServer(
        invite_timeout=invite_timeout,
        negotiation_timeout=negotiation_timeout,
    )
    async with serve(
        app.handle,
        host,
        port,
        max_size=1_000_000,
        ping_interval=heartbeat_interval,
        ping_timeout=heartbeat_timeout,
    ) as server:
        addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        print(f"Servidor de señalización WebSocket escuchando en {addresses}")
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Señalización WebSocket para el chat P2P")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--heartbeat-interval", type=float, default=10)
    parser.add_argument("--heartbeat-timeout", type=float, default=20)
    parser.add_argument("--invite-timeout", type=float, default=15)
    parser.add_argument("--negotiation-timeout", type=float, default=30)
    args = parser.parse_args()
    try:
        asyncio.run(
            run_server(
                args.host,
                args.port,
                heartbeat_interval=args.heartbeat_interval,
                heartbeat_timeout=args.heartbeat_timeout,
                invite_timeout=args.invite_timeout,
                negotiation_timeout=args.negotiation_timeout,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
