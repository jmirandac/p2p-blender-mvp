"""Room-based WebSocket signaling for aiortc peers."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import Any

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed


ROOM_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@dataclass
class Peer:
    peer_id: str
    room: str
    websocket: ServerConnection
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class SignalingServer:
    """Pair two peers per room and relay only their WebRTC descriptions."""

    def __init__(self) -> None:
        self.waiting: dict[str, Peer] = {}
        self.partners: dict[ServerConnection, Peer] = {}
        self.lock = asyncio.Lock()

    @staticmethod
    def _decode(raw: str | bytes) -> dict[str, Any]:
        if not isinstance(raw, str):
            raise ValueError("se esperaba un mensaje de texto")
        if len(raw.encode("utf-8")) > 1_000_000:
            raise ValueError("mensaje demasiado grande")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("se esperaba un objeto JSON")
        return value

    @staticmethod
    async def _send(peer: Peer, message: dict[str, Any]) -> None:
        async with peer.send_lock:
            await peer.websocket.send(json.dumps(message, separators=(",", ":")))

    async def _join(self, websocket: ServerConnection) -> Peer:
        raw = await asyncio.wait_for(websocket.recv(), timeout=10)
        request = self._decode(raw)
        if request.get("type") != "join":
            raise ValueError("el primer mensaje debe ser join")

        room = request.get("room", "")
        peer_id = request.get("peer_id", "")
        if not isinstance(room, str) or not ROOM_PATTERN.fullmatch(room):
            raise ValueError("código de sala inválido")
        if not isinstance(peer_id, str) or not ROOM_PATTERN.fullmatch(peer_id):
            raise ValueError("identificador de peer inválido")

        peer = Peer(peer_id=peer_id, room=room, websocket=websocket)
        async with self.lock:
            other = self.waiting.pop(room, None)
            if other is not None and other.peer_id == peer_id:
                self.waiting[room] = other
                raise ValueError("los peers deben usar identificadores diferentes")
            if other is None:
                self.waiting[room] = peer
            else:
                self.partners[websocket] = other
                self.partners[other.websocket] = peer

        if other is None:
            await self._send(peer, {"type": "waiting"})
        else:
            await asyncio.gather(
                self._send(
                    other,
                    {"type": "matched", "peer_id": peer.peer_id, "initiator": True},
                ),
                self._send(
                    peer,
                    {"type": "matched", "peer_id": other.peer_id, "initiator": False},
                ),
            )
        return peer

    @staticmethod
    def _validated_description(message: dict[str, Any]) -> dict[str, str]:
        description = message.get("description")
        if not isinstance(description, dict):
            raise ValueError("descripción WebRTC inválida")
        kind = description.get("type")
        sdp = description.get("sdp")
        if kind not in ("offer", "answer") or not isinstance(sdp, str) or not sdp:
            raise ValueError("descripción WebRTC inválida")
        return {"type": kind, "sdp": sdp}

    async def handle(self, websocket: ServerConnection) -> None:
        peer: Peer | None = None
        try:
            peer = await self._join(websocket)
            async for raw in websocket:
                message = self._decode(raw)
                if message.get("type") != "description":
                    raise ValueError("solo se pueden retransmitir descripciones WebRTC")
                description = self._validated_description(message)
                async with self.lock:
                    partner = self.partners.get(websocket)
                if partner is None:
                    raise ValueError("el peer aún no está emparejado")
                await self._send(
                    partner, {"type": "description", "description": description}
                )
        except (ConnectionClosed, asyncio.TimeoutError):
            pass
        except (json.JSONDecodeError, ValueError) as exc:
            if peer is None:
                peer = Peer(peer_id="", room="", websocket=websocket)
            try:
                await self._send(peer, {"type": "error", "message": str(exc)})
            except ConnectionClosed:
                pass
        finally:
            if peer is not None:
                await self._remove(peer)

    async def _remove(self, peer: Peer) -> None:
        async with self.lock:
            if self.waiting.get(peer.room) is peer:
                self.waiting.pop(peer.room, None)
            partner = self.partners.pop(peer.websocket, None)
            if partner is not None:
                self.partners.pop(partner.websocket, None)
        if partner is not None:
            try:
                await self._send(partner, {"type": "peer-left"})
            except ConnectionClosed:
                pass


async def run_server(host: str, port: int) -> None:
    app = SignalingServer()
    async with serve(app.handle, host, port, max_size=1_000_000) as server:
        addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        print(f"Servidor de señalización WebSocket escuchando en {addresses}")
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Señalización WebSocket para el chat P2P")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    try:
        asyncio.run(run_server(args.host, args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
