"""One-shot room matching server. It never relays chat messages."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import re
from dataclasses import dataclass

from .wire import read_message, write_message


ROOM_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@dataclass
class WaitingPeer:
    peer_id: str
    public_endpoint: list[object]
    local_endpoint: list[object]
    writer: asyncio.StreamWriter

    def description(self) -> dict[str, object]:
        return {
            "peer_id": self.peer_id,
            "public_endpoint": self.public_endpoint,
            "local_endpoint": self.local_endpoint,
        }


class SignalingServer:
    def __init__(self) -> None:
        self.waiting: dict[str, WaitingPeer] = {}
        self.lock = asyncio.Lock()

    @staticmethod
    def _endpoint(value: object, name: str) -> list[object]:
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError(f"{name} no es un endpoint válido")
        host, port = value
        if not isinstance(host, str) or not isinstance(port, int) or not (1 <= port <= 65535):
            raise ValueError(f"{name} no es un endpoint válido")
        return [host, port]

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer: WaitingPeer | None = None
        room = ""
        try:
            request = await asyncio.wait_for(read_message(reader), timeout=10)
            if request.get("type") != "join":
                raise ValueError("el primer mensaje debe ser join")
            room = request.get("room", "")
            peer_id = request.get("peer_id", "")
            if not isinstance(room, str) or not ROOM_PATTERN.fullmatch(room):
                raise ValueError("código de sala inválido")
            if not isinstance(peer_id, str) or not ROOM_PATTERN.fullmatch(peer_id):
                raise ValueError("identificador de peer inválido")

            peer = WaitingPeer(
                peer_id=peer_id,
                public_endpoint=self._endpoint(request.get("public_endpoint"), "public_endpoint"),
                local_endpoint=self._endpoint(request.get("local_endpoint"), "local_endpoint"),
                writer=writer,
            )

            async with self.lock:
                other = self.waiting.pop(room, None)
                if other is None or other.writer.is_closing():
                    self.waiting[room] = peer
                    other = None

            if other is None:
                await write_message(writer, {"type": "waiting"})
                await reader.read()
                return

            if other.peer_id == peer.peer_id:
                raise ValueError("los peers deben usar identificadores diferentes")

            await write_message(other.writer, {"type": "matched", "peer": peer.description()})
            await write_message(writer, {"type": "matched", "peer": other.description()})
            other.writer.close()
            with contextlib.suppress(ConnectionError):
                await other.writer.wait_closed()
        except (EOFError, ValueError, asyncio.TimeoutError) as exc:
            with contextlib.suppress(ConnectionError):
                await write_message(writer, {"type": "error", "message": str(exc)})
        finally:
            if peer is not None:
                async with self.lock:
                    if self.waiting.get(room) is peer:
                        self.waiting.pop(room, None)
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()


async def serve(host: str, port: int) -> None:
    app = SignalingServer()
    server = await asyncio.start_server(app.handle, host, port)
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f"Servidor de señalización escuchando en {addresses}")
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor de señalización para el chat P2P")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    try:
        asyncio.run(serve(args.host, args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

