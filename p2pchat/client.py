"""Terminal client using STUN, signaling, and direct UDP traffic."""

from __future__ import annotations

import argparse
import asyncio
import json
import secrets
import socket
import threading
import time
from typing import Any

from .stun import StunError, discover_public_endpoint
from .wire import read_message, write_message


def local_ip_for(remote_host: str, remote_port: int) -> str:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect((remote_host, remote_port))
        return probe.getsockname()[0]
    finally:
        probe.close()


async def match_peer(
    server_host: str,
    server_port: int,
    room: str,
    peer_id: str,
    public_endpoint: tuple[str, int],
    local_endpoint: tuple[str, int],
) -> dict[str, Any]:
    reader, writer = await asyncio.open_connection(server_host, server_port)
    try:
        await write_message(
            writer,
            {
                "type": "join",
                "room": room,
                "peer_id": peer_id,
                "public_endpoint": list(public_endpoint),
                "local_endpoint": list(local_endpoint),
            },
        )
        while True:
            message = await read_message(reader)
            if message.get("type") == "waiting":
                print("Esperando al segundo participante…")
            elif message.get("type") == "matched":
                peer = message.get("peer")
                if not isinstance(peer, dict):
                    raise RuntimeError("respuesta matched inválida")
                return peer
            elif message.get("type") == "error":
                raise RuntimeError(str(message.get("message", "error de señalización")))
    finally:
        writer.close()
        await writer.wait_closed()


class DirectChat:
    def __init__(self, udp_socket: socket.socket, peer_id: str, remote: dict[str, Any]) -> None:
        self.socket = udp_socket
        self.peer_id = peer_id
        self.remote_id = str(remote["peer_id"])
        self.candidates = {
            (str(remote["public_endpoint"][0]), int(remote["public_endpoint"][1])),
            (str(remote["local_endpoint"][0]), int(remote["local_endpoint"][1])),
        }
        self.active_endpoint: tuple[str, int] | None = None
        self.connected = threading.Event()
        self.stopped = threading.Event()
        self.lock = threading.Lock()

    def _send(self, message: dict[str, object], endpoint: tuple[str, int]) -> None:
        payload = json.dumps(message, separators=(",", ":")).encode()
        if len(payload) > 4096:
            raise ValueError("mensaje demasiado largo")
        self.socket.sendto(payload, endpoint)

    def receiver(self) -> None:
        self.socket.settimeout(0.5)
        while not self.stopped.is_set():
            try:
                payload, source = self.socket.recvfrom(8192)
                message = json.loads(payload)
                if not isinstance(message, dict) or message.get("peer_id") != self.remote_id:
                    continue
                message_type = message.get("type")
                if message_type in ("punch", "ack"):
                    with self.lock:
                        self.active_endpoint = source
                    self.connected.set()
                    if message_type == "punch":
                        self._send({"type": "ack", "peer_id": self.peer_id}, source)
                elif message_type == "chat":
                    with self.lock:
                        self.active_endpoint = source
                    self.connected.set()
                    text = str(message.get("text", ""))
                    print(f"\r{self.remote_id}> {text}\nyo> ", end="", flush=True)
                elif message_type == "bye":
                    print(f"\n{self.remote_id} ha cerrado el chat.")
                    self.stopped.set()
            except socket.timeout:
                continue
            except (OSError, ValueError, json.JSONDecodeError):
                if not self.stopped.is_set():
                    continue

    def punch(self, timeout: float = 15.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not self.connected.is_set():
            for endpoint in self.candidates:
                try:
                    self._send({"type": "punch", "peer_id": self.peer_id}, endpoint)
                except OSError:
                    pass
            self.connected.wait(0.4)
        return self.connected.is_set()

    def send_chat(self, text: str) -> None:
        with self.lock:
            endpoint = self.active_endpoint
        if endpoint is None:
            raise RuntimeError("la conexión P2P aún no está lista")
        self._send({"type": "chat", "peer_id": self.peer_id, "text": text}, endpoint)

    def close(self) -> None:
        with self.lock:
            endpoint = self.active_endpoint
        if endpoint:
            try:
                self._send({"type": "bye", "peer_id": self.peer_id}, endpoint)
            except OSError:
                pass
        self.stopped.set()
        self.socket.close()


def run(args: argparse.Namespace) -> int:
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(("0.0.0.0", args.udp_port))
    actual_port = udp_socket.getsockname()[1]

    print("Consultando STUN de Google…")
    try:
        public_endpoint = discover_public_endpoint(udp_socket)
    except StunError as exc:
        udp_socket.close()
        print(f"Error: {exc}")
        return 1

    local_endpoint = (local_ip_for(args.server, args.port), actual_port)
    print(f"Endpoint local: {local_endpoint[0]}:{local_endpoint[1]}")
    print(f"Endpoint público según STUN: {public_endpoint[0]}:{public_endpoint[1]}")
    print(f"Uniéndose a la sala {args.room!r} como {args.name!r}…")

    try:
        remote = asyncio.run(
            match_peer(
                args.server,
                args.port,
                args.room,
                args.name,
                public_endpoint,
                local_endpoint,
            )
        )
    except (OSError, EOFError, RuntimeError) as exc:
        udp_socket.close()
        print(f"Error de señalización: {exc}")
        return 1

    print(f"Peer encontrado: {remote['peer_id']}. Abriendo ruta UDP directa…")
    chat = DirectChat(udp_socket, args.name, remote)
    receiver = threading.Thread(target=chat.receiver, daemon=True)
    receiver.start()
    if not chat.punch():
        chat.close()
        print("No se pudo crear la ruta directa. Algún NAT puede impedir el hole punching UDP.")
        return 2

    print("Conexión P2P directa establecida. Escribe /salir para terminar.")
    try:
        while not chat.stopped.is_set():
            text = input("yo> ")
            if text.strip() == "/salir":
                break
            if text:
                chat.send_chat(text)
    except (EOFError, KeyboardInterrupt):
        pass
    finally:
        chat.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Chat P2P de terminal")
    parser.add_argument("--server", default="127.0.0.1", help="host del servidor de señalización")
    parser.add_argument("--port", type=int, default=9000, help="puerto TCP de señalización")
    parser.add_argument("--room", required=True, help="código compartido de la sala")
    parser.add_argument("--name", default=f"peer-{secrets.token_hex(3)}", help="nombre visible")
    parser.add_argument("--udp-port", type=int, default=0, help="puerto UDP local (0 = automático)")
    raise SystemExit(run(parser.parse_args()))


if __name__ == "__main__":
    main()

