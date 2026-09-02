"""Small RFC 5389 STUN client, sufficient for endpoint discovery."""

from __future__ import annotations

import secrets
import socket
import struct
from typing import Iterable


MAGIC_COOKIE = 0x2112A442
BINDING_REQUEST = 0x0001
BINDING_SUCCESS = 0x0101
MAPPED_ADDRESS = 0x0001
XOR_MAPPED_ADDRESS = 0x0020

GOOGLE_STUN_SERVERS = (
    ("stun.l.google.com", 19302),
    ("stun1.l.google.com", 19302),
    ("stun2.l.google.com", 19302),
)


class StunError(RuntimeError):
    """Raised when no valid STUN response can be obtained."""


def _decode_address(attribute_type: int, value: bytes) -> tuple[str, int]:
    if len(value) < 8:
        raise StunError("Atributo de dirección STUN incompleto")

    _reserved, family, encoded_port = struct.unpack("!BBH", value[:4])
    if family != 0x01:
        raise StunError("Este MVP solo admite direcciones STUN IPv4")

    address = value[4:8]
    if attribute_type == XOR_MAPPED_ADDRESS:
        port = encoded_port ^ (MAGIC_COOKIE >> 16)
        cookie = struct.pack("!I", MAGIC_COOKIE)
        address = bytes(a ^ b for a, b in zip(address, cookie))
    else:
        port = encoded_port

    return socket.inet_ntoa(address), port


def parse_binding_response(data: bytes, transaction_id: bytes) -> tuple[str, int]:
    """Return the mapped IPv4 endpoint from a STUN binding response."""
    if len(data) < 20:
        raise StunError("Respuesta STUN demasiado corta")

    message_type, message_length, cookie = struct.unpack("!HHI", data[:8])
    if message_type != BINDING_SUCCESS:
        raise StunError(f"Respuesta STUN no exitosa: 0x{message_type:04x}")
    if cookie != MAGIC_COOKIE or data[8:20] != transaction_id:
        raise StunError("La respuesta STUN no corresponde a la solicitud")
    if len(data) < 20 + message_length:
        raise StunError("Respuesta STUN truncada")

    fallback: tuple[str, int] | None = None
    offset = 20
    end = 20 + message_length
    while offset + 4 <= end:
        attribute_type, length = struct.unpack("!HH", data[offset : offset + 4])
        value_start = offset + 4
        value_end = value_start + length
        if value_end > end:
            raise StunError("Atributo STUN truncado")
        if attribute_type in (XOR_MAPPED_ADDRESS, MAPPED_ADDRESS):
            decoded = _decode_address(attribute_type, data[value_start:value_end])
            if attribute_type == XOR_MAPPED_ADDRESS:
                return decoded
            fallback = decoded
        offset = value_end + ((4 - length % 4) % 4)

    if fallback:
        return fallback
    raise StunError("La respuesta no contiene una dirección pública")


def discover_public_endpoint(
    udp_socket: socket.socket,
    servers: Iterable[tuple[str, int]] = GOOGLE_STUN_SERVERS,
    timeout: float = 2.0,
) -> tuple[str, int]:
    """Query STUN using the socket that will later carry peer traffic."""
    previous_timeout = udp_socket.gettimeout()
    errors: list[str] = []
    try:
        udp_socket.settimeout(timeout)
        for host, port in servers:
            transaction_id = secrets.token_bytes(12)
            request = struct.pack("!HHI12s", BINDING_REQUEST, 0, MAGIC_COOKIE, transaction_id)
            try:
                addresses = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_DGRAM)
                if not addresses:
                    raise OSError("sin dirección IPv4")
                destination = addresses[0][4]
                udp_socket.sendto(request, destination)
                while True:
                    response, source = udp_socket.recvfrom(2048)
                    if source[0] != destination[0]:
                        continue
                    return parse_binding_response(response, transaction_id)
            except (OSError, StunError) as exc:
                errors.append(f"{host}: {exc}")
    finally:
        udp_socket.settimeout(previous_timeout)

    raise StunError("No se pudo consultar STUN (" + "; ".join(errors) + ")")

