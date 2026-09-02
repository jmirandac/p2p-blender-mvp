"""Tiny newline-delimited JSON protocol used for signaling."""

from __future__ import annotations

import asyncio
import json
from typing import Any


MAX_LINE = 16_384


async def read_message(reader: asyncio.StreamReader) -> dict[str, Any]:
    line = await reader.readline()
    if not line:
        raise EOFError("conexión cerrada")
    if len(line) > MAX_LINE:
        raise ValueError("mensaje demasiado grande")
    value = json.loads(line)
    if not isinstance(value, dict):
        raise ValueError("se esperaba un objeto JSON")
    return value


async def write_message(writer: asyncio.StreamWriter, message: dict[str, Any]) -> None:
    writer.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")
    await writer.drain()

