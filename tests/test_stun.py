import socket
import struct
import unittest

from p2pchat.stun import MAGIC_COOKIE, XOR_MAPPED_ADDRESS, parse_binding_response


class StunParsingTests(unittest.TestCase):
    def test_parses_xor_mapped_ipv4_address(self) -> None:
        transaction_id = b"abcdefghijkl"
        ip = socket.inet_aton("203.0.113.9")
        cookie = struct.pack("!I", MAGIC_COOKIE)
        encoded_ip = bytes(a ^ b for a, b in zip(ip, cookie))
        encoded_port = 45678 ^ (MAGIC_COOKIE >> 16)
        value = struct.pack("!BBH", 0, 1, encoded_port) + encoded_ip
        attribute = struct.pack("!HH", XOR_MAPPED_ADDRESS, len(value)) + value
        response = struct.pack("!HHI12s", 0x0101, len(attribute), MAGIC_COOKIE, transaction_id) + attribute

        self.assertEqual(parse_binding_response(response, transaction_id), ("203.0.113.9", 45678))


if __name__ == "__main__":
    unittest.main()

