import asyncio
import unittest

from p2pchat.signaling import SignalingServer
from p2pchat.wire import read_message, write_message


class SignalingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = SignalingServer()
        self.server = await asyncio.start_server(self.app.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def join(self, peer_id: str):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        await write_message(
            writer,
            {
                "type": "join",
                "room": "demo",
                "peer_id": peer_id,
                "public_endpoint": ["203.0.113.1", 4000 if peer_id == "alice" else 5000],
                "local_endpoint": ["192.168.1.2", 4000 if peer_id == "alice" else 5000],
            },
        )
        return reader, writer

    async def test_pairs_two_peers_and_exchanges_endpoints(self) -> None:
        alice_reader, alice_writer = await self.join("alice")
        self.assertEqual((await read_message(alice_reader))["type"], "waiting")
        bob_reader, bob_writer = await self.join("bob")

        alice_match, bob_match = await asyncio.gather(
            read_message(alice_reader), read_message(bob_reader)
        )
        self.assertEqual(alice_match["peer"]["peer_id"], "bob")
        self.assertEqual(bob_match["peer"]["peer_id"], "alice")

        alice_writer.close()
        bob_writer.close()
        await asyncio.gather(alice_writer.wait_closed(), bob_writer.wait_closed())


if __name__ == "__main__":
    unittest.main()

