import asyncio
import json
import unittest

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from p2pchat.signaling import SignalingServer


class SignalingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = SignalingServer()
        self.server = await serve(self.app.handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def join(self, peer_id: str, room: str = "demo"):
        websocket = await connect(f"ws://127.0.0.1:{self.port}")
        await websocket.send(
            json.dumps({"type": "join", "room": room, "peer_id": peer_id})
        )
        return websocket

    async def test_pairs_two_peers_and_relays_descriptions(self) -> None:
        alice = await self.join("alice")
        self.assertEqual(json.loads(await alice.recv()), {"type": "waiting"})
        bob = await self.join("bob")

        alice_match, bob_match = await asyncio.gather(alice.recv(), bob.recv())
        self.assertEqual(
            json.loads(alice_match),
            {"type": "matched", "peer_id": "bob", "initiator": True},
        )
        self.assertEqual(
            json.loads(bob_match),
            {"type": "matched", "peer_id": "alice", "initiator": False},
        )

        offer = {
            "type": "description",
            "description": {"type": "offer", "sdp": "v=0\r\n"},
        }
        await alice.send(json.dumps(offer))
        self.assertEqual(json.loads(await bob.recv()), offer)

        await alice.close()
        self.assertEqual(json.loads(await bob.recv()), {"type": "peer-left"})
        await bob.close()

    async def test_rejects_invalid_room(self) -> None:
        peer = await self.join("alice", room="not a valid room")
        message = json.loads(await peer.recv())
        self.assertEqual(message["type"], "error")
        self.assertIn("sala", message["message"])
        await peer.close()


if __name__ == "__main__":
    unittest.main()
