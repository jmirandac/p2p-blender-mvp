import asyncio
import unittest

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from p2pchat.client import WebRTCChat, match_and_connect
from p2pchat.signaling import SignalingServer


class WebRTCChatTests(unittest.IsolatedAsyncioTestCase):
    async def test_negotiates_data_channel_and_delivers_message(self) -> None:
        received: list[str] = []
        delivered = asyncio.Event()

        def on_message(text: str) -> None:
            received.append(text)
            delivered.set()

        alice = WebRTCChat("alice", "bob", stun_url="")
        bob = WebRTCChat("bob", "alice", stun_url="", on_message=on_message)
        try:
            offer = await alice.create_offer()
            answer = await bob.accept_offer(offer)
            await alice.accept_answer(answer)
            await asyncio.wait_for(
                asyncio.gather(alice.ready.wait(), bob.ready.wait()), timeout=5
            )

            alice.send_chat("hola")
            await asyncio.wait_for(delivered.wait(), timeout=5)
            self.assertEqual(received, ["hola"])
        finally:
            await asyncio.gather(
                alice.close(notify=False), bob.close(notify=False)
            )

    async def test_connects_through_signaling_and_delivers_message(self) -> None:
        server = await serve(SignalingServer().handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        alice = bob = None
        try:
            async with (
                connect(f"ws://127.0.0.1:{port}") as alice_ws,
                connect(f"ws://127.0.0.1:{port}") as bob_ws,
            ):
                alice, bob = await asyncio.wait_for(
                    asyncio.gather(
                        match_and_connect(alice_ws, "demo", "alice", ""),
                        match_and_connect(bob_ws, "demo", "bob", ""),
                    ),
                    timeout=5,
                )
                delivered = asyncio.Event()
                messages: list[str] = []
                bob._on_message = lambda text: (messages.append(text), delivered.set())
                alice.send_chat("hola desde la sala")
                await asyncio.wait_for(delivered.wait(), timeout=5)
                self.assertEqual(messages, ["hola desde la sala"])
        finally:
            chats = [chat for chat in (alice, bob) if chat is not None]
            if chats:
                await asyncio.gather(
                    *(chat.close(notify=False) for chat in chats)
                )
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
