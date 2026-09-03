import asyncio
import unittest

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from p2pchat.client import SignalingClient, WebRTCChat
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

    async def test_persistent_clients_chat_twice_without_reconnecting(self) -> None:
        app = SignalingServer(invite_timeout=1, negotiation_timeout=5)
        server = await serve(app.handle, "127.0.0.1", 0, ping_interval=None)
        port = server.sockets[0].getsockname()[1]
        alice_messages: list[str] = []
        bob_messages: list[str] = []
        alice_client = bob_client = None
        try:
            async with (
                connect(f"ws://127.0.0.1:{port}") as alice_ws,
                connect(f"ws://127.0.0.1:{port}") as bob_ws,
            ):
                alice_client = SignalingClient(
                    alice_ws,
                    "alice",
                    stun_url="",
                    on_output=lambda _text: None,
                    on_chat_message=alice_messages.append,
                )
                bob_client = SignalingClient(
                    bob_ws,
                    "bob",
                    stun_url="",
                    on_output=lambda _text: None,
                    on_chat_message=bob_messages.append,
                )
                alice_id, bob_id = await asyncio.gather(
                    alice_client.start(), bob_client.start()
                )

                peers = await alice_client.list_peers()
                self.assertEqual(peers[0]["id"], bob_id)

                for number in (1, 2):
                    await alice_client.request_connection(bob_id)
                    await bob_client.wait_for_event("connection-request")
                    await bob_client.respond_to_invitation(True)
                    await asyncio.gather(
                        alice_client.wait_for_event("chat-started", timeout=5),
                        bob_client.wait_for_event("chat-started", timeout=5),
                    )
                    self.assertEqual(alice_client.state, "chatting")
                    self.assertEqual(bob_client.state, "chatting")

                    assert alice_client.chat is not None
                    alice_client.chat.send_chat(f"mensaje {number}")
                    async with asyncio.timeout(5):
                        while len(bob_messages) < number:
                            await asyncio.sleep(0.01)

                    await alice_client.end_chat()
                    await asyncio.gather(
                        alice_client.wait_for_event("session-ended"),
                        bob_client.wait_for_event("session-ended"),
                    )
                    self.assertEqual(alice_client.state, "waiting")
                    self.assertEqual(bob_client.state, "waiting")

                self.assertEqual(bob_messages, ["mensaje 1", "mensaje 2"])
                self.assertEqual(alice_client.peer_id, alice_id)
                await asyncio.gather(
                    alice_client.disconnect(), bob_client.disconnect()
                )
        finally:
            clients = [
                client for client in (alice_client, bob_client) if client is not None
            ]
            await asyncio.gather(
                *(client._close_local_chat() for client in clients),
                return_exceptions=True,
            )
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
