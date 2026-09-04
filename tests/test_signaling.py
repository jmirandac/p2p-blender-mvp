import asyncio
import json
import unittest

from websockets.asyncio.client import ClientConnection, connect
from websockets.asyncio.server import serve

from p2pchat.signaling import SignalingServer, parse_server_args


class SignalingConfigurationTests(unittest.TestCase):
    def test_uses_current_defaults_without_environment_variables(self) -> None:
        args = parse_server_args([], {})

        self.assertEqual(args.host, "0.0.0.0")
        self.assertEqual(args.port, 9000)
        self.assertEqual(args.heartbeat_interval, 10)
        self.assertEqual(args.heartbeat_timeout, 20)
        self.assertEqual(args.invite_timeout, 15)
        self.assertEqual(args.negotiation_timeout, 30)

    def test_reads_configuration_from_environment_variables(self) -> None:
        args = parse_server_args(
            [],
            {
                "SIGNALING_HOST": "127.0.0.1",
                "SIGNALING_PORT": "9100",
                "SIGNALING_HEARTBEAT_INTERVAL": "11.5",
                "SIGNALING_HEARTBEAT_TIMEOUT": "21.5",
                "SIGNALING_INVITE_TIMEOUT": "16.5",
                "SIGNALING_NEGOTIATION_TIMEOUT": "31.5",
            },
        )

        self.assertEqual(args.host, "127.0.0.1")
        self.assertEqual(args.port, 9100)
        self.assertEqual(args.heartbeat_interval, 11.5)
        self.assertEqual(args.heartbeat_timeout, 21.5)
        self.assertEqual(args.invite_timeout, 16.5)
        self.assertEqual(args.negotiation_timeout, 31.5)

    def test_command_line_arguments_take_precedence_over_environment(self) -> None:
        args = parse_server_args(
            ["--port", "9200", "--invite-timeout", "17"],
            {"SIGNALING_PORT": "9100", "SIGNALING_INVITE_TIMEOUT": "16"},
        )

        self.assertEqual(args.port, 9200)
        self.assertEqual(args.invite_timeout, 17)


class SignalingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = SignalingServer(invite_timeout=0.05, negotiation_timeout=0.08)
        self.server = await serve(
            self.app.handle, "127.0.0.1", 0, ping_interval=None
        )
        self.port = self.server.sockets[0].getsockname()[1]
        self.websockets: list[ClientConnection] = []

    async def asyncTearDown(self) -> None:
        await asyncio.gather(
            *(websocket.close() for websocket in self.websockets),
            return_exceptions=True,
        )
        self.server.close()
        await self.server.wait_closed()

    async def register(self, name: str) -> tuple[ClientConnection, str]:
        websocket = await connect(f"ws://127.0.0.1:{self.port}")
        self.websockets.append(websocket)
        await websocket.send(json.dumps({"type": "register", "name": name}))
        message = await self.receive(websocket)
        self.assertEqual(message["type"], "registered")
        return websocket, message["peer"]["id"]

    async def receive(
        self, websocket: ClientConnection, timeout: float = 1
    ) -> dict:
        raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
        return json.loads(raw)

    async def invite(
        self,
        requester: ClientConnection,
        target: ClientConnection,
        target_id: str,
        request_id: str = "request-1",
    ) -> str:
        await requester.send(
            json.dumps(
                {
                    "type": "connect-request",
                    "request_id": request_id,
                    "target_id": target_id,
                }
            )
        )
        pending, invitation = await asyncio.gather(
            self.receive(requester), self.receive(target)
        )
        self.assertEqual(pending["type"], "connect-pending")
        self.assertEqual(invitation["type"], "connection-request")
        self.assertEqual(pending["session_id"], invitation["session_id"])
        return pending["session_id"]

    async def accept(
        self,
        requester: ClientConnection,
        target: ClientConnection,
        session_id: str,
    ) -> tuple[dict, dict]:
        await target.send(
            json.dumps(
                {
                    "type": "connection-response",
                    "session_id": session_id,
                    "accepted": True,
                }
            )
        )
        requester_match, target_match = await asyncio.gather(
            self.receive(requester), self.receive(target)
        )
        self.assertTrue(requester_match["initiator"])
        self.assertFalse(target_match["initiator"])
        return requester_match, target_match

    async def test_registers_unique_ids_lists_and_retains_disconnected_peer(self) -> None:
        alice, alice_id = await self.register("same-name")
        bob, bob_id = await self.register("same-name")
        self.assertNotEqual(alice_id, bob_id)

        await alice.send(json.dumps({"type": "list-peers", "request_id": "list-1"}))
        listing = await self.receive(alice)
        self.assertEqual(
            listing,
            {
                "type": "peer-list",
                "request_id": "list-1",
                "peers": [
                    {"id": bob_id, "name": "same-name", "availability": "waiting"}
                ],
            },
        )

        await bob.send(json.dumps({"type": "disconnect"}))
        self.assertEqual(await self.receive(bob), {"type": "disconnected"})
        await bob.wait_closed()
        async with asyncio.timeout(1):
            while self.app.peers[bob_id].connection_status != "disconnected":
                await asyncio.sleep(0)
        self.assertEqual(self.app.peers[bob_id].connection_status, "disconnected")
        self.assertIsNone(self.app.peers[bob_id].websocket)

        await alice.send(json.dumps({"type": "list-peers", "request_id": "list-2"}))
        self.assertEqual((await self.receive(alice))["peers"], [])

    async def test_rejects_invalid_registration_and_recovers_from_bad_command(self) -> None:
        invalid = await connect(f"ws://127.0.0.1:{self.port}")
        self.websockets.append(invalid)
        await invalid.send(json.dumps({"type": "register", "name": "not valid"}))
        error = await self.receive(invalid)
        self.assertEqual(error["code"], "invalid-name")
        await invalid.wait_closed()

        alice, _ = await self.register("alice")
        await alice.send(json.dumps({"type": "does-not-exist"}))
        self.assertEqual((await self.receive(alice))["code"], "unknown-command")
        await alice.send(json.dumps({"type": "list-peers", "request_id": "still-ok"}))
        self.assertEqual((await self.receive(alice))["type"], "peer-list")

    async def test_rejects_self_unknown_and_busy_targets(self) -> None:
        alice, alice_id = await self.register("alice")
        bob, bob_id = await self.register("bob")
        charlie, _ = await self.register("charlie")

        for request_id, target_id, status in (
            ("self-1", alice_id, "self"),
            ("missing-1", "peer_missing", "unavailable"),
        ):
            await alice.send(
                json.dumps(
                    {
                        "type": "connect-request",
                        "request_id": request_id,
                        "target_id": target_id,
                    }
                )
            )
            self.assertEqual((await self.receive(alice))["status"], status)

        await self.invite(alice, bob, bob_id)
        await charlie.send(
            json.dumps(
                {
                    "type": "connect-request",
                    "request_id": "busy-1",
                    "target_id": bob_id,
                }
            )
        )
        self.assertEqual((await self.receive(charlie))["status"], "busy")

    async def test_simultaneous_requests_reserve_target_atomically(self) -> None:
        alice, _ = await self.register("alice")
        bob, _ = await self.register("bob")
        target, target_id = await self.register("target")

        await asyncio.gather(
            alice.send(
                json.dumps(
                    {
                        "type": "connect-request",
                        "request_id": "alice-request",
                        "target_id": target_id,
                    }
                )
            ),
            bob.send(
                json.dumps(
                    {
                        "type": "connect-request",
                        "request_id": "bob-request",
                        "target_id": target_id,
                    }
                )
            ),
        )
        alice_result, bob_result, invitation = await asyncio.gather(
            self.receive(alice), self.receive(bob), self.receive(target)
        )
        self.assertEqual(invitation["type"], "connection-request")
        self.assertEqual(
            {alice_result["type"], bob_result["type"]},
            {"connect-pending", "connection-result"},
        )
        loser = (
            alice_result
            if alice_result["type"] == "connection-result"
            else bob_result
        )
        self.assertEqual(loser["status"], "busy")

    async def test_rejection_and_invitation_timeout_restore_waiting(self) -> None:
        alice, alice_id = await self.register("alice")
        bob, bob_id = await self.register("bob")
        session_id = await self.invite(alice, bob, bob_id)
        await bob.send(
            json.dumps(
                {
                    "type": "connection-response",
                    "session_id": session_id,
                    "accepted": False,
                }
            )
        )
        alice_result, bob_result = await asyncio.gather(
            self.receive(alice), self.receive(bob)
        )
        self.assertEqual(alice_result["status"], "rejected")
        self.assertEqual(bob_result["status"], "rejected")

        session_id = await self.invite(alice, bob, bob_id, "timeout-1")
        alice_result, bob_result = await asyncio.gather(
            self.receive(alice), self.receive(bob)
        )
        self.assertEqual(alice_result["status"], "timeout")
        self.assertEqual(bob_result, {"type": "invitation-expired", "session_id": session_id})
        self.assertEqual(self.app.peers[alice_id].activity_state, "waiting")
        self.assertEqual(self.app.peers[bob_id].activity_state, "waiting")
        self.assertEqual(len(self.app.sessions), 0)

    async def test_accepts_relays_sdp_starts_and_ends_chat(self) -> None:
        alice, alice_id = await self.register("alice")
        bob, bob_id = await self.register("bob")
        session_id = await self.invite(alice, bob, bob_id)
        alice_match, bob_match = await self.accept(alice, bob, session_id)
        self.assertEqual(alice_match["peer"]["id"], bob_id)
        self.assertEqual(bob_match["peer"]["id"], alice_id)

        offer = {
            "type": "description",
            "session_id": session_id,
            "description": {"type": "offer", "sdp": "offer-sdp"},
        }
        await alice.send(json.dumps(offer))
        self.assertEqual(await self.receive(bob), offer)
        await bob.send(
            json.dumps(
                {
                    "type": "description",
                    "session_id": session_id,
                    "description": {"type": "answer", "sdp": "answer-sdp"},
                }
            )
        )
        answer = await self.receive(alice)
        self.assertEqual(answer["description"]["type"], "answer")

        await asyncio.gather(
            alice.send(json.dumps({"type": "chat-ready", "session_id": session_id})),
            bob.send(json.dumps({"type": "chat-ready", "session_id": session_id})),
        )
        alice_started, bob_started = await asyncio.gather(
            self.receive(alice), self.receive(bob)
        )
        self.assertEqual(alice_started["type"], "chat-started")
        self.assertEqual(bob_started["type"], "chat-started")

        await alice.send(
            json.dumps(
                {"type": "chat-end", "session_id": session_id, "reason": "left"}
            )
        )
        alice_ended, bob_ended = await asyncio.gather(
            self.receive(alice), self.receive(bob)
        )
        self.assertEqual(alice_ended["type"], "session-ended")
        self.assertEqual(bob_ended["reason"], "left")
        self.assertEqual(self.app.peers[alice_id].activity_state, "waiting")
        self.assertEqual(self.app.peers[bob_id].activity_state, "waiting")

    async def test_invalid_sdp_sequence_is_recoverable(self) -> None:
        alice, _ = await self.register("alice")
        bob, bob_id = await self.register("bob")
        session_id = await self.invite(alice, bob, bob_id)
        await self.accept(alice, bob, session_id)
        await bob.send(
            json.dumps(
                {
                    "type": "description",
                    "session_id": session_id,
                    "description": {"type": "offer", "sdp": "wrong-sender"},
                }
            )
        )
        self.assertEqual(
            (await self.receive(bob))["code"], "invalid-description-state"
        )

    async def test_negotiation_timeout_and_disconnect_release_partner(self) -> None:
        alice, alice_id = await self.register("alice")
        bob, bob_id = await self.register("bob")
        session_id = await self.invite(alice, bob, bob_id)
        await self.accept(alice, bob, session_id)
        alice_ended, bob_ended = await asyncio.gather(
            self.receive(alice), self.receive(bob)
        )
        self.assertEqual(alice_ended["reason"], "negotiation-timeout")
        self.assertEqual(bob_ended["reason"], "negotiation-timeout")

        session_id = await self.invite(alice, bob, bob_id, "disconnect-1")
        await alice.close()
        ended = await self.receive(bob)
        self.assertEqual(ended["session_id"], session_id)
        self.assertEqual(ended["reason"], "peer-disconnected")
        self.assertEqual(self.app.peers[alice_id].connection_status, "disconnected")
        self.assertEqual(self.app.peers[bob_id].activity_state, "waiting")


if __name__ == "__main__":
    unittest.main()
