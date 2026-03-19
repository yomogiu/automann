from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from workers.codex_app_server.transport import CodexAppServerTransport


class _Stream:
    def __init__(self, lines: list[bytes]) -> None:
        self.lines = list(lines)
        self.writes: list[bytes] = []

    async def readline(self) -> bytes:
        if self.lines:
            return self.lines.pop(0)
        await asyncio.sleep(0)
        return b""

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        return None


class _FakeProcess:
    def __init__(self) -> None:
        self.stdin = _Stream([])
        self.stdout = _Stream(
            [
                b'{"jsonrpc":"2.0","id":"req-1","result":{"userAgent":"codex","platformFamily":"unix","platformOs":"macos"}}\n',
                b'{"jsonrpc":"2.0","method":"thread/started","params":{"threadId":"thr-1"}}\n',
            ]
        )
        self.stderr = _Stream([])
        self.pid = 123
        self.returncode = 0

    def terminate(self) -> None:
        return None

    def kill(self) -> None:
        return None

    async def wait(self) -> int:
        return self.returncode


class CodexAppServerTransportTests(unittest.TestCase):
    def test_start_sends_initialize_and_receives_notification(self) -> None:
        fake_process = _FakeProcess()

        async def _run() -> dict:
            with patch("asyncio.create_subprocess_exec", return_value=fake_process):
                transport = CodexAppServerTransport()
                await transport.start()
                message = await transport.recv()
                await transport.aclose()
                return {
                    "written": b"".join(fake_process.stdin.writes).decode("utf-8"),
                    "kind": message.kind,
                    "method": message.message["method"],
                }

        result = asyncio.run(_run())
        self.assertIn('"method":"initialize"', result["written"])
        self.assertEqual(result["kind"], "notification")
        self.assertEqual(result["method"], "thread/started")
