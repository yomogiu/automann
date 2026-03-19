from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class TransportMessage:
    kind: str
    message: dict[str, Any] | None = None
    text: str | None = None
    received_at: datetime = field(default_factory=_utcnow)


class JsonRpcError(RuntimeError):
    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.data = data


class TransportClosedError(RuntimeError):
    pass


class CodexAppServerTransport:
    def __init__(
        self,
        command: Sequence[str] | None = None,
        *,
        cwd: str | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self.command = tuple(command or ("codex", "app-server", "--listen", "stdio://"))
        self.cwd = cwd
        self.env = dict(env or os.environ)
        self.process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._incoming: asyncio.Queue[TransportMessage] = asyncio.Queue()
        self._send_lock = asyncio.Lock()
        self._next_id = 1
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def start(self, *, initialize_params: Mapping[str, Any] | None = None) -> None:
        if self.process is not None:
            return

        self.process = await asyncio.create_subprocess_exec(
            *self.command,
            cwd=self.cwd,
            env=self.env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._reader_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        self._monitor_task = asyncio.create_task(self._monitor_process())
        params = dict(initialize_params or {})
        params.setdefault(
            "clientInfo",
            {
                "name": "automann-codex-app-server-runtime",
                "title": "Automann Codex App Server Runtime",
                "version": "0.1.0",
            },
        )
        params.setdefault("capabilities", {"experimentalApi": True})
        await self.request("initialize", params)
        await self.notify("initialized")

    async def request(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        if self.process is None or self.process.stdin is None:
            raise TransportClosedError("transport is not started")
        if self._closed:
            raise TransportClosedError("transport is closed")

        request_id = f"req-{self._next_id}"
        self._next_id += 1
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = dict(params)
        await self._write(payload)
        try:
            return await future
        except Exception:
            if self._pending.get(request_id) is future:
                self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            raise

    async def notify(self, method: str, params: Mapping[str, Any] | None = None) -> None:
        if self.process is None or self.process.stdin is None:
            raise TransportClosedError("transport is not started")
        if self._closed:
            raise TransportClosedError("transport is closed")

        payload = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = dict(params)
        await self._write(payload)

    async def respond(
        self,
        request_id: Any,
        *,
        result: Any = None,
        error: Mapping[str, Any] | JsonRpcError | None = None,
    ) -> None:
        if self.process is None or self.process.stdin is None:
            raise TransportClosedError("transport is not started")
        if self._closed:
            raise TransportClosedError("transport is closed")

        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            if isinstance(error, JsonRpcError):
                payload["error"] = {
                    "code": error.code,
                    "message": str(error),
                    "data": error.data,
                }
            else:
                payload["error"] = dict(error)
        else:
            payload["result"] = result
        await self._write(payload)

    async def recv(self) -> TransportMessage:
        if self._closed and self._incoming.empty():
            raise TransportClosedError("transport is closed")
        message = await self._incoming.get()
        if message.kind == "closed":
            raise TransportClosedError("transport is closed")
        return message

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True

        for future in self._pending.values():
            if not future.done():
                future.set_exception(TransportClosedError("transport closed"))
        self._pending.clear()

        if self.process is not None:
            if self.process.returncode is None:
                self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), timeout=5)
                except TimeoutError:
                    self.process.kill()
                    await self.process.wait()

        for task in (self._reader_task, self._stderr_task, self._monitor_task):
            if task is not None:
                task.cancel()
        await self._incoming.put(TransportMessage(kind="closed"))

    async def _write(self, payload: Mapping[str, Any]) -> None:
        if self.process is None or self.process.stdin is None:
            raise TransportClosedError("transport is not started")
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
        async with self._send_lock:
            self.process.stdin.write(data)
            await self.process.stdin.drain()

    async def _read_stdout(self) -> None:
        assert self.process is not None
        stdout = self.process.stdout
        if stdout is None:
            return
        try:
            while True:
                line = await stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip("\n")
                if not text:
                    continue
                try:
                    message = json.loads(text)
                except json.JSONDecodeError:
                    await self._incoming.put(TransportMessage(kind="stdout", text=text))
                    continue
                if isinstance(message, dict):
                    if "id" in message and ("result" in message or "error" in message) and "method" not in message:
                        request_id = str(message["id"])
                        future = self._pending.pop(request_id, None)
                        if future is not None and not future.done():
                            if "error" in message and message["error"] is not None:
                                error = message["error"]
                                code = int(error.get("code", -32000)) if isinstance(error, dict) else -32000
                                error_message = str(error.get("message", "JSON-RPC error")) if isinstance(error, dict) else "JSON-RPC error"
                                data = error.get("data") if isinstance(error, dict) else None
                                future.set_exception(JsonRpcError(code, error_message, data))
                            else:
                                future.set_result(message.get("result"))
                        continue
                    kind = "request" if "id" in message else "notification"
                    await self._incoming.put(TransportMessage(kind=kind, message=message))
                else:
                    await self._incoming.put(
                        TransportMessage(
                            kind="stdout",
                            text=text,
                            message={"value": message},
                        )
                    )
        finally:
            await self._incoming.put(TransportMessage(kind="closed"))

    async def _read_stderr(self) -> None:
        assert self.process is not None
        stderr = self.process.stderr
        if stderr is None:
            return
        try:
            while True:
                line = await stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip("\n")
                if not text:
                    continue
                await self._incoming.put(TransportMessage(kind="stderr", text=text))
        finally:
            await self._incoming.put(TransportMessage(kind="closed"))

    async def _monitor_process(self) -> None:
        if self.process is None:
            return
        try:
            returncode = await self.process.wait()
            if not self._closed and returncode is not None:
                await self._incoming.put(
                    TransportMessage(
                        kind="process/exited",
                        message={"returncode": returncode},
                    )
                )
        finally:
            self._closed = True


__all__ = [
    "CodexAppServerTransport",
    "JsonRpcError",
    "TransportClosedError",
    "TransportMessage",
]
