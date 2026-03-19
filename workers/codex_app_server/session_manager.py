from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping, Sequence

from .transport import CodexAppServerTransport, TransportClosedError, TransportMessage


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _get_nested_value(payload: Any, *names: str) -> Any:
    stack: list[Any] = [payload]
    seen: set[int] = set()
    wanted = {name for name in names if name}
    while stack:
        current = stack.pop()
        ident = id(current)
        if ident in seen:
            continue
        seen.add(ident)
        if isinstance(current, Mapping):
            for key in wanted:
                if key in current and current[key] is not None:
                    return current[key]
            for value in current.values():
                if isinstance(value, (Mapping, list, tuple)):
                    stack.append(value)
        elif isinstance(current, (list, tuple)):
            stack.extend(current)
    return None


def _extract_payload(message: dict[str, Any] | None) -> Any:
    if message is None:
        return None
    if "params" in message:
        return message["params"]
    if "result" in message:
        return message["result"]
    if "error" in message:
        return message["error"]
    return message


@dataclass(slots=True)
class AppServerEvent:
    seq: int
    type: str
    run_id: str
    thread_id: str | None
    turn_id: str | None
    item_id: str | None
    payload: Any
    ts: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "type": self.type,
            "run_id": self.run_id,
            "thread_id": self.thread_id,
            "turn_id": self.turn_id,
            "item_id": self.item_id,
            "payload": self.payload,
            "ts": self.ts.isoformat(),
        }


@dataclass(slots=True)
class PendingServerRequest:
    request_id: Any
    method: str
    params: Any
    run_id: str
    thread_id: str | None
    turn_id: str | None
    received_at: datetime = field(default_factory=_utcnow)


@dataclass(slots=True)
class RunSessionSnapshot:
    run_id: str
    process_id: int | None
    thread_id: str | None
    active_turn_id: str | None
    state: str
    pending_request_ids: list[Any]
    event_count: int
    last_seq: int
    closed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "process_id": self.process_id,
            "thread_id": self.thread_id,
            "active_turn_id": self.active_turn_id,
            "state": self.state,
            "pending_request_ids": self.pending_request_ids,
            "event_count": self.event_count,
            "last_seq": self.last_seq,
            "closed": self.closed,
        }


@dataclass(slots=True)
class RunSession:
    run_id: str
    transport: CodexAppServerTransport
    cwd: str | None = None
    state: str = "starting"
    thread_id: str | None = None
    active_turn_id: str | None = None
    closed: bool = False
    last_error: str | None = None
    pending_requests: dict[str, PendingServerRequest] = field(default_factory=dict)
    event_buffer: deque[AppServerEvent] = field(default_factory=lambda: deque(maxlen=256))
    seq: int = 0
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    dispatcher_task: asyncio.Task[None] | None = None
    monitor_task: asyncio.Task[None] | None = None

    def snapshot(self) -> RunSessionSnapshot:
        process_id = None
        if self.transport.process is not None:
            process_id = self.transport.process.pid
        return RunSessionSnapshot(
            run_id=self.run_id,
            process_id=process_id,
            thread_id=self.thread_id,
            active_turn_id=self.active_turn_id,
            state=self.state,
            pending_request_ids=list(self.pending_requests),
            event_count=len(self.event_buffer),
            last_seq=self.seq,
            closed=self.closed,
        )


class CodexAppServerSessionManager:
    def __init__(
        self,
        *,
        transport_factory: type[CodexAppServerTransport] = CodexAppServerTransport,
        command: Sequence[str] | None = None,
        max_events: int = 256,
    ) -> None:
        self._transport_factory = transport_factory
        self._command = tuple(command or ("codex", "app-server", "--listen", "stdio://"))
        self._max_events = max_events
        self._sessions: dict[str, RunSession] = {}
        self._lock: asyncio.Lock | None = None

    async def _manager_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def start_run(
        self,
        run_id: str,
        *,
        cwd: str | None = None,
        thread_params: Mapping[str, Any] | None = None,
        turn_input: Sequence[Any] | None = None,
        turn_params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        session = await self._ensure_session(run_id, cwd=cwd)
        async with session.lock:
            if session.thread_id is None:
                thread_result = await session.transport.request(
                    "thread/start",
                    dict(thread_params or {}),
                )
                thread_id = _get_nested_value(thread_result, "threadId", "thread_id", "id")
                if thread_id is not None:
                    session.thread_id = str(thread_id)
                self._record_local_event(
                    session,
                    event_type="thread/started",
                    payload=thread_result,
                    thread_id=session.thread_id,
                    turn_id=session.active_turn_id,
                )
            turn_result = await self._start_turn(
                session,
                input_items=turn_input or [],
                turn_params=turn_params,
            )
            return {
                "run_id": run_id,
                "thread_id": session.thread_id,
                "turn_id": session.active_turn_id,
                "thread_result": thread_result if "thread_result" in locals() else None,
                "turn_result": turn_result,
                "session": session.snapshot().to_dict(),
            }

    async def continue_run(
        self,
        run_id: str,
        *,
        cwd: str | None = None,
        thread_id: str | None = None,
        thread_params: Mapping[str, Any] | None = None,
        turn_input: Sequence[Any] | None = None,
        turn_params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        session = await self._ensure_session(run_id, cwd=cwd)
        async with session.lock:
            if thread_id is not None and thread_id != session.thread_id:
                resumed = await session.transport.request(
                    "thread/resume",
                    {"threadId": thread_id, **dict(thread_params or {})},
                )
                resumed_thread_id = _get_nested_value(resumed, "threadId", "thread_id", "id")
                session.thread_id = str(resumed_thread_id or thread_id)
                self._record_local_event(
                    session,
                    event_type="thread/resumed",
                    payload=resumed,
                    thread_id=session.thread_id,
                    turn_id=session.active_turn_id,
                )
            elif session.thread_id is None:
                if thread_id is None:
                    raise ValueError("continue_run requires an existing thread_id or a resumed thread_id")
                resumed = await session.transport.request(
                    "thread/resume",
                    {"threadId": thread_id, **dict(thread_params or {})},
                )
                resumed_thread_id = _get_nested_value(resumed, "threadId", "thread_id", "id")
                session.thread_id = str(resumed_thread_id or thread_id)
                self._record_local_event(
                    session,
                    event_type="thread/resumed",
                    payload=resumed,
                    thread_id=session.thread_id,
                    turn_id=session.active_turn_id,
                )
            turn_result = await self._start_turn(
                session,
                input_items=turn_input or [],
                turn_params=turn_params,
            )
            return {
                "run_id": run_id,
                "thread_id": session.thread_id,
                "turn_id": session.active_turn_id,
                "turn_result": turn_result,
                "session": session.snapshot().to_dict(),
            }

    async def interrupt(self, run_id: str, *, thread_id: str | None = None, turn_id: str | None = None) -> dict[str, Any]:
        session = await self._ensure_existing_session(run_id)
        async with session.lock:
            thread = thread_id or session.thread_id
            turn = turn_id or session.active_turn_id
            if thread is None or turn is None:
                raise ValueError("interrupt requires an active thread and turn")
            result = await session.transport.request("turn/interrupt", {"threadId": thread, "turnId": turn})
            self._record_local_event(
                session,
                event_type="turn/interrupt",
                payload=result,
                thread_id=thread,
                turn_id=turn,
            )
            session.state = "interrupted"
            return {"run_id": run_id, "result": result, "session": session.snapshot().to_dict()}

    async def steer(
        self,
        run_id: str,
        *,
        input_items: Sequence[Any],
        expected_turn_id: str | None = None,
        thread_id: str | None = None,
    ) -> dict[str, Any]:
        session = await self._ensure_existing_session(run_id)
        async with session.lock:
            thread = thread_id or session.thread_id
            if thread is None:
                raise ValueError("steer requires an active thread")
            expected = expected_turn_id or session.active_turn_id
            if expected is None:
                raise ValueError("steer requires an active turn id")
            result = await session.transport.request(
                "turn/steer",
                {
                    "threadId": thread,
                    "expectedTurnId": expected,
                    "input": list(input_items),
                },
            )
            turn_id = _get_nested_value(result, "turnId", "turn_id", "id")
            if turn_id is not None:
                session.active_turn_id = str(turn_id)
            self._record_local_event(
                session,
                event_type="turn/steered",
                payload=result,
                thread_id=thread,
                turn_id=session.active_turn_id,
            )
            session.state = "running"
            return {"run_id": run_id, "result": result, "session": session.snapshot().to_dict()}

    async def answer_pending_request(
        self,
        run_id: str,
        request_id: Any,
        *,
        result: Any = None,
        error: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        session = await self._ensure_existing_session(run_id)
        async with session.lock:
            request_key = str(request_id)
            pending = session.pending_requests.get(request_key)
            if pending is None:
                raise KeyError(f"pending request not found: {request_id}")
            await session.transport.respond(request_id, result=result, error=error)
            session.pending_requests.pop(request_key, None)
            self._record_local_event(
                session,
                event_type="request/answered",
                payload={
                    "request_id": request_id,
                    "result": result,
                    "error": dict(error) if error is not None else None,
                    "method": pending.method,
                },
                thread_id=session.thread_id,
                turn_id=session.active_turn_id,
            )
            if session.state == "waiting_input":
                session.state = "running"
            return {"run_id": run_id, "session": session.snapshot().to_dict()}

    async def stream_events(self, run_id: str, *, after_seq: int = 0) -> AsyncIterator[dict[str, Any]]:
        session = await self._ensure_existing_session(run_id)
        last_seq = after_seq
        while True:
            replay: list[AppServerEvent]
            async with session.condition:
                replay = [event for event in session.event_buffer if event.seq > last_seq]
                if not replay and not session.closed:
                    await session.condition.wait_for(
                        lambda: session.closed or session.seq > last_seq
                    )
                    replay = [event for event in session.event_buffer if event.seq > last_seq]
                if session.closed and not replay:
                    return
            for event in replay:
                last_seq = event.seq
                yield event.to_dict()

    async def snapshot(self, run_id: str) -> dict[str, Any]:
        session = await self._ensure_existing_session(run_id)
        return session.snapshot().to_dict()

    async def shutdown(self) -> None:
        lock = await self._manager_lock()
        async with lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            await session.transport.aclose()
            session.closed = True
            self._notify(session)

    async def _ensure_session(self, run_id: str, *, cwd: str | None = None) -> RunSession:
        lock = await self._manager_lock()
        async with lock:
            session = self._sessions.get(run_id)
            if session is None or session.closed or session.transport.closed:
                transport = self._transport_factory(self._command, cwd=cwd)
                await transport.start()
                session = RunSession(run_id=run_id, transport=transport, cwd=cwd)
                session.event_buffer = deque(maxlen=self._max_events)
                self._sessions[run_id] = session
                session.dispatcher_task = asyncio.create_task(self._dispatch_loop(session))
            elif cwd is not None:
                session.cwd = cwd
            return session

    async def _ensure_existing_session(self, run_id: str) -> RunSession:
        lock = await self._manager_lock()
        async with lock:
            session = self._sessions.get(run_id)
            if session is None:
                raise KeyError(f"run session not found: {run_id}")
            return session

    async def _start_turn(
        self,
        session: RunSession,
        *,
        input_items: Sequence[Any],
        turn_params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if session.thread_id is None:
            raise ValueError("start_run requires a thread_id before starting a turn")
        params = dict(turn_params or {})
        params.setdefault("threadId", session.thread_id)
        params.setdefault("input", list(input_items))
        result = await session.transport.request("turn/start", params)
        turn_id = _get_nested_value(result, "turnId", "turn_id", "id")
        if turn_id is not None:
            session.active_turn_id = str(turn_id)
        self._record_local_event(
            session,
            event_type="turn/started",
            payload=result,
            thread_id=session.thread_id,
            turn_id=session.active_turn_id,
        )
        session.state = "running"
        return result

    async def _dispatch_loop(self, session: RunSession) -> None:
        try:
            while True:
                try:
                    message = await session.transport.recv()
                except TransportClosedError:
                    break
                self._apply_incoming_message(session, message)
        finally:
            session.closed = True
            session.state = "closed"
            self._notify(session)

    async def _monitor_transport(self, session: RunSession) -> None:
        try:
            while not session.closed and not session.transport.closed:
                proc = session.transport.process
                if proc is None:
                    break
                returncode = await proc.wait()
                if returncode is not None:
                    self._record_local_event(
                        session,
                        event_type="process/exited",
                        payload={"returncode": returncode},
                        thread_id=session.thread_id,
                        turn_id=session.active_turn_id,
                    )
                    session.closed = True
                    session.state = "closed"
                    self._notify(session)
                    return
        finally:
            session.closed = True

    def _apply_incoming_message(self, session: RunSession, message: TransportMessage) -> None:
        if message.kind == "stderr":
            self._record_local_event(
                session,
                event_type="stderr",
                payload={"line": message.text},
                thread_id=session.thread_id,
                turn_id=session.active_turn_id,
            )
            return
        if message.kind == "stdout":
            self._record_local_event(
                session,
                event_type="stdout",
                payload=message.message or {"line": message.text},
                thread_id=session.thread_id,
                turn_id=session.active_turn_id,
            )
            return
        if message.kind == "request":
            raw = message.message or {}
            method = str(raw.get("method") or "request")
            request_id = raw.get("id")
            pending = PendingServerRequest(
                request_id=request_id,
                method=method,
                params=raw.get("params"),
                run_id=session.run_id,
                thread_id=_get_nested_value(raw, "threadId", "thread_id", "threadid") or session.thread_id,
                turn_id=_get_nested_value(raw, "turnId", "turn_id", "turnid") or session.active_turn_id,
            )
            if request_id is not None:
                session.pending_requests[str(request_id)] = pending
            session.state = "waiting_input"
            self._record_local_event(
                session,
                event_type=f"request:{method}",
                payload={
                    "id": request_id,
                    "method": method,
                    "params": raw.get("params"),
                },
                thread_id=pending.thread_id,
                turn_id=pending.turn_id,
            )
            return
        if message.kind == "notification":
            raw = message.message or {}
            method = str(raw.get("method") or "notification")
            params = raw.get("params")
            thread_id = _get_nested_value(raw, "threadId", "thread_id", "threadid") or session.thread_id
            turn_id = _get_nested_value(raw, "turnId", "turn_id", "turnid") or session.active_turn_id
            item_id = _get_nested_value(raw, "itemId", "item_id", "itemid")
            if method in {"thread/started", "thread/resumed"}:
                resolved_thread = _get_nested_value(params, "threadId", "thread_id", "id") or _get_nested_value(
                    raw,
                    "threadId",
                    "thread_id",
                    "id",
                )
                if resolved_thread is not None:
                    session.thread_id = str(resolved_thread)
                    thread_id = session.thread_id
            if method in {"turn/started", "turn/steered"}:
                resolved_turn = _get_nested_value(params, "turnId", "turn_id", "id") or _get_nested_value(
                    raw,
                    "turnId",
                    "turn_id",
                    "id",
                )
                if resolved_turn is not None:
                    session.active_turn_id = str(resolved_turn)
                    turn_id = session.active_turn_id
                session.state = "running"
            if method in {"turn/completed", "turn/failed", "turn/interrupt"}:
                resolved_turn = _get_nested_value(params, "turnId", "turn_id", "id") or _get_nested_value(
                    raw,
                    "turnId",
                    "turn_id",
                    "id",
                )
                if resolved_turn is not None:
                    turn_id = str(resolved_turn)
                if session.active_turn_id is None or turn_id == session.active_turn_id:
                    session.active_turn_id = None
                session.state = "closed" if method == "turn/failed" else "running"
                if method == "turn/interrupt":
                    session.state = "interrupted"
            self._record_local_event(
                session,
                event_type=method,
                payload=params if params is not None else raw,
                thread_id=thread_id,
                turn_id=turn_id,
                item_id=str(item_id) if item_id is not None else None,
            )
            return
        if message.kind == "process/exited":
            self._record_local_event(
                session,
                event_type="process/exited",
                payload=message.message or {},
                thread_id=session.thread_id,
                turn_id=session.active_turn_id,
            )
            return
        self._record_local_event(
            session,
            event_type=message.kind,
            payload=message.message or {"text": message.text},
            thread_id=session.thread_id,
            turn_id=session.active_turn_id,
        )

    def _record_local_event(
        self,
        session: RunSession,
        *,
        event_type: str,
        payload: Any,
        thread_id: str | None,
        turn_id: str | None,
        item_id: str | None = None,
    ) -> AppServerEvent:
        session.seq += 1
        event = AppServerEvent(
            seq=session.seq,
            type=event_type,
            run_id=session.run_id,
            thread_id=thread_id,
            turn_id=turn_id,
            item_id=item_id,
            payload=payload,
        )
        session.event_buffer.append(event)
        self._notify(session)
        return event

    def _notify(self, session: RunSession) -> None:
        async def _wake() -> None:
            async with session.condition:
                session.condition.notify_all()

        asyncio.create_task(_wake())


__all__ = [
    "AppServerEvent",
    "CodexAppServerSessionManager",
    "PendingServerRequest",
    "RunSessionSnapshot",
]
