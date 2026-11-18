import asyncio
from threading import Event

import pytest
from abassurance.shared import request_limits
from abassurance.shared.request_limits import RequestBodyLimit, RequestLimits
from abassurance.shared.spark_runtime import SparkBusy, SparkRuntime
from starlette.responses import JSONResponse


def test_rate_limit_expires_without_affecting_other_accounts(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(request_limits.time, "monotonic", lambda: now[0])

    limits = RequestLimits()

    assert limits.retry_after("alice", 2) == 0
    assert limits.retry_after("alice", 2) == 0
    assert limits.retry_after("alice", 2) == 60
    assert limits.retry_after("bob", 2) == 0

    now[0] += 61

    assert limits.retry_after("alice", 2) == 0


def test_limiter_storage_is_bounded_and_full_buckets_cannot_be_evicted(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(request_limits.time, "monotonic", lambda: now[0])

    limits = RequestLimits(max_keys=2)

    assert limits.retry_after("a", 1) == 0
    assert limits.retry_after("b", 1) == 0
    assert limits.retry_after("c", 1) == 60
    assert limits.retry_after("a", 1) == 60
    assert len(limits.windows) == 2

    now[0] += 61

    assert limits.retry_after("c", 1) == 0


@pytest.mark.parametrize(
    ("headers", "chunks", "expected"),
    [
        ([(b"content-length", b"10")], [b"small"], 413),
        ([], [b"123", b"456"], 413),
        ([(b"content-length", b"1")], [b"123456"], 413),
        ([(b"content-length", b"bad")], [b"1"], 400),
        ([], [b"12", b"34"], 200),
    ],
)
def test_body_size_is_checked_before_parsing_including_chunked(headers, chunks, expected):
    called = []
    sent = []

    messages = [
        {"type": "http.request", "body": chunk, "more_body": index < len(chunks) - 1}
        for index, chunk in enumerate(chunks)
    ]

    async def app(scope, receive, send):
        called.append((await receive())["body"])
        await JSONResponse({"ok": True})(scope, receive, send)

    async def receive():
        return messages.pop(0)

    async def send(message):
        sent.append(message)

    middleware = RequestBodyLimit(app, max_bytes=5)
    scope = {"type": "http", "method": "POST", "headers": headers}

    asyncio.run(middleware(scope, receive, send))

    assert sent[0]["status"] == expected
    assert bool(called) == (expected == 200)

    if called:
        assert called == [b"1234"]


def test_slow_body_times_out_before_app_is_called():
    sent = []

    async def receive():
        await asyncio.sleep(10)
        return {"type": "http.request", "body": b"", "more_body": False}

    async def app(scope, receive, send):
        pytest.fail("Body must be rejected before entering the app")

    async def send(message):
        sent.append(message)

    middleware = RequestBodyLimit(app, timeout=0.01)
    scope = {"type": "http", "method": "POST", "headers": []}

    asyncio.run(middleware(scope, receive, send))

    assert sent[0]["status"] == 408


def test_timed_out_spark_job_keeps_its_slot_until_it_really_finishes(monkeypatch):
    runtime = SparkRuntime(max_pending=1)
    started = Event()
    release = Event()

    monkeypatch.setattr(runtime, "_call", lambda action: action(None))

    def blocked(_):
        started.set()
        release.wait(3)

    async def exercise():
        task = asyncio.create_task(runtime.run(blocked, timeout=0.02))

        assert await asyncio.to_thread(started.wait, 1)

        with pytest.raises(SparkBusy):
            await runtime.run(lambda _: None)

        with pytest.raises(TimeoutError):
            await task

        with pytest.raises(SparkBusy):
            await runtime.run(lambda _: None)

        release.set()

        await asyncio.wrap_future(runtime.executor.submit(lambda: None))

        assert await runtime.run(lambda _: 42) == 42

    try:
        asyncio.run(exercise())
    finally:
        release.set()
        runtime.close()


def test_failed_spark_job_releases_its_slot(monkeypatch):
    runtime = SparkRuntime(max_pending=1)

    monkeypatch.setattr(runtime, "_call", lambda action: action(None))

    def fail(_):
        raise ValueError("test failure")

    async def exercise():
        with pytest.raises(ValueError):
            await runtime.run(fail)

        assert await runtime.run(lambda _: 42) == 42

    try:
        asyncio.run(exercise())
    finally:
        runtime.close()


def test_closed_executor_does_not_leak_queue_capacity():
    runtime = SparkRuntime(max_pending=1)
    runtime.close()

    async def exercise():
        for _ in range(2):
            with pytest.raises(RuntimeError, match="shutdown"):
                await runtime.run(lambda _: None)

    asyncio.run(exercise())
