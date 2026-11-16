from __future__ import annotations

import asyncio
import json
import logging

import pytest

from notifications.observability import (
    configure_logging,
    correlation_context,
    get_correlation_id,
    get_logger,
)


def _json_lines(out: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def test_log_includes_correlation_id(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json=True)
    log = get_logger("test")

    with correlation_context("abc-123"):
        log.info("dentro", foo=1)
    log.info("fuera")

    inside, outside = _json_lines(capsys.readouterr().out)
    assert inside["event"] == "dentro"
    assert inside["correlation_id"] == "abc-123"
    assert inside["foo"] == 1
    assert inside["level"] == "info"
    assert "correlation_id" not in outside


def test_stdlib_logs_are_rendered_as_json_with_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json=True)

    with correlation_context("std-1"):
        logging.getLogger("third.party").warning("hola %s", "mundo")

    (line,) = _json_lines(capsys.readouterr().out)
    assert line["event"] == "hola mundo"
    assert line["correlation_id"] == "std-1"
    assert line["logger"] == "third.party"


def test_correlation_context_generates_id_when_missing() -> None:
    with correlation_context() as cid:
        assert cid
        assert get_correlation_id() == cid
    assert get_correlation_id() is None


async def test_correlation_id_is_isolated_per_task() -> None:
    async def worker(cid: str) -> str | None:
        with correlation_context(cid):
            await asyncio.sleep(0)
            return get_correlation_id()

    results = await asyncio.gather(*(worker(f"cid-{i}") for i in range(5)))

    assert results == [f"cid-{i}" for i in range(5)]
