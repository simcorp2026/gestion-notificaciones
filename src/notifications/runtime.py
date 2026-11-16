"""Utilidades de ciclo de vida de procesos (señales, reintento de arranque)."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable

from notifications.observability import get_logger

log = get_logger(__name__)


def install_shutdown_handlers(stop: asyncio.Event) -> None:
    """SIGTERM/SIGINT → ``stop.set()``. En Windows no hay ``add_signal_handler``: se usa
    ``signal.signal`` y se vuelve al loop con ``call_soon_threadsafe``."""
    loop = asyncio.get_running_loop()

    def _request_stop(signame: str) -> None:
        if not stop.is_set():
            log.info("shutdown.requested", signal=signame)
            stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop, sig.name)
        except NotImplementedError:
            signal.signal(
                sig, lambda s, _f: loop.call_soon_threadsafe(_request_stop, signal.Signals(s).name)
            )


async def wait_or_stop(stop: asyncio.Event, *events: asyncio.Event, within: float) -> None:
    """Duerme ``within`` s o hasta que se active ``stop`` o alguno de ``events``."""
    waiters = [asyncio.create_task(e.wait()) for e in (stop, *events)]
    try:
        await asyncio.wait(waiters, timeout=within, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waiters:
            w.cancel()


async def retry_until_ready(
    what: str,
    attempt: Callable[[], Awaitable[None]],
    stop: asyncio.Event,
    *,
    interval: float = 2.0,
) -> bool:
    """Reintenta ``attempt`` hasta que funcione o se pida parar. Devuelve False si se paró."""
    while not stop.is_set():
        try:
            await attempt()
        except Exception as exc:
            log.warning("startup.dependency_unavailable", dependency=what, error=repr(exc))
            await wait_or_stop(stop, within=interval)
        else:
            return True
    return False
