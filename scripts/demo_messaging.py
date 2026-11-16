"""Demo manual de la capa de mensajería contra el RabbitMQ de docker compose.

Declara una topología aislada (namespace ``demo`` por defecto), arranca un consumidor con un
comportamiento elegido, publica N mensajes y muestra dónde terminó cada uno.

    uv run python scripts/demo_messaging.py --mode transient
    uv run python scripts/demo_messaging.py --mode flaky --count 20 --fail-rate 0.5
    uv run python scripts/demo_messaging.py --mode permanent --keep  # conserva la DLQ
"""

from __future__ import annotations

import argparse
import asyncio
import random
from collections import Counter
from collections.abc import Awaitable, Callable

from aio_pika.abc import AbstractRobustConnection

from notifications.config import RabbitMQSettings, RetrySettings
from notifications.contracts import Channel, Envelope
from notifications.errors import PermanentError, TransientError
from notifications.messaging.bus import DeliveryContext
from notifications.messaging.rabbitmq import RabbitMQBus
from notifications.observability import configure_logging, get_logger

log = get_logger("demo")


def build_handler(
    mode: str, fail_rate: float, stats: Counter[str]
) -> Callable[[Envelope, DeliveryContext], Awaitable[None]]:
    async def handler(envelope: Envelope, ctx: DeliveryContext) -> None:
        stats["calls"] += 1
        log.info("demo.handle", n=envelope.payload.get("n"), attempt=ctx.attempt)
        await asyncio.sleep(random.uniform(0.01, 0.1))  # noqa: S311 - latencia simulada
        if mode == "transient":
            raise TransientError("proveedor no disponible (simulado)")
        if mode == "permanent":
            raise PermanentError("destinatario inválido (simulado)")
        if mode == "flaky" and random.random() < fail_rate:  # noqa: S311
            raise TransientError("fallo aleatorio (simulado)")
        stats["ok"] += 1

    return handler


async def queue_depths(conn: AbstractRobustConnection, names: list[str]) -> dict[str, int]:
    channel = await conn.channel()
    try:
        result = {}
        for name in names:
            q = await channel.declare_queue(name, passive=True)
            result[name] = q.declaration_result.message_count or 0
        return result
    finally:
        await channel.close()


async def purge(conn: AbstractRobustConnection, names: list[str]) -> None:
    channel = await conn.channel()
    try:
        for name in names:
            await (await channel.get_queue(name)).purge()
    finally:
        await channel.close()


async def main(args: argparse.Namespace) -> None:
    configure_logging("INFO", json=args.json)
    delays = [float(d) for d in args.delays.split(",")]
    channel = Channel(args.channel)

    bus = RabbitMQBus.from_settings(
        RabbitMQSettings(),  # se lee de RABBITMQ_* / .env
        RetrySettings(delays_seconds=delays),
        namespace=args.namespace,
    )
    await bus.connect()
    await bus.setup_topology()
    t = bus.topology
    queues = [t.main_queue(channel), *t.retry_queues(channel), t.dlq(channel)]
    await purge(bus.connection, queues)

    stats: Counter[str] = Counter()
    consumer = await bus.consume(channel, build_handler(args.mode, args.fail_rate, stats))
    for i in range(args.count):
        await bus.publish(Envelope(type="demo", payload={"n": i}), channel)
    log.info("demo.published", count=args.count, mode=args.mode, delays=delays)

    wait = args.wait if args.wait is not None else sum(delays) + 2
    log.info("demo.waiting", seconds=wait)
    await asyncio.sleep(wait)
    await consumer.stop()

    depths = await queue_depths(bus.connection, queues)
    print("\n=== Resultado ===")
    print(f"llamadas al handler: {stats['calls']}  |  éxitos: {stats['ok']}")
    for name, n in depths.items():
        print(f"  {name:<40} {n:>4} mensajes")
    if not args.keep:
        await purge(bus.connection, queues)
    else:
        print(f"\nColas conservadas: revisa {t.dlq(channel)} en http://localhost:15672")
    await bus.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--mode", choices=["ok", "transient", "permanent", "flaky"], default="transient")
    p.add_argument("--count", type=int, default=3)
    p.add_argument("--channel", choices=[c.value for c in Channel], default="email")
    p.add_argument("--delays", default="1,2,3", help="segundos por nivel de retry, CSV")
    p.add_argument("--fail-rate", type=float, default=0.5)
    p.add_argument("--namespace", default="demo")
    p.add_argument("--wait", type=float, default=None, help="segundos a esperar (def: suma+2)")
    p.add_argument("--keep", action="store_true", help="no purgar las colas al terminar")
    p.add_argument("--json", action="store_true", help="logs en JSON")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
