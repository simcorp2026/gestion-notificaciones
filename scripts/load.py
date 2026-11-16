"""Genera carga contra la API: N notificaciones en canales aleatorios, con una fracción de
destinatarios inválidos (→ DLQ por error permanente) y de envíos duplicados (misma
``idempotency_key``).

    uv run python scripts/load.py --count 200
    uv run python scripts/load.py --count 50 --invalid-rate 0.2 --duplicate-rate 0.1
"""

from __future__ import annotations

import argparse
import asyncio
import random
import uuid
from collections import Counter
from typing import Any

import httpx

VALID = {
    "email": lambda i: f"user{i}@example.com",
    "sms": lambda i: f"+3460{i:07d}",
    "push": lambda i: f"fcm:{i:06d}{uuid.uuid4().hex}",
}
INVALID = {"email": "no-es-email", "sms": "600111222", "push": "corto"}


def build(i: int, invalid_rate: float) -> dict[str, Any]:
    channel = random.choice(list(VALID))  # noqa: S311
    bad = random.random() < invalid_rate  # noqa: S311
    return {
        "channel": channel,
        "recipient": INVALID[channel] if bad else VALID[channel](i),
        "template": "load-test",
        "payload": {"i": i},
        "idempotency_key": f"load-{uuid.uuid4().hex}",
    }


async def main(args: argparse.Namespace) -> None:
    bodies = [build(i, args.invalid_rate) for i in range(args.count)]
    dups = [b for b in bodies if random.random() < args.duplicate_rate]  # noqa: S311
    sem = asyncio.Semaphore(args.concurrency)
    stats: Counter[str] = Counter()

    async with httpx.AsyncClient(base_url=args.url, timeout=10) as client:

        async def post(body: dict[str, Any]) -> None:
            async with sem:
                r = await client.post("/notifications", json=body)
                replay = r.headers.get("Idempotent-Replayed") == "true"
                stats[f"{r.status_code}{' (replay)' if replay else ''}"] += 1

        await asyncio.gather(*(post(b) for b in bodies))
        await asyncio.gather(*(post(b) for b in dups))

    channels = Counter(b["channel"] for b in bodies)
    invalid = sum(1 for b in bodies if b["recipient"] in INVALID.values())
    print(f"enviadas: {len(bodies)} + {len(dups)} duplicadas  |  por canal: {dict(channels)}")
    print(f"destinatarios inválidos (→ DLQ directo): {invalid}")
    print(f"respuestas: {dict(stats)}")
    print("\nSigue el estado con:  just stats")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--url", default="http://localhost:8000")
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--invalid-rate", type=float, default=0.05)
    p.add_argument("--duplicate-rate", type=float, default=0.1)
    p.add_argument("--concurrency", type=int, default=20)
    asyncio.run(main(p.parse_args()))
