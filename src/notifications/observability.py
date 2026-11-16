"""Logging estructurado (structlog/JSON) con correlation_id y métricas Prometheus."""

from __future__ import annotations

import logging
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

CORRELATION_ID_KEY = "correlation_id"

# --------------------------------------------------------------------------- logging


def configure_logging(level: str = "INFO", *, json: bool = True) -> None:
    """Configura structlog y redirige el logging stdlib (alembic, aio-pika, uvicorn) al mismo
    pipeline, para que todo salga en un único formato con el ``correlation_id`` del contexto."""
    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            renderer,
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: str | None = None, **initial: Any) -> structlog.stdlib.BoundLogger:
    logger: structlog.stdlib.BoundLogger = structlog.stdlib.get_logger(name)
    return logger.bind(**initial) if initial else logger


def new_correlation_id() -> str:
    return str(uuid.uuid4())


def get_correlation_id() -> str | None:
    value = structlog.contextvars.get_contextvars().get(CORRELATION_ID_KEY)
    return str(value) if value is not None else None


@contextmanager
def correlation_context(correlation_id: str | None = None) -> Iterator[str]:
    """Vincula un ``correlation_id`` al contexto (contextvars → seguro por task asyncio)."""
    cid = correlation_id or new_correlation_id()
    with structlog.contextvars.bound_contextvars(**{CORRELATION_ID_KEY: cid}):
        yield cid


# --------------------------------------------------------------------------- métricas

NOTIFICATIONS_PUBLISHED = Counter(
    "notifications_published_total",
    "Eventos publicados al broker por el relay.",
    ["channel"],
)
NOTIFICATIONS_PROCESSED = Counter(
    "notifications_processed_total",
    "Mensajes procesados por los workers según resultado.",
    ["channel", "result"],  # result: ok | retry | dead | duplicate
)
NOTIFICATION_PROCESSING_SECONDS = Histogram(
    "notification_processing_seconds",
    "Duración del procesamiento de un mensaje en el worker.",
    ["channel"],
    # Latencias simuladas 50-300 ms + DB; cubre también reintentos lentos.
    buckets=(0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2.5, 5, 10),
)
QUEUE_DEPTH = Gauge("queue_depth", "Mensajes listos por cola (management API).", ["queue"])
DLQ_DEPTH = Gauge(
    "dlq_depth", "Dead letters pendientes (sin replay ni descarte) por canal.", ["channel"]
)

# Extra (no exigidas por el enunciado, útiles en el dashboard).
NOTIFICATIONS_ACCEPTED = Counter(
    "notifications_accepted_total", "Notificaciones aceptadas por la API (202 nuevas).", ["channel"]
)
OUTBOX_PENDING = Gauge("outbox_pending", "Eventos en outbox aún no publicados.")
BROKER_CONNECTED = Gauge("broker_connected", "1 si la conexión AMQP del proceso está activa.")
DEAD_LETTERS_ARCHIVED = Counter(
    "dead_letters_archived_total", "Mensajes muertos archivados en Postgres.", ["channel", "reason"]
)
DEAD_LETTERS_REPLAYED = Counter(
    "dead_letters_replayed_total", "Dead letters devueltas a la cola principal.", ["channel"]
)


def start_metrics_server(port: int) -> None:
    """Expone ``/metrics`` en un hilo daemon (procesos sin servidor HTTP: relay, workers, dlq).

    El servidor de ``prometheus_client`` es síncrono pero vive en su propio hilo: no bloquea el
    event loop.
    """
    start_http_server(port)
    get_logger(__name__).info("metrics.listening", port=port)
