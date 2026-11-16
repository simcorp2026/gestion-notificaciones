"""Adaptador RabbitMQ (aio-pika) de ``MessageBus``.

- Conexión ``connect_robust``: reconecta y re-declara canales/consumidores tras caída del broker.
- Publicación con *publisher confirms* + ``mandatory`` → ``publish`` sólo retorna si el broker
  aceptó y enrutó el mensaje; si no, ``PublishError``.
- Consumo con ``ack`` manual. Retry/DLQ por republicación confirmada y *después* ``ack`` (ADR 003).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import aio_pika
import structlog
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from aio_pika.exceptions import AMQPError, ChannelInvalidStateError
from pydantic import ValidationError

from notifications.config import RabbitMQSettings, RetrySettings
from notifications.contracts import Channel, Envelope
from notifications.messaging.bus import (
    DeadLetterHandler,
    DeliveryContext,
    Handler,
    PublishError,
    RawMessage,
)
from notifications.messaging.headers import (
    H_ATTEMPT,
    H_DEAD_REASON,
    H_ERROR_CLASS,
    H_ERROR_MESSAGE,
    H_FAILED_AT,
    H_FIRST_FAILED_AT,
    H_ORIGINAL_ROUTING_KEY,
    H_TRACEBACK,
)
from notifications.messaging.policy import (
    DeadReason,
    Outcome,
    decide,
    summarize_traceback,
)
from notifications.messaging.topology import Topology
from notifications.observability import BROKER_CONNECTED, correlation_context, get_logger

log = get_logger(__name__)

_ERROR_MESSAGE_MAX = 500


def attempt_from_headers(headers: Mapping[str, Any] | None) -> int:
    raw = (headers or {}).get(H_ATTEMPT, 0)
    try:
        return max(int(raw), 0)
    except (TypeError, ValueError):
        return 0


class RabbitMQBus:
    def __init__(
        self,
        url: str,
        topology: Topology,
        *,
        prefetch: int = 10,
        publish_timeout: float = 10.0,
        reconnect_interval: float = 5.0,
    ) -> None:
        self._url = url
        self.topology = topology
        self._prefetch = prefetch
        self._publish_timeout = publish_timeout
        self._reconnect_interval = reconnect_interval
        self._connection: AbstractRobustConnection | None = None
        self._publish_channel: AbstractChannel | None = None
        self._exchange: AbstractExchange | None = None

    @classmethod
    def from_settings(
        cls, rabbit: RabbitMQSettings, retry: RetrySettings, *, namespace: str = ""
    ) -> RabbitMQBus:
        return cls(
            str(rabbit.url),
            Topology.from_settings(rabbit, retry, namespace=namespace),
            prefetch=rabbit.prefetch,
            publish_timeout=rabbit.publish_timeout_seconds,
            reconnect_interval=rabbit.reconnect_interval_seconds,
        )

    # ------------------------------------------------------------------ ciclo de vida
    async def connect(self) -> None:
        # aio-pika lee `reconnect_interval` de la query string de la URL.
        url = _with_query(self._url, reconnect_interval=str(self._reconnect_interval))
        self._connection = await aio_pika.connect_robust(url)
        self._publish_channel = await self._connection.channel(
            publisher_confirms=True, on_return_raises=True
        )
        # connect_robust reconecta solo; los callbacks sólo reflejan el estado en métricas y logs.
        self._connection.close_callbacks.add(self._on_connection_lost)
        self._connection.reconnect_callbacks.add(self._on_reconnected)
        BROKER_CONNECTED.set(1)
        log.info("rabbitmq.connected")

    @staticmethod
    def _on_connection_lost(*_: object) -> None:
        BROKER_CONNECTED.set(0)
        log.warning("rabbitmq.connection_lost")

    @staticmethod
    def _on_reconnected(*_: object) -> None:
        BROKER_CONNECTED.set(1)
        log.info("rabbitmq.reconnected")

    async def close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._connection = self._publish_channel = self._exchange = None
        log.info("rabbitmq.closed")

    @property
    def connection(self) -> AbstractRobustConnection:
        if self._connection is None:
            msg = "RabbitMQBus no conectado: llama a connect()"
            raise RuntimeError(msg)
        return self._connection

    @property
    def _channel(self) -> AbstractChannel:
        if self._publish_channel is None:
            # PublishError (no RuntimeError): publicar sin conexión es "broker no disponible".
            msg = "RabbitMQBus no conectado"
            raise PublishError(msg)
        return self._publish_channel

    async def _main_exchange(self) -> AbstractExchange:
        if self._exchange is None:
            self._exchange = await self._channel.get_exchange(self.topology.exchange)
        return self._exchange

    # ------------------------------------------------------------------ topología
    async def setup_topology(self) -> None:
        t = self.topology
        ch = self._channel
        exchange = await ch.declare_exchange(t.exchange, ExchangeType.TOPIC, durable=True)
        dlx = await ch.declare_exchange(t.dlx, ExchangeType.DIRECT, durable=True)

        for channel in t.channels:
            main = await ch.declare_queue(
                t.main_queue(channel), durable=True, arguments=t.main_queue_arguments(channel)
            )
            await main.bind(exchange, routing_key=t.routing_key(channel))
            # Camino de vuelta desde las colas de retry (TTL → DLX → main).
            await main.bind(dlx, routing_key=t.main_queue(channel))

            for level, name in enumerate(t.retry_queues(channel)):
                await ch.declare_queue(
                    name, durable=True, arguments=t.retry_queue_arguments(channel, level)
                )

            dlq = await ch.declare_queue(t.dlq(channel), durable=True, arguments=t.dlq_arguments())
            await dlq.bind(dlx, routing_key=t.dlq(channel))

        self._exchange = exchange
        log.info(
            "rabbitmq.topology_ready", exchange=t.exchange, channels=[str(c) for c in t.channels]
        )

    # ------------------------------------------------------------------ publicación
    async def publish(self, envelope: Envelope, channel: Channel) -> None:
        message = Message(
            body=envelope.to_bytes(),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=str(envelope.id),
            correlation_id=envelope.correlation_id,
            type=envelope.type,
            timestamp=envelope.occurred_at,
            headers={H_ATTEMPT: 0},
        )
        exchange = await self._main_exchange()
        await self._confirmed_publish(exchange, message, self.topology.routing_key(channel))

    async def publish_to_queue(self, message: Message, queue: str) -> None:
        """Publica directo a una cola (default exchange). Usado por retry, DLQ y replay."""
        await self._confirmed_publish(self._channel.default_exchange, message, queue)

    async def _confirmed_publish(
        self, exchange: AbstractExchange, message: Message, routing_key: str
    ) -> None:
        try:
            # Timeout total (incluye esperar a que un canal robusto se reabra), no sólo el confirm.
            async with asyncio.timeout(self._publish_timeout):
                await exchange.publish(message, routing_key=routing_key, mandatory=True)
        # ChannelInvalidStateError (canal cerrado mientras el broker está caído) es RuntimeError,
        # no AMQPError; ConnectionError cubre el socket cortado.
        except (AMQPError, ChannelInvalidStateError, ConnectionError, TimeoutError) as exc:
            raise PublishError(f"publish a {routing_key!r} no confirmado: {exc!r}") from exc

    # ------------------------------------------------------------------ consumo
    async def consume(self, channel: Channel, handler: Handler) -> RabbitMQConsumer:
        amqp_channel = await self.connection.channel()
        await amqp_channel.set_qos(prefetch_count=self._prefetch)
        queue = await amqp_channel.get_queue(self.topology.main_queue(channel), ensure=True)
        consumer = RabbitMQConsumer(self, channel, handler, amqp_channel, queue)
        await consumer.start()
        return consumer

    async def consume_dead_letters(
        self, channel: Channel, handler: DeadLetterHandler
    ) -> RabbitMQDeadLetterConsumer:
        amqp_channel = await self.connection.channel()
        await amqp_channel.set_qos(prefetch_count=self._prefetch)
        queue = await amqp_channel.get_queue(self.topology.dlq(channel), ensure=True)
        consumer = RabbitMQDeadLetterConsumer(channel, handler, amqp_channel, queue)
        await consumer.start()
        return consumer

    # ------------------------------------------------------------------ replay
    async def replay(self, message: RawMessage) -> None:
        """Republica un mensaje muerto en ``<canal>.main`` (directo, sin pasar por el topic)."""
        amqp_message = Message(
            body=message.body,
            headers=dict(message.headers),
            content_type=message.content_type,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            type=message.type,
        )
        await self.publish_to_queue(amqp_message, self.topology.main_queue(message.channel))


class _BaseConsumer:
    """Ciclo de vida común: consumo con ack manual, seguimiento de mensajes en vuelo y drenaje."""

    def __init__(
        self, channel: Channel, amqp_channel: AbstractChannel, queue: AbstractQueue
    ) -> None:
        self._channel = channel
        self._amqp_channel = amqp_channel
        self._queue = queue
        self._tag: str | None = None
        self._inflight: set[asyncio.Task[Any]] = set()

    async def start(self) -> None:
        self._tag = await self._queue.consume(self._on_message, no_ack=False)
        log.info("consumer.started", queue=self._queue.name, channel=self._channel)

    async def _process(self, message: AbstractIncomingMessage) -> None:
        raise NotImplementedError

    async def stop(self, drain_timeout: float = 30.0) -> None:
        if self._tag is not None:
            await self._queue.cancel(self._tag)
            self._tag = None
        if self._inflight:
            log.info("consumer.draining", inflight=len(self._inflight))
            _, pending = await asyncio.wait(self._inflight, timeout=drain_timeout)
            if pending:
                log.warning("consumer.drain_timeout", pending=len(pending))
        if not self._amqp_channel.is_closed:
            await self._amqp_channel.close()
        log.info("consumer.stopped", queue=self._queue.name)

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        # aio-pika cancela los callbacks en vuelo cuando se cae la conexión. Sin el ``shield``, la
        # cancelación cortaría el envío a mitad y haría rollback de la marca de procesado: al
        # re-entregarse, el mensaje se enviaría otra vez. Con él, el procesamiento termina y hace
        # commit; sólo se pierde el ack, y la re-entrega se detecta como duplicado.
        task = asyncio.create_task(self._process(message))
        self._inflight.add(task)
        task.add_done_callback(self._on_task_done)
        await asyncio.shield(task)

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        self._inflight.discard(task)
        if not task.cancelled() and (exc := task.exception()) is not None:
            log.error("consumer.unhandled_error", error=repr(exc), exc_info=exc)


class RabbitMQConsumer(_BaseConsumer):
    """Consumidor de ``<canal>.main`` con la política de retry/DLQ."""

    def __init__(
        self,
        bus: RabbitMQBus,
        channel: Channel,
        handler: Handler,
        amqp_channel: AbstractChannel,
        queue: AbstractQueue,
    ) -> None:
        super().__init__(channel, amqp_channel, queue)
        self._bus = bus
        self._handler = handler

    async def _process(self, message: AbstractIncomingMessage) -> None:
        attempt = attempt_from_headers(message.headers)
        try:
            envelope = Envelope.from_bytes(message.body)
        except ValidationError as exc:
            with structlog.contextvars.bound_contextvars(message_id=message.message_id):
                log.warning("message.invalid", error=str(exc)[:_ERROR_MESSAGE_MAX])
                await self._dead_letter(message, exc, attempt, DeadReason.INVALID)
            return

        ctx = DeliveryContext(
            channel=self._channel,
            attempt=attempt,
            redelivered=message.redelivered or False,
            headers=dict(message.headers or {}),
        )
        with (
            correlation_context(envelope.correlation_id),
            structlog.contextvars.bound_contextvars(
                message_id=str(envelope.id), channel=str(self._channel), attempt=attempt
            ),
        ):
            try:
                await self._handler(envelope, ctx)
            except asyncio.CancelledError:
                await self._safe_nack(message)
                raise
            except Exception as exc:
                await self._handle_failure(message, exc, attempt)
                return
            await _safe_ack(message)
            log.info("message.ok")

    async def _handle_failure(
        self, message: AbstractIncomingMessage, exc: Exception, attempt: int
    ) -> None:
        decision = decide(exc, attempt, self._bus.topology.retry_delays)
        if decision.outcome is Outcome.DEAD:
            assert decision.reason is not None  # noqa: S101 - invariante de decide()
            log.warning("message.dead", reason=decision.reason, error=repr(exc))
            await self._dead_letter(message, exc, attempt, decision.reason)
            return

        assert decision.retry_level is not None  # noqa: S101
        queue = self._bus.topology.retry_queue(self._channel, decision.retry_level)
        headers = {
            **(message.headers or {}),
            H_ATTEMPT: decision.next_attempt,
            H_ERROR_CLASS: type(exc).__name__,
            H_ERROR_MESSAGE: str(exc)[:_ERROR_MESSAGE_MAX],
            H_FAILED_AT: _now_iso(),
        }
        headers.setdefault(H_FIRST_FAILED_AT, headers[H_FAILED_AT])
        log.info("message.retry", next_attempt=decision.next_attempt, queue=queue, error=repr(exc))
        await self._republish_then_ack(message, queue, headers)

    async def _dead_letter(
        self,
        message: AbstractIncomingMessage,
        exc: BaseException,
        attempt: int,
        reason: DeadReason,
    ) -> None:
        headers = {
            **(message.headers or {}),
            H_ATTEMPT: attempt,
            H_DEAD_REASON: str(reason),
            H_ERROR_CLASS: type(exc).__name__,
            H_ERROR_MESSAGE: str(exc)[:_ERROR_MESSAGE_MAX],
            H_TRACEBACK: summarize_traceback(exc),
            H_FAILED_AT: _now_iso(),
            H_ORIGINAL_ROUTING_KEY: message.routing_key or "",
        }
        await self._republish_then_ack(message, self._bus.topology.dlq(self._channel), headers)

    async def _republish_then_ack(
        self, message: AbstractIncomingMessage, queue: str, headers: dict[str, Any]
    ) -> None:
        clone = Message(
            body=message.body,
            headers=headers,
            content_type=message.content_type,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            type=message.type,
            timestamp=message.timestamp,
        )
        try:
            await self._bus.publish_to_queue(clone, queue)
        except PublishError:
            # No se pudo mover: se devuelve a la cola para no perderlo (cuenta en delivery-limit).
            log.exception("message.republish_failed", queue=queue)
            await self._safe_nack(message)
            return
        await _safe_ack(message)

    @staticmethod
    async def _safe_nack(message: AbstractIncomingMessage) -> None:
        try:
            await message.nack(requeue=True)
        except (AMQPError, ChannelInvalidStateError, ConnectionError):
            # Canal caído: el broker re-entregará el mensaje al reconectar.
            log.warning("message.nack_failed")


class RabbitMQDeadLetterConsumer(_BaseConsumer):
    """Consumidor de ``<canal>.dlq``: entrega el mensaje crudo al handler y hace ``ack``.

    El handler debe reintentar internamente sus fallos (DB caída): si lanza, el mensaje se
    devuelve a la cola tras una pausa, lo que cuenta para el ``delivery-limit`` de la quorum queue.
    """

    _NACK_PAUSE_SECONDS = 2.0

    def __init__(
        self,
        channel: Channel,
        handler: DeadLetterHandler,
        amqp_channel: AbstractChannel,
        queue: AbstractQueue,
    ) -> None:
        super().__init__(channel, amqp_channel, queue)
        self._handler = handler

    async def _process(self, message: AbstractIncomingMessage) -> None:
        raw = RawMessage(
            channel=self._channel,
            body=message.body,
            headers=dict(message.headers or {}),
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            content_type=message.content_type,
            type=message.type,
        )
        with structlog.contextvars.bound_contextvars(
            message_id=message.message_id, channel=str(self._channel)
        ):
            try:
                await self._handler(raw)
            except asyncio.CancelledError:
                await RabbitMQConsumer._safe_nack(message)
                raise
            except Exception:
                log.exception("dead_letter.handler_failed")
                await asyncio.sleep(self._NACK_PAUSE_SECONDS)
                await RabbitMQConsumer._safe_nack(message)
                return
            await _safe_ack(message)


async def _safe_ack(message: AbstractIncomingMessage) -> None:
    """Si el canal cayó (broker reiniciándose), el ack no llega: el broker re-entregará el
    mensaje tras reconectar y la idempotencia del consumidor evita reprocesarlo."""
    try:
        await message.ack()
    except (AMQPError, ChannelInvalidStateError, ConnectionError) as exc:
        log.warning("message.ack_failed", error=repr(exc))


def _with_query(url: str, **params: str) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query))
    query.update(params)
    return urlunsplit(parts._replace(query=urlencode(query)))


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()
