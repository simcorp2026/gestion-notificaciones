# Arquitectura

## Flujo

```
Cliente ──HTTP──> API FastAPI ──(tx)──> Postgres: outbox
                                          │ relay: SELECT ... FOR UPDATE SKIP LOCKED
                                          ▼ publish + confirm → marca published
                        exchange `notifications` (topic, durable)
                          │ notification.email | .sms | .push
              ┌───────────┼───────────────┐
              ▼           ▼               ▼
         email.main    sms.main       push.main        (quorum)
              │ TransientError → publish a retry.<n> + ack
              ▼
   email.retry.5s → email.retry.30s → email.retry.300s  (TTL + DLX → main)
              │ PermanentError o reintentos agotados → publish a DLQ + ack
              ▼
          email.dlq ──dlq-archiver──> Postgres: dead_letters ──> API admin (list/replay/discard)
                                                              │ replay: x-attempt=0
                                                              └──────────────> email.main
```

## Componentes

| Proceso | Entrypoint | Responsabilidad |
|---|---|---|
| API | `notifications-api` | `POST /notifications` (outbox), estado, admin DLQ |
| Relay | `notifications-relay` | outbox → RabbitMQ con confirms; N réplicas vía `SKIP LOCKED` |
| Worker | `notifications-worker --channel <c>` | consume `main`, idempotencia, retry/DLQ |
| DLQ archiver | `notifications-dlq` | consume `<c>.dlq` de todos los canales → `dead_letters` |

Una sola imagen Docker; el comando decide el rol.

## Garantías

- **At-least-once** extremo a extremo + idempotencia del consumidor → *effectively-once* (ADR 008).
- Mensajes `PERSISTENT`, colas durables; `main` quorum (ADR 005).
- Retry/DLQ por republicación explícita con confirm antes del `ack` (ADR 003).

## Envelope

```json
{ "id": "uuid", "type": "notification.requested", "version": 1,
  "occurred_at": "ISO-8601", "correlation_id": "uuid", "payload": { } }
```

Propiedades AMQP: `message_id = envelope.id`, `correlation_id`, `type`, `PERSISTENT`.

| Cabecera | Dónde | Significado |
|---|---|---|
| `x-attempt` | siempre | reintentos ya hechos (0 = primera entrega; replay lo reinicia) |
| `x-error-class`, `x-error-message` | retry y DLQ | último error |
| `x-failed-at`, `x-first-failed-at` | retry y DLQ | ISO-8601 UTC |
| `x-dead-reason` | DLQ | `permanent` \| `exhausted` \| `invalid` |
| `x-traceback` | DLQ | resumen (últimos 2000 caracteres) |
| `x-original-routing-key` | DLQ | cola/clave por la que llegó |
| `x-replay-count`, `x-replayed-from-dead-letter` | replay | nº de replays e id de la dead letter |

## Tablas

| Tabla | Escribe | Propósito |
|---|---|---|
| `notifications` | API, relay, worker, archiver, admin | agregado con estado |
| `outbox` | API (tx), relay | eventos pendientes de publicar |
| `processed_messages` | worker | idempotencia del consumidor |
| `delivery_attempts` | worker | historial de intentos |
| `dead_letters` | archiver, admin | DLQ persistente (`pending → replayed \| discarded`) |

## Capa de mensajería (`notifications.messaging`)

| Módulo | Contenido |
|---|---|
| `bus.py` | `EventPublisher` / `Replayer` / `MessageBus` (Protocol), `RawMessage`, `PublishError` |
| `headers.py` | nombres de cabeceras (contrato neutral al broker) |
| `policy.py` | `decide()` retry vs DLQ, sin dependencia del broker (reutilizable por Kafka) |
| `topology.py` | nombres y argumentos de colas, puro |
| `rabbitmq.py` | `RabbitMQBus`, `RabbitMQConsumer`, `RabbitMQDeadLetterConsumer` (aio-pika) |

## Estado por fase

- [x] Fase 1 — scaffold, config, logging, Alembic, compose (infra), CI
- [x] Fase 2 — topología, publisher con confirms, consumer base
- [x] Fase 3 — API + outbox + relay
- [x] Fase 4 — workers, idempotencia, clasificación de errores
- [x] Fase 5 — admin DLQ + estado
- [x] Fase 6 — métricas, dashboard, graceful shutdown, reconexión
