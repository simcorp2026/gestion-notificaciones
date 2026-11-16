"""Configuración 12-factor vía variables de entorno, validada al arrancar.

Cada bloque es un ``BaseSettings`` independiente con su propio prefijo, de modo que un proceso
puede cargar sólo lo que necesita (p. ej. Alembic sólo ``DatabaseSettings``), mientras que
``Settings`` compone todos y falla rápido si falta o es inválida cualquier variable.
"""

from __future__ import annotations

import itertools
import sys
from functools import lru_cache
from typing import Annotated, Literal

from pydantic import (
    AmqpDsn,
    Field,
    HttpUrl,
    PostgresDsn,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

_BASE_CONFIG = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

PositiveInt = Annotated[int, Field(gt=0)]
PositiveFloat = Annotated[float, Field(gt=0, allow_inf_nan=False)]


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="DB_")

    url: PostgresDsn
    pool_size: PositiveInt = 10
    max_overflow: Annotated[int, Field(ge=0)] = 5
    echo: bool = False

    @field_validator("url")
    @classmethod
    def _require_asyncpg(cls, v: PostgresDsn) -> PostgresDsn:
        if v.scheme != "postgresql+asyncpg":
            msg = "DB_URL debe usar el driver async: postgresql+asyncpg://..."
            raise ValueError(msg)
        return v


class RabbitMQSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="RABBITMQ_")

    url: AmqpDsn
    management_url: HttpUrl
    prefetch: PositiveInt = 10
    exchange: str = Field(default="notifications", min_length=1)
    # Entregas máximas de un mismo mensaje en ``main`` (crash del worker sin ack) antes de DLQ.
    delivery_limit: PositiveInt = 10
    publish_timeout_seconds: PositiveFloat = 10.0
    reconnect_interval_seconds: PositiveFloat = 2.0


class RelaySettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="RELAY_")

    batch_size: Annotated[int, Field(gt=0, le=1000)] = 100
    # Polling de respaldo; LISTEN/NOTIFY despierta al relay antes cuando hay inserts.
    poll_interval_seconds: PositiveFloat = 1.0
    listen_notify: bool = True
    # Backoff por fila cuando el publish falla (broker caído): base * 2^(intentos-1), con tope.
    backoff_base_seconds: PositiveFloat = 1.0
    backoff_max_seconds: PositiveFloat = 60.0


class MetricsSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="METRICS_")

    # Puerto de /metrics en procesos sin HTTP propio (relay, workers, dlq). La API usa el suyo.
    port: Annotated[int, Field(gt=0, lt=65536)] = 9100
    enabled: bool = True
    # Sondeo de profundidad de colas (management API) y de outbox/DLQ (Postgres).
    monitor_interval_seconds: PositiveFloat = 5.0


Probability = Annotated[float, Field(ge=0.0, le=1.0)]


class WorkerSettings(BaseSettings):
    """Proveedor simulado + ciclo de vida del worker. Cada proceso atiende un canal."""

    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="WORKER_")

    sim_latency_min_ms: Annotated[int, Field(ge=0)] = 50
    sim_latency_max_ms: Annotated[int, Field(ge=0)] = 300
    sim_transient_failure_rate: Probability = 0.1
    sim_permanent_failure_rate: Probability = 0.0
    drain_timeout_seconds: PositiveFloat = 30.0

    @model_validator(mode="after")
    def _latency_range(self) -> WorkerSettings:
        if self.sim_latency_min_ms > self.sim_latency_max_ms:
            msg = "WORKER_SIM_LATENCY_MIN_MS no puede ser mayor que WORKER_SIM_LATENCY_MAX_MS"
            raise ValueError(msg)
        if self.sim_transient_failure_rate + self.sim_permanent_failure_rate > 1:
            msg = "la suma de tasas de fallo simuladas no puede superar 1"
            raise ValueError(msg)
        return self


class RetrySettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="RETRY_")

    # Un elemento por nivel de retry (cola ``<canal>.retry.<n>s``); longitud = máx. reintentos.
    # Admite fracciones (p. ej. 0.2) para tests rápidos; resolución de 1 ms.
    delays_seconds: list[PositiveFloat] = Field(default_factory=lambda: [5.0, 30.0, 300.0])

    @field_validator("delays_seconds")
    @classmethod
    def _non_empty_and_increasing(cls, v: list[float]) -> list[float]:
        if not v:
            msg = "RETRY_DELAYS_SECONDS no puede estar vacío"
            raise ValueError(msg)
        if any(d < 0.001 for d in v):  # noqa: PLR2004 - resolución mínima del TTL (1 ms)
            msg = "RETRY_DELAYS_SECONDS: cada delay debe ser >= 0.001 s"
            raise ValueError(msg)
        if any(b <= a for a, b in itertools.pairwise(v)):
            msg = "RETRY_DELAYS_SECONDS debe ser estrictamente creciente"
            raise ValueError(msg)
        return v

    @property
    def max_attempts(self) -> int:
        """Intentos totales = intento inicial + un reintento por nivel."""
        return len(self.delays_seconds) + 1


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="API_")

    admin_key: SecretStr = Field(min_length=16)
    host: str = "0.0.0.0"  # noqa: S104 - escucha en todas las interfaces dentro del contenedor
    port: Annotated[int, Field(gt=0, lt=65536)] = 8000


class Settings(BaseSettings):
    model_config = SettingsConfigDict(**_BASE_CONFIG, env_prefix="APP_")

    env: Literal["dev", "test", "prod"] = "dev"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_json: bool = True

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    rabbitmq: RabbitMQSettings = Field(default_factory=RabbitMQSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    relay: RelaySettings = Field(default_factory=RelaySettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    metrics: MetricsSettings = Field(default_factory=MetricsSettings)
    api: ApiSettings = Field(default_factory=ApiSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Carga y cachea la configuración. Lanza ``ValidationError`` si es inválida."""
    return Settings()


def load_settings_or_exit() -> Settings:
    """Para entrypoints: imprime los errores de configuración y sale con código 2."""
    try:
        return get_settings()
    except ValidationError as exc:
        print(f"Configuración inválida:\n{exc}", file=sys.stderr)
        raise SystemExit(2) from exc
