from __future__ import annotations

from collections.abc import Mapping
import math
from typing import Annotated, Any, ClassVar, Literal, Self

from pydantic import (
    AnyHttpUrl,
    Field,
    RedisDsn,
    SecretStr,
    StringConstraints,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

ConsumerName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"),
]
_REDIS_DSN_ADAPTER = TypeAdapter(RedisDsn)
_HTTP_URL_ADAPTER = TypeAdapter(AnyHttpUrl)


class Settings(BaseSettings):
    """Least-privilege, fixture-confined email-worker configuration."""

    service_name: ClassVar[str] = "signaldesk-email-worker"

    model_config = SettingsConfigDict(
        env_prefix="SIGNALDESK_EMAIL_WORKER_",
        extra="forbid",
        hide_input_in_errors=True,
        strict=True,
        validate_default=True,
    )

    redis_url: SecretStr
    control_api_base_url: AnyHttpUrl
    email_worker_service_credential: SecretStr
    consumer_name: ConsumerName

    stream_name: Literal["signaldesk:emails"] = "signaldesk:emails"
    consumer_group: Literal["email-workers"] = "email-workers"
    dlq_stream_name: Literal["signaldesk:emails:dlq"] = "signaldesk:emails:dlq"
    smtp_host: Literal["mailpit"] = "mailpit"
    smtp_port: Literal[1025] = 1025
    mailpit_api_base_url: AnyHttpUrl = _HTTP_URL_ADAPTER.validate_python(
        "http://mailpit:8025"
    )
    sender_email: Literal["notifications@signaldesk.test"] = (
        "notifications@signaldesk.test"
    )

    block_time_ms: int = Field(default=1_000, ge=1, le=5_000)
    stale_idle_ms: int = Field(default=150_000, ge=1_000, le=300_000)
    max_deliveries: int = Field(default=5, ge=1, le=20)
    batch_size: Literal[1] = 1
    redis_connect_timeout_seconds: float = Field(default=3.0, gt=0, le=10)
    redis_socket_timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    api_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    api_max_response_bytes: int = Field(default=16_384, ge=256, le=65_536)
    mailpit_api_timeout_seconds: float = Field(default=5.0, gt=0, le=30)
    mailpit_api_max_response_bytes: int = Field(default=16_384, ge=256, le=65_536)
    smtp_timeout_seconds: float = Field(default=10.0, gt=0, le=30)
    rendered_body_max_bytes: int = Field(default=8_192, ge=256, le=65_536)
    message_max_bytes: int = Field(default=32_768, ge=1_024, le=131_072)
    event_max_bytes: int = Field(default=8_192, ge=256, le=16_384)
    recovery_age_seconds: float = Field(default=60.0, gt=0, le=240)

    @model_validator(mode="before")
    @classmethod
    def sanitize_sensitive_inputs_before_validation(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        masked = dict(value)
        if "redis_url" in masked:
            raw = masked["redis_url"]
            if isinstance(raw, SecretStr):
                pass
            elif isinstance(raw, str):
                masked["redis_url"] = SecretStr(raw)
            elif isinstance(raw, (bytes, bytearray, memoryview)):
                try:
                    decoded = bytes(raw).decode("utf-8", "strict")
                except UnicodeDecodeError:
                    decoded = "invalid://"
                masked["redis_url"] = SecretStr(decoded)
            else:
                masked["redis_url"] = SecretStr("invalid://")
        credential = masked.get("email_worker_service_credential")
        if not isinstance(credential, SecretStr):
            if isinstance(credential, str):
                masked["email_worker_service_credential"] = SecretStr(credential)
            elif isinstance(credential, (bytes, bytearray, memoryview)):
                try:
                    decoded = bytes(credential).decode("utf-8", "strict")
                except UnicodeDecodeError:
                    decoded = "invalid"
                masked["email_worker_service_credential"] = SecretStr(decoded)
            elif "email_worker_service_credential" in masked:
                masked["email_worker_service_credential"] = SecretStr("invalid")
        for field_name in ("control_api_base_url", "mailpit_api_base_url"):
            if field_name not in masked:
                continue
            candidate = masked[field_name]
            if isinstance(candidate, (bytes, bytearray, memoryview)):
                try:
                    candidate = bytes(candidate).decode("utf-8", "strict")
                except UnicodeDecodeError:
                    candidate = "invalid://"
            if not isinstance(candidate, (str, AnyHttpUrl)):
                masked[field_name] = "invalid://"
                continue
            try:
                parsed = _HTTP_URL_ADAPTER.validate_python(candidate)
            except (ValidationError, TypeError, ValueError):
                masked[field_name] = "invalid://"
                continue
            common_invalid = (
                parsed.username is not None
                or parsed.password is not None
                or parsed.query is not None
                or parsed.fragment is not None
            )
            mailpit_invalid = field_name == "mailpit_api_base_url" and (
                parsed.scheme != "http"
                or parsed.host != "mailpit"
                or parsed.port != 8025
                or parsed.path not in {"", "/"}
            )
            masked[field_name] = (
                "invalid://" if common_invalid or mailpit_invalid else parsed
            )
        return masked

    @field_validator("email_worker_service_credential")
    @classmethod
    def validate_credential(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if (
            len(raw) < 32
            or raw != raw.strip()
            or not raw.isascii()
            or any(character.isspace() for character in raw)
        ):
            raise ValueError(
                "email worker credential must be at least 32 non-whitespace ASCII characters"
            )
        return value

    @field_validator("redis_url")
    @classmethod
    def validate_redis_url(cls, value: SecretStr) -> SecretStr:
        message = (
            "Redis URL must be an unauthenticated redis:// URL for database 0 "
            "without query or fragment"
        )
        try:
            parsed = _REDIS_DSN_ADAPTER.validate_python(
                value.get_secret_value(), strict=True
            )
        except (ValidationError, TypeError, ValueError):
            raise ValueError(message) from None
        if (
            parsed.scheme != "redis"
            or parsed.path not in {None, "", "/", "/0"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query is not None
            or parsed.fragment is not None
        ):
            raise ValueError(message) from None
        return value

    @field_validator("control_api_base_url")
    @classmethod
    def validate_control_base(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username is not None or value.password is not None:
            raise ValueError("control API URL must not contain credentials")
        if value.query is not None or value.fragment is not None:
            raise ValueError("control API URL must not contain query or fragment")
        return value

    @field_validator("mailpit_api_base_url")
    @classmethod
    def validate_mailpit_base(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if (
            value.scheme != "http"
            or value.host != "mailpit"
            or value.port != 8025
            or value.username is not None
            or value.password is not None
            or value.path not in {"", "/"}
            or value.query is not None
            or value.fragment is not None
        ):
            raise ValueError("Mailpit API must be exactly http://mailpit:8025")
        return value

    @model_validator(mode="after")
    def validate_stale_window(self) -> Self:
        bounded_ms = math.ceil(self.max_work_seconds * 1_000)
        if self.stale_idle_ms <= bounded_ms + 100:
            raise ValueError(
                "stale_idle_ms must exceed the full claimed-batch control, Mailpit, SMTP, Redis work and a 100ms margin"
            )
        return self

    @property
    def max_work_seconds(self) -> float:
        # redis-py is configured with RESP2, DB 0, no authentication, health
        # checks, SETINFO, or retries. A disconnected command can therefore use
        # one connect timeout, one socket timeout while writing, and one socket
        # timeout while reading its sole response. The acquisition response can
        # consume one socket timeout after Redis makes this single entry pending.
        redis_command = (
            self.redis_connect_timeout_seconds + 2 * self.redis_socket_timeout_seconds
        )
        # The longest entry path has four control calls (claim/fetch and
        # mark/fetch), one Mailpit search, one SMTP transaction, and four Redis
        # commands (delivery count, two ownership renewals, then ACK/DLQ).
        entry_work = (
            4 * self.api_timeout_seconds
            + self.mailpit_api_timeout_seconds
            + self.smtp_timeout_seconds
            + 4 * redis_command
        )
        return self.redis_socket_timeout_seconds + entry_work
