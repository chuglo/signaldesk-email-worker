from __future__ import annotations

import traceback

import pytest
from pydantic import SecretStr, ValidationError

from signaldesk_email_worker.settings import Settings


VALID = {
    "redis_url": "redis://localhost:6379/0",
    "control_api_base_url": "https://control.example.test",
    "email_worker_service_credential": "e" * 32,
    "consumer_name": "email-worker-01",
}


def test_settings_are_least_privilege_and_mailpit_confined() -> None:
    settings = Settings(**VALID)
    assert settings.stream_name == "signaldesk:emails"
    assert settings.consumer_group == "email-workers"
    assert settings.dlq_stream_name == "signaldesk:emails:dlq"
    assert settings.smtp_host == "mailpit"
    assert settings.smtp_port == 1025
    assert str(settings.mailpit_api_base_url) == "http://mailpit:8025/"
    assert settings.sender_email == "notifications@signaldesk.test"
    assert isinstance(settings.redis_url, SecretStr)
    assert "e" * 32 not in repr(settings)
    assert not set(Settings.model_fields) & {
        "web_bff_service_credential",
        "diagnostic_worker_service_credential",
        "export_worker_service_credential",
        "smtp_username",
        "smtp_password",
        "smtp_tls",
        "smtp_starttls",
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("smtp_host", "localhost"),
        ("smtp_host", "mailpit.example.test"),
        ("smtp_port", 25),
        ("mailpit_api_base_url", "https://mailpit:8025"),
        ("mailpit_api_base_url", "http://user:pass@mailpit:8025"),
        ("mailpit_api_base_url", "http://mailpit:8025/api"),
        ("mailpit_api_base_url", "http://mailpit:8025/?q=x"),
        ("control_api_base_url", "https://user:pass@control.example.test"),
        ("redis_url", "redis://localhost:6379/1"),
        ("redis_url", "rediss://localhost:6379/0"),
        ("consumer_name", "email worker"),
        ("email_worker_service_credential", "x" * 31),
    ],
)
def test_settings_reject_endpoint_escape_and_unsafe_identity(
    field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        Settings(**(VALID | {field: value}))


@pytest.mark.parametrize(
    ("redis_url", "secret"),
    [
        ("redis://:raw-password-secret@localhost:6379/0", "raw-password-secret"),
        (b"redis://:raw-bytes-secret@localhost:6379/0", "raw-bytes-secret"),
        (bytearray(b"redis://localhost:6379/0?raw-query-secret"), "raw-query-secret"),
        (
            memoryview(b"redis://localhost:6379/0#raw-fragment-secret"),
            "raw-fragment-secret",
        ),
    ],
)
def test_redis_secret_never_leaks_in_structured_errors(
    redis_url: object, secret: str
) -> None:
    with pytest.raises(ValidationError) as caught:
        Settings(**(VALID | {"redis_url": redis_url}))
    rendered = [
        str(caught.value),
        repr(caught.value),
        repr(caught.value.errors(include_input=True)),
        caught.value.json(include_input=True),
    ]
    assert all(secret not in item for item in rendered)


@pytest.mark.parametrize(
    ("field", "value", "secret"),
    [
        ("email_worker_service_credential", "credential-raw-sentinel", "raw-sentinel"),
        (
            "email_worker_service_credential",
            b"credential-bytes-sentinel",
            "bytes-sentinel",
        ),
        (
            "email_worker_service_credential",
            bytearray(b"credential-bytearray-sentinel"),
            "bytearray-sentinel",
        ),
        (
            "email_worker_service_credential",
            memoryview(b"credential-memoryview-sentinel"),
            "memoryview-sentinel",
        ),
        (
            "control_api_base_url",
            "https://url-user:url-password-sentinel@control.example.test",
            "url-password-sentinel",
        ),
        (
            "mailpit_api_base_url",
            "http://mailpit-user:mailpit-password-sentinel@mailpit:8025",
            "mailpit-password-sentinel",
        ),
    ],
)
def test_all_credential_bearing_settings_are_sanitized_before_structured_validation(
    field: str, value: object, secret: str
) -> None:
    with pytest.raises(ValidationError) as caught:
        Settings(**(VALID | {field: value}))
    rendered = [
        str(caught.value),
        repr(caught.value),
        repr(caught.value.errors(include_input=True)),
        caught.value.json(include_input=True),
        "".join(
            traceback.format_exception(
                type(caught.value), caught.value, caught.value.__traceback__
            )
        ),
    ]
    assert all(secret not in item for item in rendered)


def test_unsupported_redis_input_is_replaced_without_invoking_hostile_representations() -> (
    None
):
    calls: list[str] = []
    secret = "hostile-redis-object-secret-sentinel"

    class HostileRedisUrl:
        def __str__(self) -> str:
            calls.append("str")
            return secret

        def __repr__(self) -> str:
            calls.append("repr")
            return secret

        def __bytes__(self) -> bytes:
            calls.append("bytes")
            return secret.encode()

    with pytest.raises(ValidationError) as caught:
        Settings(**(VALID | {"redis_url": HostileRedisUrl()}))
    rendered = [
        str(caught.value),
        repr(caught.value),
        repr(caught.value.errors(include_input=True)),
        caught.value.json(include_input=True),
        "".join(
            traceback.format_exception(
                type(caught.value), caught.value, caught.value.__traceback__
            )
        ),
    ]
    assert calls == []
    assert all(secret not in item for item in rendered)


def test_unsupported_credential_input_is_replaced_before_validation() -> None:
    class HostileCredential:
        def __repr__(self) -> str:
            return "unsupported-object-secret-sentinel"

    with pytest.raises(ValidationError) as caught:
        Settings(**(VALID | {"email_worker_service_credential": HostileCredential()}))
    assert "unsupported-object-secret-sentinel" not in repr(
        caught.value.errors(include_input=True)
    )


def test_environment_secret_and_url_inputs_are_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = {
        "REDIS_URL": VALID["redis_url"],
        "CONTROL_API_BASE_URL": "https://env-user:env-url-secret-sentinel@control.example.test",
        "EMAIL_WORKER_SERVICE_CREDENTIAL": "env-credential-secret-sentinel",
        "CONSUMER_NAME": VALID["consumer_name"],
    }
    for key, value in environment.items():
        monkeypatch.setenv(f"SIGNALDESK_EMAIL_WORKER_{key}", value)
    with pytest.raises(ValidationError) as caught:
        Settings()  # type: ignore[call-arg]
    rendered = caught.value.json(include_input=True)
    assert "env-url-secret-sentinel" not in rendered
    assert "env-credential-secret-sentinel" not in rendered


def test_stale_window_exceeds_entire_claimed_batch_work_sequence() -> None:
    bounded = {
        "api_timeout_seconds": 1.0,
        "mailpit_api_timeout_seconds": 1.0,
        "smtp_timeout_seconds": 1.0,
        "redis_connect_timeout_seconds": 0.1,
        "redis_socket_timeout_seconds": 1.0,
        "recovery_age_seconds": 2.0,
        "batch_size": 1,
    }
    with pytest.raises(ValidationError, match="stale_idle_ms must exceed"):
        Settings(**(VALID | bounded | {"stale_idle_ms": 15_500}))
    settings = Settings(**(VALID | bounded | {"stale_idle_ms": 15_501}))
    assert settings.max_work_seconds == pytest.approx(15.4)
    assert settings.stale_idle_ms == 15_501


@pytest.mark.parametrize("batch_size", [0, 2, 100])
def test_batch_size_must_be_exactly_one(batch_size: int) -> None:
    tiny_timeouts = {
        "stale_idle_ms": 300_000,
        "redis_connect_timeout_seconds": 0.001,
        "redis_socket_timeout_seconds": 0.001,
        "api_timeout_seconds": 0.001,
        "mailpit_api_timeout_seconds": 0.001,
        "smtp_timeout_seconds": 0.001,
    }
    with pytest.raises(ValidationError) as caught:
        Settings(**(VALID | tiny_timeouts | {"batch_size": batch_size}))
    assert caught.value.errors()[0]["loc"] == ("batch_size",)


def test_hostile_timeout_configuration_cannot_exceed_stale_window_cap() -> None:
    with pytest.raises(ValidationError, match="stale_idle_ms must exceed"):
        Settings(
            **(
                VALID
                | {
                    "stale_idle_ms": 300_000,
                    "redis_connect_timeout_seconds": 10.0,
                    "redis_socket_timeout_seconds": 30.0,
                    "api_timeout_seconds": 30.0,
                    "mailpit_api_timeout_seconds": 30.0,
                    "smtp_timeout_seconds": 30.0,
                }
            )
        )


def test_default_claim_batch_fits_the_default_stale_ownership_window() -> None:
    settings = Settings(**VALID)
    assert settings.batch_size == 1
    assert settings.max_work_seconds == pytest.approx(137.0)
    assert settings.stale_idle_ms > settings.max_work_seconds * 1_000 + 100
