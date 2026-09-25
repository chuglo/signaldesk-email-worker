from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from threading import Event, Thread
import time
import traceback
from typing import Any
from uuid import UUID, uuid4

import pytest
import redis
from signaldesk_contracts import EmailRequestedV1

from signaldesk_email_worker import consumer as consumer_module
from signaldesk_email_worker.consumer import EmailConsumer, TopologyError
from signaldesk_email_worker.control_client import (
    DeliveryScope,
    FatalCredentialError,
    StateConflict,
    TransientControlError,
)
from signaldesk_email_worker.mailpit_client import MailpitTransientError
from signaldesk_email_worker.settings import Settings
from signaldesk_email_worker.smtp_delivery import (
    MailRenderer,
    SmtpAcceptanceUnknownError,
    SmtpTerminalError,
)

CREDENTIAL = "email-worker-secret-value-0000001"


def make_settings(redis_url: str, consumer: str, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "redis_url": redis_url,
        "control_api_base_url": "https://control.example.test",
        "email_worker_service_credential": CREDENTIAL,
        "consumer_name": consumer,
        "block_time_ms": 10,
        "stale_idle_ms": 1_000,
        "api_timeout_seconds": 0.01,
        "mailpit_api_timeout_seconds": 0.01,
        "smtp_timeout_seconds": 0.01,
        "redis_connect_timeout_seconds": 0.01,
        "redis_socket_timeout_seconds": 0.01,
        "recovery_age_seconds": 0.1,
        "max_deliveries": 3,
    }
    values.update(overrides)
    return Settings(**values)


def requested_event(
    *,
    delivery_id: UUID | None = None,
    organization_id: UUID | None = None,
    correlation_id: UUID | None = None,
) -> EmailRequestedV1:
    return EmailRequestedV1(
        schema_version=1,
        event_id=uuid4(),
        event_type="email.requested.v1",
        occurred_at=datetime.now(timezone.utc),
        correlation_id=correlation_id or uuid4(),
        organization_id=organization_id or uuid4(),
        email_delivery_id=delivery_id or uuid4(),
    )


def scope(
    event: EmailRequestedV1, status: str = "sending", *, age: float = 0
) -> DeliveryScope:
    observed = datetime.now(timezone.utc)
    attempted = observed - timedelta(seconds=age)
    return DeliveryScope(
        email_delivery_id=event.email_delivery_id,
        organization_id=event.organization_id,
        recipient_email="authoritative@example.test",
        template_name="diagnostic_completed",
        template_data={"diagnostic_job_id": str(uuid4()), "status": "completed"},
        correlation_id=event.correlation_id,
        status=status,
        message_id=f"<{event.email_delivery_id}@signaldesk.local>",
        attempted_at=None if status == "pending" else attempted,
        sent_at=attempted if status == "sent" else None,
        observed_at=observed,
    )


def add_event(client: redis.Redis, event: EmailRequestedV1, **extra: str) -> bytes:
    fields = {"event": event.model_dump_json(), "event_id": str(event.event_id)} | extra
    return client.xadd("signaldesk:emails", fields)


class FakeControl:
    def __init__(
        self,
        claim: object,
        fetch: object | None = None,
        mark: object | None = None,
        failed: object | None = None,
    ) -> None:
        self.claim_result = claim
        self.fetch_result = fetch
        self.mark_result = mark
        self.failed_result = failed
        self.calls: list[str] = []

    @staticmethod
    def _result(value: object) -> Any:
        if isinstance(value, Exception):
            raise value
        return value

    def claim(self, _delivery_id: UUID) -> DeliveryScope:
        self.calls.append("claim")
        return self._result(self.claim_result)

    def fetch(self, _delivery_id: UUID) -> DeliveryScope:
        self.calls.append("fetch")
        return self._result(self.fetch_result)

    def mark_sent(self, _delivery_id: UUID) -> DeliveryScope:
        self.calls.append("mark_sent")
        return self._result(self.mark_result)

    def mark_failed(self, _delivery_id: UUID) -> DeliveryScope:
        self.calls.append("mark_failed")
        return self._result(self.failed_result)


class FakeMailpit:
    def __init__(self, *outcomes: object) -> None:
        self.outcomes = iter(outcomes)
        self.message_ids: list[str] = []

    def contains_message_id(self, message_id: str) -> bool:
        self.message_ids.append(message_id)
        outcome = next(self.outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return bool(outcome)


class RecordingDelivery:
    def __init__(self, outcome: Exception | None = None) -> None:
        self.sent: list[dict[str, object]] = []
        self.outcome = outcome

    def send(self, **kwargs: object) -> None:
        self.sent.append(kwargs)
        if self.outcome is not None:
            raise self.outcome


def consumer(
    redis_client: redis.Redis,
    redis_url: str,
    name: str,
    control: FakeControl,
    mailpit: FakeMailpit,
    delivery: RecordingDelivery,
    **settings: object,
) -> EmailConsumer:
    return EmailConsumer(
        settings=make_settings(redis_url, name, **settings),
        redis_client=redis_client,
        control_client=control,  # type: ignore[arg-type]
        mailpit_client=mailpit,  # type: ignore[arg-type]
        smtp_delivery=delivery,  # type: ignore[arg-type]
        renderer=MailRenderer(max_body_bytes=4096),
    )


def test_initial_claim_reconciles_then_sends_authoritative_scope_and_acks(
    redis_client: redis.Redis, redis_url: str
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    authoritative = scope(event)
    control = FakeControl(authoritative, mark=scope(event, "sent"))
    mailpit = FakeMailpit(False)
    smtp = RecordingDelivery()
    worker = consumer(redis_client, redis_url, "email-initial", control, mailpit, smtp)
    worker.setup()

    assert worker.process_once() == 1
    assert control.calls == ["claim", "mark_sent"]
    assert mailpit.message_ids == [authoritative.message_id]
    assert len(smtp.sent) == 1
    assert smtp.sent[0]["recipient"] == "authoritative@example.test"
    assert smtp.sent[0]["message_id"] == authoritative.message_id
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 0


@pytest.mark.parametrize("foreign_organization", [False, True])
def test_export_object_key_is_bound_to_authoritative_delivery_organization(
    redis_client: redis.Redis,
    redis_url: str,
    foreign_organization: bool,
) -> None:
    event = requested_event()
    export_job_id = uuid4()
    object_organization = uuid4() if foreign_organization else event.organization_id
    authoritative = scope(event).model_copy(
        update={
            "template_name": "export_completed",
            "template_data": {
                "export_job_id": str(export_job_id),
                "status": "completed",
                "format": "csv",
                "object_key": f"exports/{object_organization}/{export_job_id}.csv",
                "object_sha256": "a" * 64,
                "size_bytes": 42,
            },
        }
    )
    terminal_scope = scope(event, "failed" if foreign_organization else "sent")
    control = FakeControl(
        authoritative,
        mark=terminal_scope if not foreign_organization else None,
        failed=terminal_scope if foreign_organization else None,
    )
    smtp = RecordingDelivery()
    add_event(redis_client, event)
    worker = consumer(
        redis_client,
        redis_url,
        f"email-export-org-{foreign_organization}",
        control,
        FakeMailpit(False),
        smtp,
    )
    worker.setup()

    assert worker.process_once() == 1
    assert control.calls == (
        ["claim", "mark_failed"] if foreign_organization else ["claim", "mark_sent"]
    )
    assert len(smtp.sent) == (0 if foreign_organization else 1)
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 0
    assert redis_client.xlen("signaldesk:emails:dlq") == int(foreign_organization)


def test_smtp_accepted_then_sent_timeout_recovers_by_message_id_without_duplicate(
    redis_client: redis.Redis, redis_url: str
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    sending = scope(event)
    first_smtp = RecordingDelivery()
    first = consumer(
        redis_client,
        redis_url,
        "email-crash-a",
        FakeControl(sending, mark=TransientControlError("lost response")),
        FakeMailpit(False),
        first_smtp,
    )
    first.setup()
    first.process_once()
    assert len(first_smtp.sent) == 1
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 1
    time.sleep(1.05)

    recovered_smtp = RecordingDelivery()
    recovered_sending = sending.model_copy(
        update={"observed_at": datetime.now(timezone.utc)}
    )
    recovered_control = FakeControl(
        StateConflict("sending"), fetch=recovered_sending, mark=scope(event, "sent")
    )
    recovered = consumer(
        redis_client,
        redis_url,
        "email-crash-b",
        recovered_control,
        FakeMailpit(True),
        recovered_smtp,
    )
    recovered.setup()
    recovered.process_once()
    assert recovered_control.calls == ["claim", "fetch", "mark_sent"]
    assert recovered_smtp.sent == []
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 0


@pytest.mark.parametrize("case", ["unavailable", "recent", "stale", "sent"])
def test_recovery_resend_policy_is_fail_closed(
    redis_client: redis.Redis, redis_url: str, case: str
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    smtp = RecordingDelivery()
    if case == "sent":
        fetched = scope(event, "sent")
        mailpit = FakeMailpit()
        mark = None
    else:
        fetched = scope(event, age=2 if case == "stale" else 0)
        mailpit = FakeMailpit(
            MailpitTransientError("unavailable") if case == "unavailable" else False
        )
        mark = scope(event, "sent")
    control = FakeControl(StateConflict("state"), fetch=fetched, mark=mark)
    worker = consumer(
        redis_client,
        redis_url,
        f"email-recovery-{case}",
        control,
        mailpit,
        smtp,
        recovery_age_seconds=1.0,
        stale_idle_ms=2_000,
    )
    worker.setup()
    worker.process_once()

    if case in {"unavailable", "recent"}:
        assert smtp.sent == []
        assert (
            redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 1
        )
    elif case == "stale":
        assert len(smtp.sent) == 1
        assert (
            redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 0
        )
    else:
        assert smtp.sent == []
        assert (
            redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 0
        )


def test_recovery_age_uses_control_database_observation_not_worker_wall_clock(
    redis_client: redis.Redis,
    redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    recent = scope(event, age=0.01)

    class ExplodingLocalClock:
        @staticmethod
        def now(*_args, **_kwargs):
            raise AssertionError("worker wall clock must not authorize recovery")

    monkeypatch.setattr(consumer_module, "datetime", ExplodingLocalClock, raising=False)
    smtp = RecordingDelivery()
    worker = consumer(
        redis_client,
        redis_url,
        "email-server-clock",
        FakeControl(StateConflict("sending"), fetch=recent),
        FakeMailpit(False),
        smtp,
        recovery_age_seconds=1.0,
        stale_idle_ms=2_000,
    )
    worker.setup()
    worker.process_once()
    assert smtp.sent == []


@pytest.mark.parametrize("lost_response", [False, True])
def test_definite_terminal_smtp_failure_is_durably_failed_before_dlq(
    redis_client: redis.Redis,
    redis_url: str,
    lost_response: bool,
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    sending = scope(event)
    failed = scope(event, "failed")
    control = FakeControl(
        sending,
        fetch=failed,
        failed=(
            TransientControlError("lost failed response") if lost_response else failed
        ),
    )
    smtp = RecordingDelivery(SmtpTerminalError("recipient_refused"))
    worker = consumer(
        redis_client,
        redis_url,
        f"email-terminal-{lost_response}",
        control,
        FakeMailpit(False),
        smtp,
    )
    worker.setup()
    worker.process_once()
    assert control.calls == (
        ["claim", "mark_failed", "fetch"] if lost_response else ["claim", "mark_failed"]
    )
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 0
    assert redis_client.xlen("signaldesk:emails:dlq") == 1


def test_acceptance_unknown_smtp_failure_is_never_failed_or_dead_lettered(
    redis_client: redis.Redis,
    redis_url: str,
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    sending = scope(event)
    control = FakeControl(sending)
    worker = consumer(
        redis_client,
        redis_url,
        "email-unknown-smtp",
        control,
        FakeMailpit(False),
        RecordingDelivery(SmtpAcceptanceUnknownError("smtp_acceptance_unknown")),
    )
    worker.setup()
    worker.process_once()
    assert control.calls == ["claim"]
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 1
    assert redis_client.xlen("signaldesk:emails:dlq") == 0


def test_authoritative_failed_recovery_is_dead_lettered_without_smtp(
    redis_client: redis.Redis,
    redis_url: str,
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    smtp = RecordingDelivery()
    worker = consumer(
        redis_client,
        redis_url,
        "email-failed-recovery",
        FakeControl(StateConflict("failed"), fetch=scope(event, "failed")),
        FakeMailpit(),
        smtp,
    )
    worker.setup()
    worker.process_once()
    assert smtp.sent == []
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 0
    assert redis_client.xlen("signaldesk:emails:dlq") == 1


def test_extra_queue_authority_and_oversized_malformed_events_dlq_safely(
    redis_client: redis.Redis, redis_url: str
) -> None:
    event = requested_event()
    add_event(
        redis_client,
        event,
        recipient="attacker@example.test",
        template="forged",
        body="TOPSECRET",
    )
    redis_client.xadd(
        "signaldesk:emails", {"event": "X" * 9000, "event_id": str(uuid4())}
    )
    redis_client.xadd(
        "signaldesk:emails",
        {"event": '{"credential":"TOPSECRET"}', "event_id": str(uuid4())},
    )
    smtp = RecordingDelivery()
    worker = consumer(
        redis_client,
        redis_url,
        "email-poison",
        FakeControl(TransientControlError("unused")),
        FakeMailpit(),
        smtp,
    )
    worker.setup()
    for _ in range(3):
        assert worker.process_once() == 1
    assert smtp.sent == []
    records = redis_client.xrange("signaldesk:emails:dlq")
    assert len(records) == 3
    combined = b"".join(value for _, fields in records for value in fields.values())
    assert b"TOPSECRET" not in combined
    assert b"attacker" not in combined


def test_exact_max_delivery_moves_pre_smtp_control_failure_to_one_dlq(
    redis_client: redis.Redis, redis_url: str
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    first = consumer(
        redis_client,
        redis_url,
        "email-attempt-a",
        FakeControl(TransientControlError("unavailable")),
        FakeMailpit(),
        RecordingDelivery(),
        max_deliveries=2,
    )
    first.setup()
    first.process_once()
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 1
    time.sleep(1.05)
    second = consumer(
        redis_client,
        redis_url,
        "email-attempt-b",
        FakeControl(TransientControlError("unavailable")),
        FakeMailpit(),
        RecordingDelivery(),
        max_deliveries=2,
    )
    second.setup()
    second.process_once()
    second.process_once()
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 0
    assert redis_client.xlen("signaldesk:emails:dlq") == 1


def test_fatal_auth_is_left_pending_and_stop_prevents_next_item(
    redis_client: redis.Redis, redis_url: str
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    fatal = consumer(
        redis_client,
        redis_url,
        "email-fatal",
        FakeControl(FatalCredentialError("denied")),
        FakeMailpit(),
        RecordingDelivery(),
    )
    fatal.setup()
    with pytest.raises(FatalCredentialError):
        fatal.process_once()
    assert redis_client.xpending("signaldesk:emails", "email-workers")["pending"] == 1

    class BatchRedis:
        def xautoclaim(self, *_args: object, **_kwargs: object):
            return [b"0-0", [(b"1-0", {}), (b"2-0", {})], []]

    stop = Event()
    stopped = EmailConsumer(
        settings=make_settings("redis://redis.test:6379/0", "email-stop"),
        redis_client=BatchRedis(),  # type: ignore[arg-type]
        control_client=FakeControl(TransientControlError("unused")),  # type: ignore[arg-type]
        mailpit_client=FakeMailpit(),  # type: ignore[arg-type]
        smtp_delivery=RecordingDelivery(),  # type: ignore[arg-type]
        renderer=MailRenderer(),
        stop_event=stop,
    )
    stopped._ready = True
    processed: list[object] = []

    def process(entry_id: object, _fields: object) -> None:
        processed.append(entry_id)
        stop.set()

    stopped._process_entry = process  # type: ignore[method-assign]
    assert stopped.process_once() == 1
    assert processed == [b"1-0"]


def test_invalid_template_cannot_hide_sensitive_values_in_later_fatal_error(
    redis_client: redis.Redis, redis_url: str
) -> None:
    sentinel = "sensitive-authoritative-template-sentinel"
    event = requested_event()
    add_event(redis_client, event)
    invalid = scope(event).model_copy(
        update={
            "template_data": {
                "diagnostic_job_id": str(uuid4()),
                "status": "completed",
                "unexpected": sentinel,
            }
        }
    )
    worker = consumer(
        redis_client,
        redis_url,
        "email-template-fatal-chain",
        FakeControl(
            invalid,
            failed=FatalCredentialError("control API credential denied"),
        ),
        FakeMailpit(),
        RecordingDelivery(),
    )
    worker.setup()

    with pytest.raises(FatalCredentialError) as caught:
        worker.process_once()
    assert_sanitized_exception(caught.value, sentinel)


def assert_sanitized_exception(error: BaseException, sentinel: str) -> None:
    assert error.__cause__ is None
    assert error.__context__ is None
    renderings = [
        str(error),
        repr(error),
        "".join(traceback.format_exception(type(error), error, error.__traceback__)),
        repr(getattr(error, "__notes__", None)),
    ]
    errors = getattr(error, "errors", None)
    if callable(errors):
        renderings.append(repr(errors()))
    assert all(sentinel not in rendering for rendering in renderings)


def test_invalid_redis_result_token_is_absent_from_outward_exception_graph() -> None:
    sentinel = "sensitive-redis-result-sentinel"
    with pytest.raises(RuntimeError) as caught:
        EmailConsumer._parse_atomic_result(
            [1, b"\xff" + sentinel.encode()], "acknowledged", "acknowledge"
        )
    assert_sanitized_exception(caught.value, sentinel)


def test_reconciliation_does_not_attach_prior_control_error_to_fatal_error(
    redis_client: redis.Redis, redis_url: str
) -> None:
    sentinel = "sensitive-prior-control-error-sentinel"
    event = requested_event()
    add_event(redis_client, event)
    worker = consumer(
        redis_client,
        redis_url,
        "email-reconcile-fatal-chain",
        FakeControl(
            StateConflict(sentinel),
            fetch=FatalCredentialError("control API credential denied"),
        ),
        FakeMailpit(),
        RecordingDelivery(),
    )
    worker.setup()
    with pytest.raises(FatalCredentialError) as caught:
        worker.process_once()
    assert_sanitized_exception(caught.value, sentinel)


@pytest.mark.parametrize("current_action", ["ack", "dlq"])
def test_stale_owner_cannot_ack_or_dlq_after_reclaim(
    redis_client: redis.Redis, redis_url: str, current_action: str
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    stale = consumer(
        redis_client,
        redis_url,
        "email-owner-a",
        FakeControl(TransientControlError("unused")),
        FakeMailpit(),
        RecordingDelivery(),
    )
    current = consumer(
        redis_client,
        redis_url,
        "email-owner-b",
        FakeControl(TransientControlError("unused")),
        FakeMailpit(),
        RecordingDelivery(),
    )
    stale.setup()
    entry_id = redis_client.xreadgroup(
        "email-workers", stale.consumer_identity, {"signaldesk:emails": ">"}, count=1
    )[0][1][0][0]
    assert stale._delivery_count(entry_id) == 1
    redis_client.xautoclaim(
        "signaldesk:emails",
        "email-workers",
        current.consumer_identity,
        min_idle_time=0,
        start_id="0-0",
        count=1,
    )
    raw = event.model_dump_json().encode()
    assert not stale._ack(entry_id, 1)
    assert not stale._dead_letter(entry_id, raw, event, "stale", 1)
    if current_action == "ack":
        assert current._ack(entry_id, 2)
        assert not current._ack(entry_id, 2)
        assert redis_client.xlen("signaldesk:emails:dlq") == 0
    else:
        assert current._dead_letter(entry_id, raw, event, "current_terminal", 2)
        assert not current._dead_letter(entry_id, raw, event, "current_terminal", 2)
        assert redis_client.xlen("signaldesk:emails:dlq") == 1


def test_same_label_consumers_receive_distinct_bounded_process_identities(
    redis_client: redis.Redis, redis_url: str
) -> None:
    label = "a" * 128
    first = consumer(
        redis_client,
        redis_url,
        label,
        FakeControl(TransientControlError("unused")),
        FakeMailpit(),
        RecordingDelivery(),
    )
    second = consumer(
        redis_client,
        redis_url,
        label,
        FakeControl(TransientControlError("unused")),
        FakeMailpit(),
        RecordingDelivery(),
    )

    assert first.consumer_identity != second.consumer_identity
    assert re.fullmatch(rf"{label}:[0-9a-f]{{32}}", first.consumer_identity)
    assert len(first.consumer_identity.encode("ascii")) == 161
    assert CREDENTIAL not in first.consumer_identity
    assert redis_url not in first.consumer_identity


def test_same_label_reclaim_fences_stale_process_before_delivery_count(
    redis_client: redis.Redis, redis_url: str
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    stale_redis = redis.Redis.from_url(redis_url, decode_responses=False)
    current_redis = redis.Redis.from_url(redis_url, decode_responses=False)
    delivery_count_started = Event()
    resume_stale = Event()
    stale_errors: list[BaseException] = []
    eval_calls: list[tuple[object, ...]] = []
    stale_control = FakeControl(scope(event))
    stale_mailpit = FakeMailpit(False)
    stale_smtp = RecordingDelivery()
    stale = consumer(
        stale_redis,
        redis_url,
        "email-same-label",
        stale_control,
        stale_mailpit,
        stale_smtp,
    )
    current = consumer(
        current_redis,
        redis_url,
        "email-same-label",
        FakeControl(TransientControlError("unused")),
        FakeMailpit(),
        RecordingDelivery(),
    )
    original_pending = stale_redis.xpending_range
    original_eval = stale_redis.eval

    def paused_pending(*args: object, **kwargs: object):
        delivery_count_started.set()
        assert resume_stale.wait(timeout=2)
        return original_pending(*args, **kwargs)

    def recording_eval(*args: object, **kwargs: object):
        eval_calls.append(args)
        return original_eval(*args, **kwargs)

    stale_redis.xpending_range = paused_pending  # type: ignore[method-assign]
    stale_redis.eval = recording_eval  # type: ignore[method-assign]
    try:
        stale.setup()
        current.setup()

        def run_stale() -> None:
            try:
                stale.process_once()
            except BaseException as error:
                stale_errors.append(error)

        processing = Thread(target=run_stale)
        processing.start()
        assert delivery_count_started.wait(timeout=2)
        reclaimed = current_redis.xautoclaim(
            "signaldesk:emails",
            "email-workers",
            current.consumer_identity,
            min_idle_time=0,
            start_id="0-0",
            count=1,
        )
        assert len(reclaimed[1]) == 1
        resume_stale.set()
        processing.join(timeout=2)
        assert not processing.is_alive()

        assert stale_errors == []
        assert stale_control.calls == []
        assert stale_mailpit.message_ids == []
        assert stale_smtp.sent == []
        assert eval_calls == []
        pending = current_redis.xpending_range(
            "signaldesk:emails", "email-workers", min="-", max="+", count=1
        )[0]
        assert pending["consumer"] == current.consumer_identity.encode("ascii")
        assert pending["times_delivered"] == 2
    finally:
        resume_stale.set()
        stale_redis.close()
        current_redis.close()


def test_owner_renewal_preserves_exact_delivery_generation(
    redis_client: redis.Redis,
    redis_url: str,
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    worker = consumer(
        redis_client,
        redis_url,
        "email-renew-owner",
        FakeControl(TransientControlError("unused")),
        FakeMailpit(),
        RecordingDelivery(),
    )
    worker.setup()
    entry_id = redis_client.xreadgroup(
        "email-workers", worker.consumer_identity, {"signaldesk:emails": ">"}, count=1
    )[0][1][0][0]
    assert worker._delivery_count(entry_id) == 1
    assert worker._renew_ownership(entry_id, 1)
    pending = redis_client.xpending_range(
        "signaldesk:emails", "email-workers", min=entry_id, max=entry_id, count=1
    )[0]
    assert pending["consumer"] == worker.consumer_identity.encode("ascii")
    assert pending["times_delivered"] == 1


def test_reclaimed_stale_owner_cannot_send_after_absence_proof(
    redis_client: redis.Redis,
    redis_url: str,
) -> None:
    event = requested_event()
    add_event(redis_client, event)
    absence_started = Event()
    release_absence = Event()

    class BlockingAbsence:
        def contains_message_id(self, _message_id: str) -> bool:
            absence_started.set()
            assert release_absence.wait(timeout=2)
            return False

    smtp = RecordingDelivery()
    stale = consumer(
        redis_client,
        redis_url,
        "email-stale-sender-a",
        FakeControl(scope(event)),
        BlockingAbsence(),  # type: ignore[arg-type]
        smtp,
    )
    stale.setup()
    current = consumer(
        redis_client,
        redis_url,
        "email-stale-sender-b",
        FakeControl(TransientControlError("unused")),
        FakeMailpit(),
        RecordingDelivery(),
    )
    processing = Thread(target=stale.process_once)
    processing.start()
    assert absence_started.wait(timeout=2)
    reclaimed = redis_client.xautoclaim(
        "signaldesk:emails",
        "email-workers",
        current.consumer_identity,
        min_idle_time=0,
        start_id="0-0",
        count=1,
    )
    assert len(reclaimed[1]) == 1
    release_absence.set()
    processing.join(timeout=2)
    assert not processing.is_alive()
    assert smtp.sent == []
    pending = redis_client.xpending_range(
        "signaldesk:emails", "email-workers", min="-", max="+", count=1
    )[0]
    assert pending["consumer"] == current.consumer_identity.encode("ascii")


def test_xautoclaim_cursor_persists_across_polls() -> None:
    class CursorRedis:
        def __init__(self) -> None:
            self.starts: list[bytes | str] = []
            self.next_ids = iter([b"5-0", b"0-0"])

        def xautoclaim(self, *_args: object, start_id: bytes | str, **_kwargs: object):
            self.starts.append(start_id)
            return [next(self.next_ids), [], []]

        def xreadgroup(self, *_args: object, **_kwargs: object):
            return []

    broker = CursorRedis()
    worker = EmailConsumer(
        settings=make_settings("redis://redis.test:6379/0", "email-cursor"),
        redis_client=broker,
        control_client=FakeControl(TransientControlError("unused")),  # type: ignore[arg-type]
        mailpit_client=FakeMailpit(),  # type: ignore[arg-type]
        smtp_delivery=RecordingDelivery(),  # type: ignore[arg-type]
        renderer=MailRenderer(),
    )
    worker._ready = True
    assert worker.process_once() == 0
    assert worker.process_once() == 0
    assert broker.starts == ["0-0", b"5-0"]


def test_setup_rejects_non_primary_topology_before_group() -> None:
    class Replica:
        def info(self, _section: str):
            return {"cluster_enabled": 0}

        def role(self):
            return [b"slave"]

    worker = EmailConsumer(
        settings=make_settings("redis://redis.test:6379/0", "email-replica"),
        redis_client=Replica(),  # type: ignore[arg-type]
        control_client=FakeControl(TransientControlError("unused")),  # type: ignore[arg-type]
        mailpit_client=FakeMailpit(),  # type: ignore[arg-type]
        smtp_delivery=RecordingDelivery(),  # type: ignore[arg-type]
        renderer=MailRenderer(),
    )
    with pytest.raises(TopologyError):
        worker.setup()
