from __future__ import annotations

import hashlib
from threading import Event
from typing import Any
from uuid import UUID, uuid4

from pydantic import ValidationError
from redis.exceptions import ResponseError
from signaldesk_contracts import EmailRequestedV1

from signaldesk_email_worker.control_client import (
    ControlApiClient,
    DeliveryScope,
    NotFoundError,
    StateConflict,
    TransientControlError,
)
from signaldesk_email_worker.mailpit_client import (
    MailpitAnomalyError,
    MailpitClient,
    MailpitTransientError,
)
from signaldesk_email_worker.settings import Settings
from signaldesk_email_worker.smtp_delivery import (
    MailRenderer,
    SmtpAcceptanceUnknownError,
    SmtpDelivery,
    SmtpDeliveryError,
    SmtpTerminalError,
)


class TopologyError(RuntimeError):
    pass


_ACK_SCRIPT = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then return {0, 'ownership_lost'} end
if pending[1][2] ~= ARGV[3] or tostring(pending[1][4]) ~= ARGV[4] then
  return {0, 'ownership_lost'}
end
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged ~= 1 then return {-1, 'ack_failed'} end
return {1, 'acknowledged'}
"""

_RENEW_SCRIPT = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending ~= 1 then return {0, 'ownership_lost'} end
if pending[1][2] ~= ARGV[3] or tostring(pending[1][4]) ~= ARGV[4] then
  return {0, 'ownership_lost'}
end
local renewed = redis.call(
  'XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2],
  'IDLE', 0, 'RETRYCOUNT', ARGV[4], 'JUSTID'
)
if #renewed ~= 1 or renewed[1] ~= ARGV[2] then return {-1, 'renew_failed'} end
return {1, 'renewed'}
"""

_DLQ_SCRIPT = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then return {0, 'ownership_lost'} end
if pending[1][2] ~= ARGV[3] or tostring(pending[1][4]) ~= ARGV[4] then
  return {0, 'ownership_lost'}
end
local dlq_id = redis.call(
  'XADD', KEYS[2], '*',
  'event', ARGV[5],
  'event_id', ARGV[6],
  'failure_code', ARGV[7],
  'source_stream', ARGV[8],
  'source_id', ARGV[2],
  'attempt_count', ARGV[4]
)
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged ~= 1 then return {-1, 'ack_failed'} end
return {1, 'dead_lettered'}
"""


class EmailConsumer:
    """Crash-safe identifier-only email worker with authoritative reconciliation."""

    def __init__(
        self,
        *,
        settings: Settings,
        redis_client: Any,
        control_client: ControlApiClient,
        mailpit_client: MailpitClient,
        smtp_delivery: SmtpDelivery,
        renderer: MailRenderer,
        stop_event: Event | None = None,
    ) -> None:
        self.settings = settings
        self.redis = redis_client
        self.control = control_client
        self.mailpit = mailpit_client
        self.smtp = smtp_delivery
        self.renderer = renderer
        self.stop_event = stop_event or Event()
        # The configured name is an operator label, not a reusable ownership token.
        self.consumer_identity = f"{settings.consumer_name}:{uuid4().hex}"
        self._ready = False
        self._autoclaim_cursor: bytes | str = "0-0"

    def setup(self) -> None:
        cluster = self.redis.info("cluster")
        enabled = cluster.get("cluster_enabled", cluster.get(b"cluster_enabled"))
        if enabled not in {0, "0", b"0"}:
            raise TopologyError("email worker requires standalone Redis")
        role = self.redis.role()
        role_name = role[0] if role else None
        if role_name not in {"master", b"master"}:
            raise TopologyError("email worker requires standalone primary Redis")
        try:
            self.redis.xgroup_create(
                self.settings.stream_name,
                self.settings.consumer_group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise
        self._ready = True

    def run(self) -> None:
        if not self._ready:
            self.setup()
        while not self.stop_event.is_set():
            self.process_once()

    def process_once(self) -> int:
        if not self._ready:
            raise RuntimeError("consumer setup is required")
        reclaimed = self.redis.xautoclaim(
            self.settings.stream_name,
            self.settings.consumer_group,
            self.consumer_identity,
            min_idle_time=self.settings.stale_idle_ms,
            start_id=self._autoclaim_cursor,
            count=self.settings.batch_size,
        )
        if reclaimed:
            self._autoclaim_cursor = reclaimed[0]
        entries = reclaimed[1] if reclaimed and len(reclaimed) > 1 else []
        if not entries:
            streams = self.redis.xreadgroup(
                self.settings.consumer_group,
                self.consumer_identity,
                {self.settings.stream_name: ">"},
                count=self.settings.batch_size,
                block=self.settings.block_time_ms,
            )
            entries = streams[0][1] if streams else []
        processed = 0
        for entry_id, fields in entries:
            if self.stop_event.is_set():
                break
            self._process_entry(entry_id, fields)
            processed += 1
        return processed

    def _process_entry(self, entry_id: bytes | str, fields: dict[Any, Any]) -> None:
        attempts = self._delivery_count(entry_id)
        if attempts is None:
            return
        raw_fields = {
            self._decode_field_name(key): self._field_bytes(value)
            for key, value in fields.items()
        }
        raw_event = raw_fields.get("event", b"")
        event: EmailRequestedV1 | None = None
        failure: str | None = None
        if set(raw_fields) != {"event", "event_id"}:
            failure = "invalid_stream_fields"
        elif len(raw_event) > self.settings.event_max_bytes:
            failure = "oversized_event"
        elif len(raw_fields["event_id"]) > 64:
            failure = "invalid_event_id"
        else:
            try:
                event = EmailRequestedV1.model_validate_json(raw_event)
            except (ValidationError, ValueError):
                failure = "invalid_event"
            if event is not None:
                try:
                    field_event_id = UUID(raw_fields["event_id"].decode("ascii"))
                except (UnicodeDecodeError, ValueError):
                    failure = "invalid_event_id"
                else:
                    if field_event_id != event.event_id:
                        failure = "event_id_mismatch"
        if failure is not None:
            self._dead_letter(entry_id, raw_event, event, failure, attempts)
            return
        assert event is not None

        recovery = False
        scope: DeliveryScope | None = None
        claim_failure: str | None = None
        try:
            scope = self.control.claim(event.email_delivery_id)
        except NotFoundError:
            claim_failure = "not_found"
        except StateConflict:
            claim_failure = "state_conflict"
        except TransientControlError:
            claim_failure = "transient"
        if claim_failure == "not_found":
            self._dead_letter(
                entry_id, raw_event, event, "delivery_not_found", attempts
            )
            return
        if claim_failure == "transient":
            self._retry_or_dead_letter(
                entry_id, raw_event, event, "control_transient", attempts
            )
            return
        if claim_failure == "state_conflict":
            recovery = True
            fetch_failure: str | None = None
            try:
                scope = self.control.fetch(event.email_delivery_id)
            except NotFoundError:
                fetch_failure = "not_found"
            except (StateConflict, TransientControlError):
                fetch_failure = "transient"
            if fetch_failure == "not_found":
                self._dead_letter(
                    entry_id, raw_event, event, "delivery_not_found", attempts
                )
                return
            if fetch_failure == "transient":
                self._retry_or_dead_letter(
                    entry_id, raw_event, event, "control_transient", attempts
                )
                return
        if scope is None:
            raise RuntimeError("control API request ended without delivery scope")

        if not self._scope_matches(event, scope):
            self._dead_letter(entry_id, raw_event, event, "scope_mismatch", attempts)
            return
        if scope.status == "sent":
            self._ack(entry_id, attempts)
            return
        if scope.status == "failed":
            self._dead_letter(entry_id, raw_event, event, "delivery_failed", attempts)
            return
        if scope.status != "sending" or scope.attempted_at is None:
            self._retry_or_dead_letter(
                entry_id, raw_event, event, "state_conflict", attempts
            )
            return
        if recovery:
            age = (scope.observed_at - scope.attempted_at).total_seconds()
            if age < self.settings.recovery_age_seconds:
                return

        rendered = None
        try:
            rendered = self.renderer.render(
                delivery_id=scope.email_delivery_id,
                organization_id=scope.organization_id,
                correlation_id=scope.correlation_id,
                template_name=scope.template_name,
                template_data=scope.template_data,
            )
        except ValueError:
            pass
        if rendered is None:
            self._record_failed_and_dead_letter(
                entry_id, raw_event, event, "invalid_authority", attempts
            )
            return
        try:
            found = self.mailpit.contains_message_id(scope.message_id)
        except (MailpitTransientError, MailpitAnomalyError):
            # SMTP acceptance may already have happened. Never resend or DLQ without
            # a successful authoritative absence proof.
            return
        if found:
            self._record_sent_and_ack(entry_id, event, attempts)
            return
        # This atomic same-owner renewal is deliberately adjacent to the
        # authoritative absence proof and the irreversible SMTP transaction.
        if not self._renew_ownership(entry_id, attempts):
            return
        terminal_smtp_failure = False
        try:
            self.smtp.send(
                recipient=scope.recipient_email,
                delivery_id=scope.email_delivery_id,
                correlation_id=scope.correlation_id,
                subject=rendered.subject,
                body=rendered.body,
                message_id=scope.message_id,
            )
        except SmtpTerminalError:
            terminal_smtp_failure = True
        except (SmtpAcceptanceUnknownError, SmtpDeliveryError):
            # A disconnect after DATA may hide acceptance; reconciliation must run
            # before any later retry.
            return
        if terminal_smtp_failure:
            self._record_failed_and_dead_letter(
                entry_id, raw_event, event, "smtp_terminal", attempts
            )
            return
        self._record_sent_and_ack(entry_id, event, attempts)

    def _record_sent_and_ack(
        self, entry_id: bytes | str, event: EmailRequestedV1, attempts: int
    ) -> None:
        if not self._renew_ownership(entry_id, attempts):
            return
        marked: DeliveryScope | None = None
        reconcile = False
        try:
            marked = self.control.mark_sent(event.email_delivery_id)
        except StateConflict:
            reconcile = True
        except (NotFoundError, TransientControlError):
            return
        if reconcile:
            try:
                marked = self.control.fetch(event.email_delivery_id)
            except (NotFoundError, StateConflict, TransientControlError):
                return
        if marked is None:
            raise RuntimeError("control API request ended without delivery scope")
        if self._scope_matches(event, marked) and marked.status == "sent":
            self._ack(entry_id, attempts)

    def _record_failed_and_dead_letter(
        self,
        entry_id: bytes | str,
        raw_event: bytes,
        event: EmailRequestedV1,
        failure_code: str,
        attempts: int,
    ) -> None:
        if not self._renew_ownership(entry_id, attempts):
            return
        marked: DeliveryScope | None = None
        reconcile = False
        try:
            marked = self.control.mark_failed(event.email_delivery_id)
        except (StateConflict, TransientControlError):
            reconcile = True
        except NotFoundError:
            return
        if reconcile:
            try:
                marked = self.control.fetch(event.email_delivery_id)
            except (NotFoundError, StateConflict, TransientControlError):
                return
        if marked is None:
            raise RuntimeError("control API request ended without delivery scope")
        if self._scope_matches(event, marked) and marked.status == "failed":
            self._dead_letter(entry_id, raw_event, event, failure_code, attempts)

    @staticmethod
    def _scope_matches(event: EmailRequestedV1, scope: DeliveryScope) -> bool:
        return (
            scope.email_delivery_id == event.email_delivery_id
            and scope.organization_id == event.organization_id
            and scope.correlation_id == event.correlation_id
            and scope.message_id == f"<{event.email_delivery_id}@signaldesk.local>"
        )

    def _delivery_count(self, entry_id: bytes | str) -> int | None:
        pending = self.redis.xpending_range(
            self.settings.stream_name,
            self.settings.consumer_group,
            min=entry_id,
            max=entry_id,
            count=1,
            consumername=self.consumer_identity,
        )
        if not pending:
            return None
        record = pending[0]
        return max(
            1, int(record.get("times_delivered", record.get(b"times_delivered", 1)))
        )

    def _retry_or_dead_letter(
        self,
        entry_id: bytes | str,
        raw_event: bytes,
        event: EmailRequestedV1,
        failure_code: str,
        attempts: int,
    ) -> None:
        if attempts >= self.settings.max_deliveries:
            self._dead_letter(entry_id, raw_event, event, failure_code, attempts)

    def _renew_ownership(self, entry_id: bytes | str, attempts: int) -> bool:
        source_id = (
            entry_id.decode("ascii", "strict")
            if isinstance(entry_id, bytes)
            else entry_id
        )
        result = self.redis.eval(
            _RENEW_SCRIPT,
            1,
            self.settings.stream_name,
            self.settings.consumer_group,
            source_id,
            self.consumer_identity,
            str(attempts),
        )
        return self._parse_atomic_result(result, "renewed", "renew ownership of")

    def _ack(self, entry_id: bytes | str, attempts: int) -> bool:
        source_id = (
            entry_id.decode("ascii", "strict")
            if isinstance(entry_id, bytes)
            else entry_id
        )
        result = self.redis.eval(
            _ACK_SCRIPT,
            1,
            self.settings.stream_name,
            self.settings.consumer_group,
            source_id,
            self.consumer_identity,
            str(attempts),
        )
        return self._parse_atomic_result(result, "acknowledged", "acknowledge")

    def _dead_letter(
        self,
        entry_id: bytes | str,
        raw_event: bytes,
        event: EmailRequestedV1 | None,
        failure_code: str,
        attempts: int,
    ) -> bool:
        source_id = (
            entry_id.decode("ascii", "strict")
            if isinstance(entry_id, bytes)
            else entry_id
        )
        if event is None:
            safe_event = "sha256:" + hashlib.sha256(raw_event).hexdigest()
            safe_event_id = "invalid"
        else:
            safe_event = event.model_dump_json()
            safe_event_id = str(event.event_id)
        result = self.redis.eval(
            _DLQ_SCRIPT,
            2,
            self.settings.stream_name,
            self.settings.dlq_stream_name,
            self.settings.consumer_group,
            source_id,
            self.consumer_identity,
            str(attempts),
            safe_event,
            safe_event_id,
            failure_code,
            self.settings.stream_name,
        )
        return self._parse_atomic_result(result, "dead_lettered", "dead-letter")

    @staticmethod
    def _parse_atomic_result(result: Any, success_token: str, operation: str) -> bool:
        message = f"Redis did not atomically {operation} the email event"
        if not isinstance(result, (list, tuple)) or len(result) != 2:
            raise RuntimeError(message)
        status, raw_token = result
        if type(status) is not int or not isinstance(raw_token, (bytes, str)):
            raise RuntimeError(message)
        token: str | None = None
        try:
            token = (
                raw_token.decode("ascii") if isinstance(raw_token, bytes) else raw_token
            )
        except UnicodeDecodeError:
            pass
        if token is None:
            raise RuntimeError(message)
        if status == 0 and token == "ownership_lost":
            return False
        if status == 1 and token == success_token:
            return True
        raise RuntimeError(message)

    @staticmethod
    def _decode_field_name(value: Any) -> str:
        if isinstance(value, bytes):
            try:
                return value.decode("ascii")
            except UnicodeDecodeError:
                return "<invalid>"
        return str(value)

    @staticmethod
    def _field_bytes(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8", "replace")
