from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from email.message import EmailMessage
from email.policy import SMTP
import socket
from threading import Event, Lock, Thread
import time
from typing import Literal, Protocol
from uuid import UUID

from email_validator import EmailNotValidError, validate_email
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

SENDER = "notifications@signaldesk.test"
_MAX_REPLY_LINE_BYTES = 1_000
_MAX_REPLY_BYTES = 8_192
_MAX_REPLY_LINES = 32


class SmtpDeliveryError(RuntimeError):
    """Base class for sanitized, enumerated SMTP outcomes."""


class SmtpTerminalError(SmtpDeliveryError):
    """SMTP definitely did not accept this message and retry cannot help."""


class SmtpAcceptanceUnknownError(SmtpDeliveryError):
    """The connection ended without authoritative acceptance evidence."""


class MailTransport(Protocol):
    def send(
        self, sender: str, recipients: tuple[str, ...], message: bytes
    ) -> dict[str, tuple[int, bytes]]: ...

    def close(self) -> None: ...


class MailpitSmtpTransport:
    """One-shot, absolute-deadline SMTP transactions confined to Mailpit.

    DNS runs in a daemon transaction thread. On deadline, cancellation is set and
    any live socket is closed. A delayed resolver checks cancellation before it can
    create a socket, so it cannot deliver after the caller has timed out.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        timeout: float,
        max_message_bytes: int = 131_072,
    ) -> None:
        if host != "mailpit" or port != 1025:
            raise ValueError("SMTP transport is confined to mailpit:1025")
        if not 0 < timeout <= 30:
            raise ValueError("SMTP timeout must be between 0 and 30 seconds")
        if not 1_024 <= max_message_bytes <= 131_072:
            raise ValueError("max_message_bytes must be between 1024 and 131072")
        self._host = host
        self._port = port
        self._timeout = timeout
        self._max_message_bytes = max_message_bytes
        self._state_lock = Lock()
        self._send_lock = Lock()
        self._socket: socket.socket | None = None
        self._closed = False

    def send(
        self, sender: str, recipients: tuple[str, ...], message: bytes
    ) -> dict[str, tuple[int, bytes]]:
        if (
            sender != SENDER
            or len(recipients) != 1
            or not self._valid_mailbox(recipients[0])
        ):
            raise SmtpTerminalError("smtp_envelope_invalid")
        if len(message) > self._max_message_bytes:
            raise SmtpTerminalError("message_too_large")
        wire_message = self._dot_stuff(message)
        cancelled = Event()
        finished = Event()
        outcome: list[BaseException | None] = []
        deadline = time.monotonic() + self._timeout

        def transact() -> None:
            error: BaseException | None = None
            try:
                self._transaction(
                    sender=sender,
                    recipient=recipients[0],
                    wire_message=wire_message,
                    cancelled=cancelled,
                    deadline=deadline,
                )
            except SmtpDeliveryError as caught:
                error = caught
            except Exception:
                error = SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
            finally:
                self._abort_socket()
                outcome.append(error)
                finished.set()

        with self._send_lock:
            with self._state_lock:
                if self._closed:
                    raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
            thread = Thread(
                target=transact, name="mailpit-smtp-transaction", daemon=True
            )
            thread.start()
            remaining = max(0.0, deadline - time.monotonic())
            if not finished.wait(remaining):
                cancelled.set()
                self._abort_socket()
                raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown") from None
            if not outcome:
                raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
            error = outcome[0]
            if error is not None:
                raise error from None
            return {}

    @staticmethod
    def _valid_mailbox(value: str) -> bool:
        return (
            3 <= len(value) <= 320
            and value.isascii()
            and "\r" not in value
            and "\n" not in value
            and "@" in value
        )

    def _transaction(
        self,
        *,
        sender: str,
        recipient: str,
        wire_message: bytes,
        cancelled: Event,
        deadline: float,
    ) -> None:
        self._check_deadline(cancelled, deadline)
        addresses = socket.getaddrinfo(
            self._host,
            self._port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
        self._check_deadline(cancelled, deadline)
        if not addresses or len(addresses) > 8:
            raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
        connection: socket.socket | None = None
        for family, socktype, protocol, _canonical, address in addresses:
            self._check_deadline(cancelled, deadline)
            candidate = socket.socket(family, socktype, protocol)
            try:
                self._set_socket(candidate, cancelled)
                candidate.settimeout(self._remaining(deadline))
                candidate.connect(address)
                connection = candidate
                break
            except (OSError, TimeoutError):
                self._clear_socket(candidate)
                candidate.close()
        if connection is None:
            raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
        acceptance_unknown = False
        try:
            if self._read_reply(connection, cancelled, deadline) != 220:
                raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
            self._send_command(
                connection, b"EHLO signaldesk.local\r\n", cancelled, deadline
            )
            if self._read_reply(connection, cancelled, deadline) != 250:
                self._send_command(
                    connection, b"HELO signaldesk.local\r\n", cancelled, deadline
                )
                if self._read_reply(connection, cancelled, deadline) != 250:
                    raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
            self._send_command(
                connection,
                f"MAIL FROM:<{sender}>\r\n".encode("ascii"),
                cancelled,
                deadline,
            )
            if self._read_reply(connection, cancelled, deadline) != 250:
                raise SmtpTerminalError("sender_refused")
            self._send_command(
                connection,
                f"RCPT TO:<{recipient}>\r\n".encode("ascii"),
                cancelled,
                deadline,
            )
            if self._read_reply(connection, cancelled, deadline) not in {250, 251}:
                raise SmtpTerminalError("recipient_refused")
            self._send_command(connection, b"DATA\r\n", cancelled, deadline)
            if self._read_reply(connection, cancelled, deadline) != 354:
                raise SmtpTerminalError("data_refused")
            self._send_command(connection, wire_message, cancelled, deadline)
            if self._read_reply(connection, cancelled, deadline) != 250:
                raise SmtpTerminalError("data_refused")
        except SmtpDeliveryError:
            raise
        except (OSError, TimeoutError, ValueError):
            acceptance_unknown = True
        finally:
            self._clear_socket(connection)
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()
        if acceptance_unknown:
            raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")

    def _set_socket(self, connection: socket.socket, cancelled: Event) -> None:
        with self._state_lock:
            if self._closed or cancelled.is_set():
                connection.close()
                raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
            self._socket = connection

    def _clear_socket(self, connection: socket.socket) -> None:
        with self._state_lock:
            if self._socket is connection:
                self._socket = None

    def _abort_socket(self) -> None:
        with self._state_lock:
            connection = self._socket
            self._socket = None
        if connection is not None:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                connection.close()
            except OSError:
                pass

    @staticmethod
    def _remaining(deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        return remaining

    @classmethod
    def _check_deadline(cls, cancelled: Event, deadline: float) -> None:
        if cancelled.is_set():
            raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
        cls._remaining(deadline)

    @classmethod
    def _send_command(
        cls,
        connection: socket.socket,
        content: bytes,
        cancelled: Event,
        deadline: float,
    ) -> None:
        cls._check_deadline(cancelled, deadline)
        connection.settimeout(cls._remaining(deadline))
        connection.sendall(content)
        cls._check_deadline(cancelled, deadline)

    @classmethod
    def _read_line(
        cls, connection: socket.socket, cancelled: Event, deadline: float
    ) -> bytes:
        line = bytearray()
        while not line.endswith(b"\n"):
            cls._check_deadline(cancelled, deadline)
            connection.settimeout(cls._remaining(deadline))
            chunk = connection.recv(1)
            if not chunk:
                raise OSError("SMTP disconnected")
            line.extend(chunk)
            if len(line) > _MAX_REPLY_LINE_BYTES:
                raise ValueError("SMTP response line too large")
        if not line.endswith(b"\r\n"):
            raise ValueError("malformed SMTP response")
        return bytes(line)

    @classmethod
    def _read_reply(
        cls, connection: socket.socket, cancelled: Event, deadline: float
    ) -> int:
        total = 0
        lines = 0
        expected_code: bytes | None = None
        while True:
            line = cls._read_line(connection, cancelled, deadline)
            total += len(line)
            lines += 1
            if total > _MAX_REPLY_BYTES or lines > _MAX_REPLY_LINES:
                raise ValueError("SMTP response too large")
            if len(line) < 5 or not line[:3].isdigit() or line[3:4] not in {b" ", b"-"}:
                raise ValueError("malformed SMTP response")
            if expected_code is None:
                expected_code = line[:3]
            elif line[:3] != expected_code:
                raise ValueError("inconsistent SMTP response")
            if line[3:4] == b" ":
                return int(expected_code)

    def _dot_stuff(self, message: bytes) -> bytes:
        if b"\x00" in message or b"\n" in message.replace(b"\r\n", b""):
            raise SmtpTerminalError("message_invalid")
        lines = message.split(b"\r\n")
        stuffed = b"\r\n".join(
            (b"." + line) if line.startswith(b".") else line for line in lines
        )
        if not stuffed.endswith(b"\r\n"):
            stuffed += b"\r\n"
        wire = stuffed + b".\r\n"
        if len(wire) > (2 * self._max_message_bytes + 5):
            raise SmtpTerminalError("message_too_large")
        return wire

    def close(self) -> None:
        with self._state_lock:
            self._closed = True
        self._abort_socket()


def message_id_for_delivery(delivery_id: UUID) -> str:
    return f"<{delivery_id}@signaldesk.local>"


class _DiagnosticTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    diagnostic_job_id: str
    status: Literal["completed"]

    @field_validator("diagnostic_job_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("invalid identifier")
        return value


class _ExportTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    export_job_id: str
    status: Literal["completed"]
    format: Literal["csv", "json"]
    object_key: str = Field(min_length=1, max_length=1024)
    object_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0, le=1_073_741_824)

    @field_validator("export_job_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("invalid identifier")
        return value

    @model_validator(mode="after")
    def validate_object_key(self) -> "_ExportTemplate":
        prefix = "exports/"
        if not self.object_key.startswith(prefix):
            raise ValueError("invalid object key")
        organization_text, separator, filename = self.object_key[
            len(prefix) :
        ].partition("/")
        try:
            organization_id = UUID(organization_text)
        except ValueError:
            organization_id = None
        if (
            separator != "/"
            or organization_id is None
            or str(organization_id) != organization_text
            or filename != f"{self.export_job_id}.{self.format}"
        ):
            raise ValueError("invalid object key")
        return self


@dataclass(frozen=True)
class RenderedMail:
    subject: str
    body: str


class MailRenderer:
    def __init__(self, *, max_body_bytes: int = 8_192) -> None:
        if not 256 <= max_body_bytes <= 65_536:
            raise ValueError("max_body_bytes must be between 256 and 65536")
        self._max_body_bytes = max_body_bytes

    def render(
        self,
        *,
        delivery_id: UUID,
        organization_id: UUID,
        correlation_id: UUID,
        template_name: str,
        template_data: Mapping[str, object],
    ) -> RenderedMail:
        if template_name == "diagnostic_completed":
            diagnostic: _DiagnosticTemplate | None = None
            try:
                diagnostic = _DiagnosticTemplate.model_validate(template_data)
            except ValidationError:
                pass
            if diagnostic is None:
                raise ValueError("template_data_invalid")
            subject = "SignalDesk diagnostic completed"
            result = f"Diagnostic job {diagnostic.diagnostic_job_id} completed."
        elif template_name == "export_completed":
            export: _ExportTemplate | None = None
            try:
                export = _ExportTemplate.model_validate(template_data)
            except ValidationError:
                pass
            if export is None:
                raise ValueError("template_data_invalid")
            expected_key = (
                f"exports/{organization_id}/{export.export_job_id}.{export.format}"
            )
            if export.object_key != expected_key:
                raise ValueError("template_data_invalid")
            subject = "SignalDesk export completed"
            result = (
                f"Export job {export.export_job_id} completed.\n"
                f"Format: {export.format}\n"
                f"Object key: {export.object_key}\n"
                f"SHA-256: {export.object_sha256}\n"
                f"Size: {export.size_bytes} bytes"
            )
        else:
            raise ValueError("template_not_allowed")
        body = (
            "SignalDesk notification\n\n"
            f"{result}\n"
            f"Delivery ID: {delivery_id}\n"
            f"Correlation ID: {correlation_id}\n"
        )
        if len(body.encode("utf-8")) > self._max_body_bytes:
            raise ValueError("rendered_body_too_large")
        return RenderedMail(subject=subject, body=body)


class SmtpDelivery:
    def __init__(
        self,
        *,
        transport: MailTransport,
        max_message_bytes: int = 32_768,
    ) -> None:
        if not 1_024 <= max_message_bytes <= 131_072:
            raise ValueError("max_message_bytes must be between 1024 and 131072")
        self._transport = transport
        self._max_message_bytes = max_message_bytes

    def send(
        self,
        *,
        recipient: str,
        delivery_id: UUID,
        correlation_id: UUID,
        subject: str,
        body: str,
        message_id: str,
    ) -> None:
        expected_message_id = message_id_for_delivery(delivery_id)
        if message_id != expected_message_id:
            raise SmtpTerminalError("message_id_invalid")
        validated = None
        try:
            validated = validate_email(
                recipient,
                check_deliverability=False,
                globally_deliverable=False,
                test_environment=True,
            )
        except EmailNotValidError:
            pass
        if validated is None:
            raise SmtpTerminalError("recipient_invalid")
        if (
            validated.normalized != recipient
            or validated.ascii_email is None
            or len(recipient) > 320
            or not recipient.isascii()
        ):
            raise SmtpTerminalError("recipient_invalid")
        message = EmailMessage(policy=SMTP)
        message["From"] = SENDER
        message["To"] = recipient
        message["Subject"] = subject
        message["Message-ID"] = message_id
        message["X-SignalDesk-Delivery-ID"] = str(delivery_id)
        message["X-SignalDesk-Correlation-ID"] = str(correlation_id)
        message.set_content(body)
        raw = message.as_bytes(policy=SMTP)
        if len(raw) > self._max_message_bytes:
            raise SmtpTerminalError("message_too_large")
        refusals: dict[str, tuple[int, bytes]] | None = None
        try:
            refusals = self._transport.send(SENDER, (recipient,), raw)
        except SmtpDeliveryError:
            raise
        except Exception:
            pass
        if refusals is None:
            raise SmtpAcceptanceUnknownError("smtp_acceptance_unknown")
        if refusals:
            raise SmtpTerminalError("recipient_refused")

    def close(self) -> None:
        self._transport.close()
