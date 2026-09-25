from __future__ import annotations

from email import message_from_bytes, policy
import socket
from threading import Thread
import time
import traceback
from uuid import UUID, uuid4

import pytest

from signaldesk_email_worker.smtp_delivery import (
    MailpitSmtpTransport,
    MailRenderer,
    SmtpDelivery,
    SmtpAcceptanceUnknownError,
    SmtpDeliveryError,
    SmtpTerminalError,
    message_id_for_delivery,
)


class RecordingTransport:
    def __init__(self, refusals: dict[str, tuple[int, bytes]] | None = None) -> None:
        self.messages: list[tuple[str, tuple[str, ...], bytes]] = []
        self.refusals = refusals or {}

    def send(
        self, sender: str, recipients: tuple[str, ...], message: bytes
    ) -> dict[str, tuple[int, bytes]]:
        self.messages.append((sender, recipients, message))
        return self.refusals

    def close(self) -> None:
        pass


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


def test_renderer_and_smtp_use_only_authoritative_scope_with_deterministic_headers() -> (
    None
):
    delivery_id = uuid4()
    correlation_id = uuid4()
    transport = RecordingTransport()
    delivery = SmtpDelivery(transport=transport, max_message_bytes=16_384)
    rendered = MailRenderer(max_body_bytes=4_096).render(
        delivery_id=delivery_id,
        organization_id=uuid4(),
        correlation_id=correlation_id,
        template_name="diagnostic_completed",
        template_data={"diagnostic_job_id": str(uuid4()), "status": "completed"},
    )

    delivery.send(
        recipient="authoritative@example.test",
        delivery_id=delivery_id,
        correlation_id=correlation_id,
        subject=rendered.subject,
        body=rendered.body,
        message_id=message_id_for_delivery(delivery_id),
    )

    sender, recipients, raw = transport.messages[0]
    parsed = message_from_bytes(raw, policy=policy.default)
    assert sender == "notifications@signaldesk.test"
    assert recipients == ("authoritative@example.test",)
    assert parsed["To"] == "authoritative@example.test"
    assert parsed["From"] == "notifications@signaldesk.test"
    assert parsed["Message-ID"] == f"<{delivery_id}@signaldesk.local>"
    assert parsed["X-SignalDesk-Delivery-ID"] == str(delivery_id)
    assert parsed["X-SignalDesk-Correlation-ID"] == str(correlation_id)
    assert str(delivery_id) in parsed.get_content()
    assert str(correlation_id) in parsed.get_content()
    assert "credential" not in raw.decode().lower()


def _control_api_export_completion_payload() -> dict[str, object]:
    organization_id = "11111111-1111-4111-8111-111111111111"
    export_job_id = "55555555-5555-4555-8555-555555555555"
    return {
        "export_job_id": export_job_id,
        "status": "completed",
        "format": "csv",
        "object_key": f"exports/{organization_id}/{export_job_id}.csv",
        "object_sha256": "a" * 64,
        "size_bytes": 123,
    }


def test_renderer_accepts_exact_control_api_export_completion_payload() -> None:
    payload = _control_api_export_completion_payload()
    export = MailRenderer(max_body_bytes=4_096).render(
        delivery_id=uuid4(),
        organization_id=UUID("11111111-1111-4111-8111-111111111111"),
        correlation_id=uuid4(),
        template_name="export_completed",
        template_data=payload,
    )
    assert export.subject == "SignalDesk export completed"
    assert f"Export job {payload['export_job_id']} completed." in export.body
    assert "Format: csv" in export.body
    assert f"Object key: {payload['object_key']}" in export.body
    assert f"SHA-256: {'a' * 64}" in export.body
    assert "Size: 123 bytes" in export.body


def test_renderer_rejects_extra_field_in_control_api_export_completion_payload() -> (
    None
):
    payload = _control_api_export_completion_payload() | {"unexpected": "not permitted"}
    with pytest.raises(ValueError, match="template_data_invalid"):
        MailRenderer(max_body_bytes=4_096).render(
            delivery_id=uuid4(),
            organization_id=UUID("11111111-1111-4111-8111-111111111111"),
            correlation_id=uuid4(),
            template_name="export_completed",
            template_data=payload,
        )


@pytest.mark.parametrize(
    "object_key",
    [
        "exports/11111111-1111-4111-8111-111111111111/55555555-5555-4555-8555-555555555555.csv\r\nBcc: attacker@example.test",
        "exports/11111111-1111-4111-8111-111111111111/55555555-5555-4555-8555-555555555555.csv\u2028injected",
        "exports/11111111-1111-4111-8111-111111111111/55555555-5555-4555-8555-555555555555.csv\u2029injected",
        "exports/11111111-1111-4111-8111-111111111111/55555555-5555-4555-8555-555555555555.csv\x00injected",
        "exports/11111111-1111-4111-8111-111111111111/../55555555-5555-4555-8555-555555555555.csv",
        "exports\\11111111-1111-4111-8111-111111111111\\55555555-5555-4555-8555-555555555555.csv",
        "exports/11111111-1111-4111-8111-111111111111/%2e%2e/55555555-5555-4555-8555-555555555555.csv",
        "exports/11111111-1111-4111-8111-111111111111/66666666-6666-4666-8666-666666666666.csv",
        "exports/11111111-1111-4111-8111-111111111111/55555555-5555-4555-8555-555555555555.json",
        "exports/11111111-1111-4111-8111-111111111111/55555555-5555-4555-8555-555555555555.csv.json",
        "exports/11111111-1111-4111-8111-111111111111/./55555555-5555-4555-8555-555555555555.csv",
        "exports/11111111-1111-4111-8111-111111111111/55555555-5555-4555-8555-555555555555%2ecsv",
        "exports/11111111-1111-4111-8111-111111111111/55555555-5555-4555-8555-555555555555.CSV",
        "exports/11111111-1111-4111-8111-111111111111/55555555555545558555555555555555.csv",
        "exports/11111111-1111-4111-8111-11111111111A/55555555-5555-4555-8555-555555555555.csv",
    ],
)
def test_renderer_rejects_non_authoritative_export_object_key(object_key: str) -> None:
    payload = _control_api_export_completion_payload() | {"object_key": object_key}
    with pytest.raises(ValueError, match="template_data_invalid"):
        MailRenderer(max_body_bytes=4_096).render(
            delivery_id=uuid4(),
            organization_id=UUID("11111111-1111-4111-8111-111111111111"),
            correlation_id=uuid4(),
            template_name="export_completed",
            template_data=payload,
        )


def test_template_and_transport_values_are_absent_from_outward_exception_graphs() -> (
    None
):
    template_sentinel = "sensitive-template-value-sentinel"
    payload = _control_api_export_completion_payload() | {
        "object_key": template_sentinel
    }
    with pytest.raises(ValueError, match="template_data_invalid") as template_error:
        MailRenderer(max_body_bytes=4_096).render(
            delivery_id=uuid4(),
            organization_id=UUID("11111111-1111-4111-8111-111111111111"),
            correlation_id=uuid4(),
            template_name="export_completed",
            template_data=payload,
        )
    assert_sanitized_exception(template_error.value, template_sentinel)

    recipient_sentinel = "sensitive-recipient-sentinel"
    delivery_id = uuid4()
    with pytest.raises(SmtpTerminalError, match="recipient_invalid") as recipient_error:
        SmtpDelivery(transport=RecordingTransport()).send(
            recipient=f"victim@example.test\n{recipient_sentinel}",
            delivery_id=delivery_id,
            correlation_id=uuid4(),
            subject="safe",
            body="safe",
            message_id=message_id_for_delivery(delivery_id),
        )
    assert_sanitized_exception(recipient_error.value, recipient_sentinel)

    transport_sentinel = "sensitive-transport-sentinel"

    class ExplodingTransport(RecordingTransport):
        def send(
            self, sender: str, recipients: tuple[str, ...], message: bytes
        ) -> dict[str, tuple[int, bytes]]:
            raise RuntimeError(transport_sentinel)

    with pytest.raises(SmtpAcceptanceUnknownError) as transport_error:
        SmtpDelivery(transport=ExplodingTransport()).send(
            recipient="victim@example.test",
            delivery_id=delivery_id,
            correlation_id=uuid4(),
            subject="safe",
            body="safe",
            message_id=message_id_for_delivery(delivery_id),
        )
    assert_sanitized_exception(transport_error.value, transport_sentinel)


def test_renderer_allows_only_strict_bounded_templates_and_escapes_data() -> None:
    renderer = MailRenderer(max_body_bytes=4_096)
    with pytest.raises(ValueError):
        renderer.render(
            delivery_id=uuid4(),
            organization_id=uuid4(),
            correlation_id=uuid4(),
            template_name="../../template",
            template_data={},
        )
    with pytest.raises(ValueError):
        renderer.render(
            delivery_id=uuid4(),
            organization_id=uuid4(),
            correlation_id=uuid4(),
            template_name="diagnostic_completed",
            template_data={
                "diagnostic_job_id": str(uuid4()),
                "status": "completed",
                "secret": "x",
            },
        )


def test_recipient_and_refusals_fail_closed_with_sanitized_error() -> None:
    delivery_id = uuid4()
    with pytest.raises(SmtpDeliveryError, match="recipient_invalid"):
        SmtpDelivery(transport=RecordingTransport()).send(
            recipient="victim@example.test\nBcc: attacker@example.test",
            delivery_id=delivery_id,
            correlation_id=uuid4(),
            subject="safe",
            body="safe",
            message_id=message_id_for_delivery(delivery_id),
        )
    refusing = RecordingTransport({"victim@example.test": (550, b"sensitive detail")})
    with pytest.raises(SmtpDeliveryError) as caught:
        SmtpDelivery(transport=refusing).send(
            recipient="victim@example.test",
            delivery_id=delivery_id,
            correlation_id=uuid4(),
            subject="safe",
            body="safe",
            message_id=message_id_for_delivery(delivery_id),
        )
    assert str(caught.value) == "recipient_refused"
    assert "sensitive" not in str(caught.value)


class MiniSmtpServer:
    def __init__(
        self, *, delay_stage: str | None = None, reject_stage: str | None = None
    ) -> None:
        self.delay_stage = delay_stage
        self.reject_stage = reject_stage
        self.commands: list[bytes] = []
        self.body = bytearray()
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.port = self.listener.getsockname()[1]
        self.thread = Thread(target=self._run, daemon=True)
        self.thread.start()

    @staticmethod
    def _line(connection: socket.socket) -> bytes:
        data = bytearray()
        while not data.endswith(b"\n"):
            chunk = connection.recv(1)
            if not chunk:
                break
            data.extend(chunk)
        return bytes(data)

    def _reply(self, connection: socket.socket, stage: str, success: bytes) -> bool:
        if self.delay_stage == stage:
            time.sleep(0.25)
        try:
            if self.reject_stage == stage:
                connection.sendall(b"550 rejected sentinel\r\n")
                return False
            connection.sendall(success)
            return True
        except OSError:
            return False

    def _run(self) -> None:
        try:
            self.listener.settimeout(1)
            connection, _ = self.listener.accept()
            with connection:
                connection.settimeout(1)
                if not self._reply(connection, "greeting", b"220 mailpit ESMTP\r\n"):
                    return
                for stage, response in (
                    ("ehlo", b"250-mailpit\r\n250 SIZE 131072\r\n"),
                    ("mail", b"250 sender ok\r\n"),
                    ("rcpt", b"250 recipient ok\r\n"),
                    ("data", b"354 continue\r\n"),
                ):
                    command = self._line(connection)
                    if not command:
                        return
                    self.commands.append(command)
                    if not self._reply(connection, stage, response):
                        return
                while True:
                    line = self._line(connection)
                    if not line:
                        return
                    if line == b".\r\n":
                        break
                    self.body.extend(line)
                self._reply(connection, "final", b"250 queued\r\n")
        except (OSError, TimeoutError):
            pass
        finally:
            self.listener.close()

    def join(self) -> None:
        self.thread.join(timeout=1)


def _route_mailpit_dns(monkeypatch: pytest.MonkeyPatch, server: MiniSmtpServer) -> None:
    def address(host: str, port: int, **_kwargs):
        assert (host, port) == ("mailpit", 1025)
        return [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                ("127.0.0.1", server.port),
            )
        ]

    monkeypatch.setattr(socket, "getaddrinfo", address)


def test_mailpit_transport_constructor_does_not_resolve_or_connect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("resolved")),
    )
    transport = MailpitSmtpTransport(host="mailpit", port=1025, timeout=0.1)
    transport.close()


@pytest.mark.parametrize("stage", ["greeting", "mail", "data", "final"])
def test_whole_smtp_transaction_is_absolutely_bounded_and_cancelled(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    server = MiniSmtpServer(delay_stage=stage)
    _route_mailpit_dns(monkeypatch, server)
    transport = MailpitSmtpTransport(host="mailpit", port=1025, timeout=0.05)
    started = time.monotonic()
    with pytest.raises(SmtpAcceptanceUnknownError, match="smtp_acceptance_unknown"):
        transport.send(
            "notifications@signaldesk.test",
            ("recipient@example.test",),
            b"Subject: safe\r\n\r\nbody\r\n",
        )
    assert time.monotonic() - started < 0.2
    server.join()
    if stage == "greeting":
        assert server.commands == []
    if stage in {"mail", "data"}:
        assert not server.body


@pytest.mark.parametrize(
    ("stage", "code"),
    [
        ("mail", "sender_refused"),
        ("rcpt", "recipient_refused"),
        ("data", "data_refused"),
    ],
)
def test_definitive_non_2xx_smtp_outcomes_are_terminal(
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    code: str,
) -> None:
    server = MiniSmtpServer(reject_stage=stage)
    _route_mailpit_dns(monkeypatch, server)
    transport = MailpitSmtpTransport(host="mailpit", port=1025, timeout=0.5)
    with pytest.raises(SmtpTerminalError, match=code):
        transport.send(
            "notifications@signaldesk.test",
            ("recipient@example.test",),
            b"Subject: safe\r\n\r\nbody\r\n",
        )
    server.join()
