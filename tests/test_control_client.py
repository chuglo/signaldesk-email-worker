from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import select
import socket
from threading import Event, Thread
import time
import traceback
from uuid import uuid4

import httpx
import pytest

from signaldesk_email_worker import _deadline_http
from signaldesk_email_worker.control_client import (
    ControlApiClient,
    FatalCredentialError,
    NotFoundError,
    StateConflict,
    TransientControlError,
)

CREDENTIAL = "email-worker-secret-value-0000001"
DELIVERY_ID = uuid4()
ORG_ID = uuid4()
CORRELATION_ID = uuid4()
ATTEMPTED = datetime.now(timezone.utc)


def payload(status: str = "sending") -> dict[str, object]:
    return {
        "email_delivery_id": str(DELIVERY_ID),
        "organization_id": str(ORG_ID),
        "recipient_email": "authoritative@example.test",
        "template_name": "diagnostic_completed",
        "template_data": {"diagnostic_job_id": str(uuid4()), "status": "completed"},
        "correlation_id": str(CORRELATION_ID),
        "status": status,
        "message_id": f"<{DELIVERY_ID}@signaldesk.local>",
        "attempted_at": ATTEMPTED.isoformat() if status != "pending" else None,
        "sent_at": ATTEMPTED.isoformat() if status == "sent" else None,
        "observed_at": ATTEMPTED.isoformat(),
    }


def client(handler: object, cap: int = 16_384) -> ControlApiClient:
    return ControlApiClient(
        base_url="https://control.example.test",
        credential=CREDENTIAL,
        timeout=2,
        max_response_bytes=cap,
        transport=httpx.MockTransport(handler),  # type: ignore[arg-type]
    )


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


def test_claim_fetch_and_mark_sent_use_path_only_authority_and_strict_scope() -> None:
    requests: list[httpx.Request] = []
    statuses = iter(["sending", "sending", "sent", "failed"])

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=payload(next(statuses)))

    api = client(handler)
    assert api.claim(DELIVERY_ID).status == "sending"
    assert api.fetch(DELIVERY_ID).recipient_email == "authoritative@example.test"
    assert api.mark_sent(DELIVERY_ID).status == "sent"
    assert api.mark_failed(DELIVERY_ID).status == "failed"
    assert [request.method for request in requests] == ["POST", "GET", "POST", "POST"]
    assert [request.url.path for request in requests] == [
        f"/internal/email-deliveries/{DELIVERY_ID}/claim",
        f"/internal/email-deliveries/{DELIVERY_ID}",
        f"/internal/email-deliveries/{DELIVERY_ID}/sent",
        f"/internal/email-deliveries/{DELIVERY_ID}/failed",
    ]
    assert all(request.content == b"" for request in requests)
    assert all(request.headers["Accept-Encoding"] == "identity" for request in requests)


@pytest.mark.parametrize(
    "mutation",
    [
        {"extra": "forbidden"},
        {"email_delivery_id": str(uuid4())},
        {"message_id": "<wrong@signaldesk.local>"},
        {"recipient_email": "x" * 321},
        {"attempted_at": "2026-07-23T10:00:00"},
        {"status": "pending"},
        {"observed_at": "2026-07-23T10:00:00"},
    ],
)
def test_scope_rejects_extra_mismatched_or_invalid_authority(
    mutation: dict[str, object],
) -> None:
    api = client(lambda _request: httpx.Response(200, json=payload() | mutation))
    with pytest.raises(TransientControlError):
        api.claim(DELIVERY_ID)


@pytest.mark.parametrize(
    "status,error",
    [
        (401, FatalCredentialError),
        (403, FatalCredentialError),
        (404, NotFoundError),
        (409, StateConflict),
        (503, TransientControlError),
    ],
)
def test_statuses_are_typed_without_reading_error_bodies(
    status: int, error: type[Exception]
) -> None:
    class Exploding(httpx.SyncByteStream):
        def __iter__(self):
            raise AssertionError("error body must not be read")

    api = client(lambda _request: httpx.Response(status, stream=Exploding()))
    with pytest.raises(error) as caught:
        api.claim(DELIVERY_ID)
    if error is FatalCredentialError:
        assert_sanitized_exception(caught.value, CREDENTIAL)


def test_transport_errors_and_bounded_raw_success_are_sanitized() -> None:
    sentinel = "sensitive-control-detail"

    def fail(_request: httpx.Request) -> httpx.Response:
        raise httpx.RemoteProtocolError(sentinel)

    with pytest.raises(TransientControlError) as caught:
        client(fail).fetch(DELIVERY_ID)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert sentinel not in "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__
        )
    )

    oversized = client(
        lambda _request: httpx.Response(200, content=b"x" * 257), cap=256
    )
    with pytest.raises(TransientControlError, match="too large"):
        oversized.fetch(DELIVERY_ID)


@pytest.mark.parametrize("case", ["malformed_json", "invalid_model", "content_length"])
def test_rejected_control_response_values_are_absent_from_exception_graph(
    case: str,
) -> None:
    sentinel = f"sensitive-control-{case}-sentinel"

    def handler(_request: httpx.Request) -> httpx.Response:
        if case == "malformed_json":
            return httpx.Response(200, content=f'{{"value":"{sentinel}"'.encode())
        if case == "invalid_model":
            return httpx.Response(200, json=payload() | {"attempted_at": sentinel})
        return httpx.Response(
            200,
            json=payload(),
            headers={"content-length": sentinel},
        )

    with pytest.raises(TransientControlError) as caught:
        client(handler).fetch(DELIVERY_ID)
    assert_sanitized_exception(caught.value, sentinel)


def test_control_client_ignores_adversarial_proxy_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy-credential-sentinel@127.0.0.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy-credential-sentinel@127.0.0.1:9")
    monkeypatch.setenv("NO_PROXY", "")
    api = client(lambda _request: httpx.Response(200, json=payload()))
    assert api.fetch(DELIVERY_ID).status == "sending"
    assert api._client._trust_env is False


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_real_loopback_control_request_has_one_absolute_deadline_and_closes_peer(
    phase: str,
) -> None:
    timeout = 1.0
    body = json.dumps(payload(), separators=(",", ":")).encode()
    headers = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Transfer-Encoding: chunked\r\n"
        b"Connection: close\r\n\r\n"
    )
    step = max(1, len(body) // 6)
    body_parts = [body[offset : offset + step] for offset in range(0, len(body), step)]
    body_frames = [
        f"{len(part):x}\r\n".encode() + part + b"\r\n" for part in body_parts
    ] + [b"0\r\n\r\n"]
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(1)
    host, port = listener.getsockname()
    peer_closed = Event()
    server_errors: list[BaseException] = []

    def serve() -> None:
        try:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(1)
                request = bytearray()
                while b"\r\n\r\n" not in request:
                    chunk = connection.recv(4096)
                    if not chunk:
                        return
                    request.extend(chunk)
                if phase == "headers":
                    parts = [
                        b"HTTP/1.1 ",
                        b"200 ",
                        b"OK\r\n",
                        b"Content-",
                        b"Type: application/json\r\n",
                        b"Transfer-Encoding: chunked\r\n",
                        b"Connection: close\r\n",
                        b"\r\n",
                        b"".join(body_frames),
                    ]
                    for part in parts:
                        time.sleep(0.4)
                        if (
                            select.select([connection], [], [], 0)[0]
                            and connection.recv(1, socket.MSG_PEEK) == b""
                        ):
                            peer_closed.set()
                            return
                        connection.sendall(part)
                else:
                    connection.sendall(headers)
                    for frame in body_frames:
                        time.sleep(0.4)
                        if (
                            select.select([connection], [], [], 0)[0]
                            and connection.recv(1, socket.MSG_PEEK) == b""
                        ):
                            peer_closed.set()
                            return
                        connection.sendall(frame)
                peer_closed.set() if connection.recv(1) == b"" else None
        except OSError:
            peer_closed.set()
        except BaseException as error:
            server_errors.append(error)
        finally:
            listener.close()

    fds_before = len(os.listdir("/dev/fd"))
    server = Thread(target=serve)
    server.start()
    api = ControlApiClient(
        base_url=f"http://{host}:{port}",
        credential=CREDENTIAL,
        timeout=timeout,
    )
    started = time.monotonic()
    with pytest.raises(TransientControlError):
        api.claim(DELIVERY_ID)
    elapsed = time.monotonic() - started
    api.close()
    server.join(1)

    assert elapsed <= timeout + 0.1
    assert peer_closed.is_set()
    assert not server.is_alive()
    assert server_errors == []
    assert len(os.listdir("/dev/fd")) <= fds_before + 1


def test_slow_resolver_is_killed_reaped_and_closes_all_descriptors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    pid_path = tmp_path / "resolver.pid"
    monkeypatch.setattr(
        _deadline_http,
        "_DNS_LOOKUP_SCRIPT",
        (
            "import os, time\n"
            f"with open({str(pid_path)!r}, 'w') as stream:\n"
            "    stream.write(str(os.getpid()))\n"
            "time.sleep(10)\n"
        ),
    )
    fds_before = len(os.listdir("/dev/fd"))
    api = ControlApiClient(
        base_url="http://slow-resolver.test:1",
        credential=CREDENTIAL,
        timeout=0.2,
    )

    started = time.monotonic()
    with pytest.raises(TransientControlError):
        api.fetch(DELIVERY_ID)
    elapsed = time.monotonic() - started
    api.close()

    assert elapsed <= 0.3
    resolver_pid = int(pid_path.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(resolver_pid, 0)
    assert len(os.listdir("/dev/fd")) <= fds_before


def test_dns_connection_and_header_accumulation_share_the_same_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(1)
    host, port = listener.getsockname()
    peer_closed = Event()
    server_errors: list[BaseException] = []
    monkeypatch.setattr(
        _deadline_http,
        "_DNS_LOOKUP_SCRIPT",
        (
            "import json, socket, sys, time\n"
            "request = json.load(sys.stdin)\n"
            "time.sleep(0.35)\n"
            f"json.dump([[socket.AF_INET, socket.SOCK_STREAM, 0, '', ['{host}', request['port']]]], sys.stdout)\n"
        ),
    )

    def serve() -> None:
        try:
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(1)
                request = bytearray()
                while b"\r\n\r\n" not in request:
                    chunk = connection.recv(4096)
                    if not chunk:
                        return
                    request.extend(chunk)
                time.sleep(0.6)
                connection.sendall(
                    b"HTTP/1.1 503 Unavailable\r\nConnection: close\r\n\r\n"
                )
                peer_closed.set() if connection.recv(1) == b"" else None
        except OSError:
            peer_closed.set()
        except BaseException as error:
            server_errors.append(error)
        finally:
            listener.close()

    server = Thread(target=serve)
    server.start()
    api = ControlApiClient(
        base_url=f"http://deadline-resolution.test:{port}",
        credential=CREDENTIAL,
        timeout=0.8,
    )
    started = time.monotonic()
    with pytest.raises(TransientControlError):
        api.fetch(DELIVERY_ID)
    elapsed = time.monotonic() - started
    api.close()
    server.join(1)

    assert elapsed <= 0.9
    assert peer_closed.is_set()
    assert not server.is_alive()
    assert server_errors == []
