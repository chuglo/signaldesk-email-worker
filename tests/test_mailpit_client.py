from __future__ import annotations

import json
import os
import select
import socket
import sys
from threading import Event, Thread
import time
import traceback
from uuid import uuid4

import httpx
import pytest

from signaldesk_email_worker.mailpit_client import (
    MailpitAnomalyError,
    MailpitClient,
    MailpitTransientError,
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


def summary(message_id: str) -> dict[str, object]:
    return {
        "ID": "mailpit-row-1",
        "MessageID": message_id,
        "Read": False,
        "From": {"Name": "", "Address": "notifications@signaldesk.test"},
        "To": [{"Name": "", "Address": "recipient@example.test"}],
        "Cc": [],
        "Bcc": [],
        "ReplyTo": [],
        "Subject": "SignalDesk notification",
        "Created": "2026-07-23T10:00:00Z",
        "Tags": [],
        "Size": 512,
        "Attachments": 0,
        "Snippet": "SignalDesk",
        "Username": "",
    }


def response(messages: list[dict[str, object]]) -> dict[str, object]:
    return {
        "messages": messages,
        "count": len(messages),
        "messages_count": len(messages),
        "messages_unread": len(messages),
        "start": 0,
        "tags": [],
        "total": len(messages),
        "unread": len(messages),
    }


def test_search_uses_official_endpoint_and_message_id_filter_then_verifies_exact_match() -> (
    None
):
    delivery_id = uuid4()
    expected = f"<{delivery_id}@signaldesk.local>"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=response([summary(expected[1:-1])]))

    client = MailpitClient(
        base_url="http://mailpit:8025",
        timeout=2,
        transport=httpx.MockTransport(handler),
    )
    assert client.contains_message_id(expected)
    request = requests[0]
    assert request.method == "GET"
    assert request.url.path == "/api/v1/search"
    assert dict(request.url.params) == {
        "query": f"message-id:{expected[1:-1]}",
        "start": "0",
        "limit": "2",
    }
    assert request.headers["Accept-Encoding"] == "identity"


def test_absence_is_proven_only_by_successful_strict_empty_search() -> None:
    expected = f"<{uuid4()}@signaldesk.local>"
    client = MailpitClient(
        base_url="http://mailpit:8025",
        timeout=2,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=response([]))
        ),
    )
    assert not client.contains_message_id(expected)


@pytest.mark.parametrize(
    "payload",
    [
        response([summary("wrong@signaldesk.local")]),
        response([summary("one@signaldesk.local"), summary("two@signaldesk.local")]),
        response([]) | {"extra": "forbidden"},
        response([]) | {"messages_count": 2},
        response([]) | {"count": 1},
        response([]) | {"messages_count": 1, "total": 0},
        response([]) | {"messages_unread": 1},
        response([]) | {"unread": 1, "total": 0},
        response([summary("one@signaldesk.local")]) | {"messages_count": 2, "total": 2},
    ],
)
def test_ambiguous_mismatched_or_invalid_search_is_fail_closed(
    payload: dict[str, object],
) -> None:
    client = MailpitClient(
        base_url="http://mailpit:8025",
        timeout=2,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=payload)
        ),
    )
    with pytest.raises(MailpitAnomalyError):
        client.contains_message_id(f"<{uuid4()}@signaldesk.local>")


def test_unavailable_oversized_and_compressed_search_are_transient_and_sanitized() -> (
    None
):
    sentinel = "sensitive-mailpit-detail"

    def fail(_request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout(sentinel)

    unavailable = MailpitClient(
        base_url="http://mailpit:8025",
        timeout=2,
        transport=httpx.MockTransport(fail),
    )
    with pytest.raises(MailpitTransientError) as caught:
        unavailable.contains_message_id(f"<{uuid4()}@signaldesk.local>")
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert sentinel not in "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__
        )
    )

    oversized = MailpitClient(
        base_url="http://mailpit:8025",
        timeout=2,
        max_response_bytes=256,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, content=b"x" * 257)
        ),
    )
    with pytest.raises(MailpitTransientError):
        oversized.contains_message_id(f"<{uuid4()}@signaldesk.local>")


@pytest.mark.parametrize("case", ["malformed_json", "invalid_model", "content_length"])
def test_rejected_mailpit_response_values_are_absent_from_exception_graph(
    case: str,
) -> None:
    sentinel = f"sensitive-mailpit-{case}-sentinel"

    def handler(_request: httpx.Request) -> httpx.Response:
        if case == "malformed_json":
            return httpx.Response(200, content=f'{{"value":"{sentinel}"'.encode())
        if case == "invalid_model":
            return httpx.Response(200, json=response([]) | {"messages": sentinel})
        return httpx.Response(
            200,
            json=response([]),
            headers={"content-length": sentinel},
        )

    client = MailpitClient(
        base_url="http://mailpit:8025",
        timeout=2,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises((MailpitAnomalyError, MailpitTransientError)) as caught:
        client.contains_message_id(f"<{uuid4()}@signaldesk.local>")
    assert_sanitized_exception(caught.value, sentinel)


def test_real_official_legacy_count_payload_and_global_totals_are_accepted() -> None:
    delivery_id = uuid4()
    expected = f"<{delivery_id}@signaldesk.local>"
    official = response([summary(expected[1:-1])]) | {
        "count": 1,
        "messages_count": 1,
        "messages_unread": 1,
        "total": 9,
        "unread": 4,
    }
    client = MailpitClient(
        base_url="http://mailpit:8025",
        timeout=2,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=official)
        ),
    )
    assert client.contains_message_id(expected)


def test_mailpit_client_ignores_adversarial_proxy_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy-credential-sentinel@127.0.0.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy-credential-sentinel@127.0.0.1:9")
    monkeypatch.setenv("NO_PROXY", "")
    expected = f"<{uuid4()}@signaldesk.local>"
    client = MailpitClient(
        base_url="http://mailpit:8025",
        timeout=2,
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json=response([]))
        ),
    )
    assert not client.contains_message_id(expected)
    assert client._client._trust_env is False


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_real_loopback_mailpit_request_has_one_absolute_deadline_and_closes_peer(
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    timeout = 1.0
    body = json.dumps(response([]), separators=(",", ":")).encode()
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

    real_getaddrinfo = socket.getaddrinfo

    def route_mailpit(
        requested_host: object, requested_port: object, *args: object, **kwargs: object
    ):
        if requested_host in {"mailpit", b"mailpit"} and requested_port == 8025:
            return real_getaddrinfo(host, port, *args, **kwargs)
        return real_getaddrinfo(requested_host, requested_port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", route_mailpit)
    deadline_module = sys.modules.get("signaldesk_email_worker._deadline_http")
    if deadline_module is not None:
        resolver_script = (
            "import json, socket, sys\n"
            "json.load(sys.stdin)\n"
            f"json.dump([[socket.AF_INET, socket.SOCK_STREAM, 0, '', ['{host}', {port}]]], sys.stdout)\n"
        )
        monkeypatch.setattr(deadline_module, "_DNS_LOOKUP_SCRIPT", resolver_script)

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
    client = MailpitClient(base_url="http://mailpit:8025", timeout=timeout)
    started = time.monotonic()
    with pytest.raises(MailpitTransientError):
        client.contains_message_id(f"<{uuid4()}@signaldesk.local>")
    elapsed = time.monotonic() - started
    client.close()
    server.join(1)

    assert elapsed <= timeout + 0.1
    assert peer_closed.is_set()
    assert not server.is_alive()
    assert server_errors == []
    assert len(os.listdir("/dev/fd")) <= fds_before + 1
