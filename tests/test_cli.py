from __future__ import annotations

from signal import SIGTERM
import socket
from threading import Event
from threading import Thread
import time

import pytest
import redis

from signaldesk_email_worker import cli
from signaldesk_email_worker.settings import Settings


VALID_SETTINGS = {
    "redis_url": "redis://127.0.0.1:6379/0",
    "control_api_base_url": "https://control.example.test",
    "email_worker_service_credential": "e" * 32,
    "consumer_name": "email-worker-cli",
}


def test_stop_handler_sets_event() -> None:
    stop = Event()
    cli.make_stop_handler(stop)(SIGTERM, None)
    assert stop.is_set()


def test_run_worker_closes_all_owned_resources(monkeypatch) -> None:
    closed: list[str] = []

    class Resource:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            closed.append(self.name)

    class Consumer:
        control = Resource("control")
        mailpit = Resource("mailpit")
        smtp = Resource("smtp")
        redis = Resource("redis")

        def setup(self) -> None:
            pass

        def process_once(self) -> int:
            return 0

    monkeypatch.setattr(cli, "create_consumer", lambda _settings, _stop: Consumer())
    cli.run_worker(object(), once=True)  # type: ignore[arg-type]
    assert closed == ["smtp", "mailpit", "control", "redis"]


@pytest.mark.parametrize("failure_name", ["smtp", "mailpit", "control", "redis"])
def test_run_worker_attempts_every_close_before_raising_sanitized_cleanup_error(
    monkeypatch: pytest.MonkeyPatch, failure_name: str
) -> None:
    close_attempts: list[str] = []

    class Resource:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            close_attempts.append(self.name)
            if self.name == failure_name:
                raise RuntimeError(f"sensitive {self.name} cleanup detail")

    class Consumer:
        smtp = Resource("smtp")
        mailpit = Resource("mailpit")
        control = Resource("control")
        redis = Resource("redis")

        def setup(self) -> None:
            pass

        def process_once(self) -> int:
            return 0

    monkeypatch.setattr(cli, "create_consumer", lambda _settings, _stop: Consumer())
    with pytest.raises(cli.ResourceCleanupError) as caught:
        cli.run_worker(object(), once=True)  # type: ignore[arg-type]
    assert close_attempts == ["smtp", "mailpit", "control", "redis"]
    assert str(caught.value) == "resource_cleanup_failed"
    assert caught.value.__cause__ is None
    assert "sensitive" not in repr(caught.value)


def test_run_worker_preserves_primary_error_while_attempting_every_failing_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_attempts: list[str] = []

    class Resource:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            close_attempts.append(self.name)
            raise RuntimeError(f"sensitive {self.name} cleanup detail")

    class Consumer:
        smtp = Resource("smtp")
        mailpit = Resource("mailpit")
        control = Resource("control")
        redis = Resource("redis")

        def setup(self) -> None:
            pass

        def process_once(self) -> int:
            raise ValueError("primary worker failure")

    monkeypatch.setattr(cli, "create_consumer", lambda _settings, _stop: Consumer())
    with pytest.raises(ValueError, match="primary worker failure") as caught:
        cli.run_worker(object(), once=True)  # type: ignore[arg-type]
    assert close_attempts == ["smtp", "mailpit", "control", "redis"]
    assert caught.value.__context__ is None


@pytest.mark.parametrize(
    ("failure_stage", "expected_closed"),
    [
        ("control", ["redis"]),
        ("mailpit", ["control", "redis"]),
        ("smtp", ["mailpit", "control", "redis"]),
    ],
)
def test_create_consumer_closes_every_resource_on_partial_construction_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
    expected_closed: list[str],
) -> None:
    closed: list[str] = []

    class Resource:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            closed.append(self.name)

    class RedisFactory:
        @staticmethod
        def from_url(*_args, **_kwargs):
            return Resource("redis")

    class Settings:
        class Secret:
            @staticmethod
            def get_secret_value() -> str:
                return "x" * 32

        redis_url = Secret()
        control_api_base_url = "https://control.example.test"
        email_worker_service_credential = Secret()
        redis_connect_timeout_seconds = 1.0
        redis_socket_timeout_seconds = 1.0
        api_timeout_seconds = 1.0
        api_max_response_bytes = 1024
        mailpit_api_base_url = "http://mailpit:8025"
        mailpit_api_timeout_seconds = 1.0
        mailpit_api_max_response_bytes = 1024
        smtp_host = "mailpit"
        smtp_port = 1025
        smtp_timeout_seconds = 1.0
        message_max_bytes = 4096
        rendered_body_max_bytes = 4096

    def build(name: str):
        if failure_stage == name:
            raise RuntimeError("sanitized construction failure")
        return Resource(name)

    monkeypatch.setattr(cli.redis, "Redis", RedisFactory)
    monkeypatch.setattr(cli, "ControlApiClient", lambda **_kwargs: build("control"))
    monkeypatch.setattr(cli, "MailpitClient", lambda **_kwargs: build("mailpit"))
    monkeypatch.setattr(cli, "MailpitSmtpTransport", lambda **_kwargs: build("smtp"))
    with pytest.raises(RuntimeError, match="sanitized construction failure"):
        cli.create_consumer(Settings(), Event())  # type: ignore[arg-type]
    assert closed == expected_closed


def test_redis_factory_explicitly_disables_auxiliary_exchanges_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class RedisFactory:
        @staticmethod
        def from_url(url: str, **kwargs: object) -> object:
            captured["url"] = url
            captured.update(kwargs)
            return object()

    monkeypatch.setattr(cli.redis, "Redis", RedisFactory)
    settings = Settings(**VALID_SETTINGS)

    cli._create_redis_client(settings)

    assert captured["url"] == VALID_SETTINGS["redis_url"]
    assert captured["decode_responses"] is False
    assert captured["health_check_interval"] == 0
    assert captured["lib_name"] is None
    assert captured["lib_version"] is None
    assert captured["protocol"] == 2
    assert captured["retry_on_timeout"] is False
    assert captured["retry_on_error"] == []
    assert captured["retry"].get_retries() == 0  # type: ignore[union-attr]
    assert captured["connection_class"] is cli._DeadlineRedisConnection


def test_redis_connect_timeout_is_one_deadline_across_all_addresses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempted_timeouts: list[float] = []

    class FakeSocket:
        timeout = 0.0

        def setsockopt(self, *_args: object) -> None:
            pass

        def settimeout(self, timeout: float) -> None:
            self.timeout = timeout

        def connect(self, _address: object) -> None:
            attempted_timeouts.append(self.timeout)
            time.sleep(self.timeout)
            raise socket.timeout

        def shutdown(self, _how: int) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        cli,
        "_resolve_addresses",
        lambda *_args: [
            (socket.AF_INET, socket.SOCK_STREAM, 0, ("192.0.2.1", 6379)),
            (socket.AF_INET, socket.SOCK_STREAM, 0, ("192.0.2.2", 6379)),
        ],
    )
    monkeypatch.setattr(cli.socket, "socket", lambda *_args: FakeSocket())
    connection = cli._DeadlineRedisConnection(
        host="redis.test",
        port=6379,
        socket_connect_timeout=0.04,
        socket_timeout=0.1,
    )

    started = time.monotonic()
    with pytest.raises(socket.timeout):
        connection._connect()
    elapsed = time.monotonic() - started

    assert len(attempted_timeouts) == 1
    assert attempted_timeouts[0] <= 0.04
    assert elapsed < 0.2


def test_redis_reconnect_wire_sends_only_command_and_never_retries() -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(0.25)
    host, port = listener.getsockname()
    commands: list[tuple[bytes, ...]] = []
    server_errors: list[BaseException] = []
    finished = Event()

    def read_command(connection: socket.socket) -> tuple[bytes, ...]:
        stream = connection.makefile("rb")
        count_line = stream.readline()
        assert count_line.startswith(b"*")
        count = int(count_line[1:-2])
        parts: list[bytes] = []
        for _ in range(count):
            length_line = stream.readline()
            assert length_line.startswith(b"$")
            length = int(length_line[1:-2])
            parts.append(stream.read(length))
            assert stream.read(2) == b"\r\n"
        return tuple(parts)

    def serve() -> None:
        try:
            for response in (b"+PONG\r\n", b"$5\r\nprobe\r\n", None):
                connection, _ = listener.accept()
                with connection:
                    commands.append(read_command(connection))
                    if response is not None:
                        time.sleep(0.01)
                        connection.sendall(response)
            try:
                extra, _ = listener.accept()
            except TimeoutError:
                pass
            else:
                with extra:
                    commands.append(read_command(extra))
        except BaseException as error:
            server_errors.append(error)
        finally:
            listener.close()
            finished.set()

    server = Thread(target=serve)
    server.start()
    settings = Settings(
        **(
            VALID_SETTINGS
            | {
                "redis_url": f"redis://{host}:{port}/0",
                "redis_connect_timeout_seconds": 0.1,
                "redis_socket_timeout_seconds": 0.1,
            }
        )
    )
    client = cli._create_redis_client(settings)
    try:
        assert client.ping()
        client.connection_pool.disconnect()
        assert client.echo(b"probe") == b"probe"
        client.connection_pool.disconnect()
        with pytest.raises(redis.RedisError):
            client.ping()
        assert finished.wait(timeout=2)
    finally:
        client.close()
        listener.close()
        server.join(timeout=2)

    assert not server.is_alive()
    assert server_errors == []
    assert commands == [(b"PING",), (b"ECHO", b"probe"), (b"PING",)]
