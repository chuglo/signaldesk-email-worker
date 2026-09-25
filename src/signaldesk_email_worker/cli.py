from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from signal import SIGINT, SIGTERM, getsignal, signal
import socket
from threading import Event
from types import FrameType
from typing import Any

import httpcore
import redis
from redis.backoff import NoBackoff
from redis.connection import Connection
from redis.retry import Retry

from signaldesk_email_worker._deadline_http import Deadline, _resolve_addresses
from signaldesk_email_worker.consumer import EmailConsumer
from signaldesk_email_worker.control_client import ControlApiClient
from signaldesk_email_worker.mailpit_client import MailpitClient
from signaldesk_email_worker.settings import Settings
from signaldesk_email_worker.smtp_delivery import (
    MailpitSmtpTransport,
    MailRenderer,
    SmtpDelivery,
)


class ResourceCleanupError(RuntimeError):
    """One or more owned resources could not be closed."""


class _DeadlineRedisConnection(Connection):
    """redis-py TCP connection with one deadline for DNS and all addresses."""

    def _connect(self) -> socket.socket:
        deadline = Deadline.after(float(self.socket_connect_timeout))
        try:
            addresses = _resolve_addresses(self.host, self.port, deadline)
        except httpcore.ConnectTimeout:
            raise socket.timeout("Redis connect deadline exceeded") from None
        except Exception:
            raise OSError("Redis name resolution failed") from None

        last_error: OSError | None = None
        for family, socktype, protocol, socket_address in addresses:
            connection: socket.socket | None = None
            try:
                connection = socket.socket(family, socktype, protocol)
                connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                if self.socket_keepalive:
                    connection.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for key, value in self.socket_keepalive_options.items():
                        connection.setsockopt(socket.IPPROTO_TCP, key, value)
                connection.settimeout(deadline.remaining())
                connection.connect(socket_address)
                deadline.remaining()
                connection.settimeout(self.socket_timeout)
                return connection
            except (TimeoutError, socket.timeout):
                last_error = socket.timeout("Redis connect deadline exceeded")
            except OSError as error:
                last_error = error
            if connection is not None:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                connection.close()
        if last_error is not None:
            raise last_error
        raise OSError("Redis resolver returned no addresses")


def _close_resources(resources: Sequence[Any]) -> bool:
    cleanup_failed = False
    for resource in resources:
        try:
            resource.close()
        except BaseException:
            cleanup_failed = True
    return cleanup_failed


def make_stop_handler(stop_event: Event) -> Callable[[int, FrameType | None], None]:
    def stop(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    return stop


def _create_redis_client(settings: Settings) -> redis.Redis:
    return redis.Redis.from_url(
        settings.redis_url.get_secret_value(),
        decode_responses=False,
        socket_connect_timeout=settings.redis_connect_timeout_seconds,
        socket_timeout=settings.redis_socket_timeout_seconds,
        health_check_interval=0,
        lib_name=None,
        lib_version=None,
        protocol=2,
        retry=Retry(NoBackoff(), 0),
        retry_on_timeout=False,
        retry_on_error=[],
        connection_class=_DeadlineRedisConnection,
    )


def create_consumer(settings: Settings, stop_event: Event) -> EmailConsumer:
    resources: list[Any] = []
    try:
        broker = _create_redis_client(settings)
        resources.append(broker)
        control = ControlApiClient(
            base_url=str(settings.control_api_base_url),
            credential=settings.email_worker_service_credential.get_secret_value(),
            timeout=settings.api_timeout_seconds,
            max_response_bytes=settings.api_max_response_bytes,
        )
        resources.append(control)
        mailpit = MailpitClient(
            base_url=str(settings.mailpit_api_base_url),
            timeout=settings.mailpit_api_timeout_seconds,
            max_response_bytes=settings.mailpit_api_max_response_bytes,
        )
        resources.append(mailpit)
        transport = MailpitSmtpTransport(
            host=settings.smtp_host,
            port=settings.smtp_port,
            timeout=settings.smtp_timeout_seconds,
            max_message_bytes=settings.message_max_bytes,
        )
        resources.append(transport)
        smtp = SmtpDelivery(
            transport=transport,
            max_message_bytes=settings.message_max_bytes,
        )
        resources[-1] = smtp
        consumer = EmailConsumer(
            settings=settings,
            redis_client=broker,
            control_client=control,
            mailpit_client=mailpit,
            smtp_delivery=smtp,
            renderer=MailRenderer(max_body_bytes=settings.rendered_body_max_bytes),
            stop_event=stop_event,
        )
    except BaseException:
        _close_resources(list(reversed(resources)))
        raise
    return consumer


def run_worker(
    settings: Settings,
    *,
    once: bool = False,
    stop_event: Event | None = None,
) -> None:
    stop = stop_event or Event()
    consumer = create_consumer(settings, stop)
    primary_failed = False
    try:
        if once:
            consumer.setup()
            consumer.process_once()
        else:
            consumer.run()
    except BaseException:
        primary_failed = True
        raise
    finally:
        resources = [
            resource
            for resource_name in ("smtp", "mailpit", "control", "redis")
            if (resource := getattr(consumer, resource_name, None)) is not None
            and hasattr(resource, "close")
        ]
        cleanup_failed = _close_resources(resources)
        if cleanup_failed and not primary_failed:
            raise ResourceCleanupError("resource_cleanup_failed") from None


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the SignalDesk email worker")
    parser.add_argument(
        "--once",
        action="store_true",
        help="process at most one bounded reclaimed/new batch and exit",
    )
    arguments = parser.parse_args(argv)
    stop_event = Event()
    handler = make_stop_handler(stop_event)
    previous = {SIGTERM: getsignal(SIGTERM), SIGINT: getsignal(SIGINT)}
    signal(SIGTERM, handler)
    signal(SIGINT, handler)
    try:
        run_worker(Settings(), once=arguments.once, stop_event=stop_event)  # type: ignore[call-arg]
    finally:
        signal(SIGTERM, previous[SIGTERM])
        signal(SIGINT, previous[SIGINT])


if __name__ == "__main__":
    main()
