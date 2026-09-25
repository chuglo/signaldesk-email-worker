from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import json
import select
import socket
import ssl
import subprocess
import sys
import time
from typing import Any, Callable, cast, Iterable, Iterator, TypeVar

import httpcore
import httpx


_DNS_LOOKUP_SCRIPT = """
import json
import socket
import sys

request = json.load(sys.stdin)
addresses = socket.getaddrinfo(
    request["host"], request["port"], family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
)
json.dump(
    [
        [family, socktype, protocol, canonical_name, list(sockaddr)]
        for family, socktype, protocol, canonical_name, sockaddr in addresses
    ],
    sys.stdout,
)
"""
_T = TypeVar("_T")


@dataclass(frozen=True)
class Deadline:
    expires_at: float

    @classmethod
    def after(cls, timeout: float) -> Deadline:
        return cls(time.monotonic() + timeout)

    def remaining(self) -> float:
        remaining = self.expires_at - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("HTTP operation deadline exceeded")
        return remaining


def _stop_resolver(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.kill()
    process.communicate()


def _resolve_addresses(
    host: str, port: int, deadline: Deadline
) -> list[tuple[int, int, int, tuple[Any, ...]]]:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        process: subprocess.Popen[str] | None = None
        try:
            deadline.remaining()
            process = subprocess.Popen(
                [sys.executable, "-I", "-c", _DNS_LOOKUP_SCRIPT],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="strict",
                env={},
                close_fds=True,
            )
            output, _ = process.communicate(
                json.dumps({"host": host, "port": port}),
                timeout=deadline.remaining(),
            )
            deadline.remaining()
            if process.returncode != 0 or len(output) > 65_536:
                raise ValueError("resolver failed")
            decoded: Any = json.loads(output)
            if not isinstance(decoded, list) or not 1 <= len(decoded) <= 64:
                raise ValueError("invalid resolver response")
            resolved: list[tuple[int, int, int, tuple[Any, ...]]] = []
            for item in decoded:
                if not isinstance(item, list) or len(item) != 5:
                    raise ValueError("invalid resolver item")
                family, socktype, protocol, canonical_name, sockaddr = item
                if (
                    family not in {socket.AF_INET, socket.AF_INET6}
                    or socktype != socket.SOCK_STREAM
                    or not isinstance(protocol, int)
                    or not isinstance(canonical_name, str)
                    or not isinstance(sockaddr, list)
                    or len(sockaddr) not in {2, 4}
                    or not isinstance(sockaddr[0], str)
                    or not isinstance(sockaddr[1], int)
                ):
                    raise ValueError("invalid resolver address")
                resolved.append((family, socktype, protocol, tuple(sockaddr)))
            return resolved
        except (subprocess.TimeoutExpired, TimeoutError):
            if process is not None:
                _stop_resolver(process)
            raise httpcore.ConnectTimeout("HTTP DNS deadline exceeded") from None
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            if process is not None:
                _stop_resolver(process)
            raise httpcore.ConnectError("HTTP DNS failed") from None
    family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
    sockaddr: tuple[Any, ...] = (
        (host, port, 0, 0) if family == socket.AF_INET6 else (host, port)
    )
    return [(family, socket.SOCK_STREAM, 0, sockaddr)]


class _DeadlineSocketStream(httpcore.NetworkStream):
    def __init__(self, stream_socket: socket.socket, deadline: Deadline) -> None:
        self._socket = stream_socket
        self._deadline = deadline

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        del timeout
        try:
            self._socket.settimeout(self._deadline.remaining())
            return self._socket.recv(max_bytes)
        except (TimeoutError, socket.timeout):
            raise httpcore.ReadTimeout("HTTP read deadline exceeded") from None
        except OSError:
            raise httpcore.ReadError("HTTP read failed") from None

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        del timeout
        try:
            remaining = memoryview(buffer)
            while remaining:
                self._socket.settimeout(self._deadline.remaining())
                sent = self._socket.send(remaining)
                if sent == 0:
                    raise OSError("socket closed during write")
                remaining = remaining[sent:]
        except (TimeoutError, socket.timeout):
            raise httpcore.WriteTimeout("HTTP write deadline exceeded") from None
        except OSError:
            raise httpcore.WriteError("HTTP write failed") from None

    def close(self) -> None:
        self._socket.close()

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.NetworkStream:
        del timeout
        try:
            return _DeadlineTlsStream(
                self,
                ssl_context=ssl_context,
                server_hostname=server_hostname,
                deadline=self._deadline,
            )
        except httpcore.TimeoutException:
            self.close()
            raise httpcore.ConnectTimeout("HTTP TLS deadline exceeded") from None
        except Exception:
            self.close()
            raise httpcore.ConnectError("HTTP TLS failed") from None

    def get_extra_info(self, info: str) -> Any:
        if info == "client_addr":
            return self._socket.getsockname()
        if info == "server_addr":
            return self._socket.getpeername()
        if info == "socket":
            return self._socket
        if info == "is_readable":
            readable, _, _ = select.select([self._socket], [], [], 0)
            return bool(readable)
        return None


class _DeadlineTlsStream(httpcore.NetworkStream):
    def __init__(
        self,
        raw_stream: _DeadlineSocketStream,
        *,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None,
        deadline: Deadline,
    ) -> None:
        self._raw_stream = raw_stream
        self._deadline = deadline
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl_object = ssl_context.wrap_bio(
            self._incoming,
            self._outgoing,
            server_side=False,
            server_hostname=server_hostname,
        )
        self._perform(self._ssl_object.do_handshake)

    def _flush(self) -> None:
        encrypted = self._outgoing.read()
        if encrypted:
            self._raw_stream.write(encrypted, timeout=self._deadline.remaining())

    def _receive(self) -> None:
        encrypted = self._raw_stream.read(65_536, timeout=self._deadline.remaining())
        if encrypted:
            self._incoming.write(encrypted)
        else:
            self._incoming.write_eof()

    def _perform(self, operation: Callable[[], _T]) -> _T:
        while True:
            self._deadline.remaining()
            try:
                result = operation()
            except ssl.SSLWantReadError:
                self._flush()
                self._receive()
            except ssl.SSLWantWriteError:
                self._flush()
            else:
                self._flush()
                return result

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        del timeout
        try:
            return self._perform(lambda: self._ssl_object.read(max_bytes))
        except httpcore.TimeoutException:
            raise
        except (ssl.SSLError, OSError):
            raise httpcore.ReadError("HTTP TLS read failed") from None

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        del timeout
        try:
            remaining = memoryview(buffer)
            while remaining:
                sent = self._perform(lambda: self._ssl_object.write(remaining))
                remaining = remaining[sent:]
        except httpcore.TimeoutException:
            raise
        except (ssl.SSLError, OSError):
            raise httpcore.WriteError("HTTP TLS write failed") from None

    def close(self) -> None:
        self._raw_stream.close()

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.NetworkStream:
        del ssl_context, server_hostname, timeout
        raise httpcore.ConnectError("nested TLS is unsupported")

    def get_extra_info(self, info: str) -> Any:
        if info == "ssl_object":
            return self._ssl_object
        return self._raw_stream.get_extra_info(info)


class _DeadlineNetworkBackend(httpcore.NetworkBackend):
    def __init__(self, deadline: Deadline) -> None:
        self._deadline = deadline

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[tuple[Any, ...]] | None = None,
    ) -> httpcore.NetworkStream:
        del timeout
        addresses = _resolve_addresses(host, port, self._deadline)
        timed_out = False
        for family, socktype, protocol, sockaddr in addresses:
            stream_socket = socket.socket(family, socktype, protocol)
            try:
                if local_address is not None:
                    bind_address: tuple[Any, ...] = (
                        (local_address, 0, 0, 0)
                        if family == socket.AF_INET6
                        else (local_address, 0)
                    )
                    stream_socket.bind(bind_address)
                for option in socket_options or ():
                    stream_socket.setsockopt(*option)
                stream_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                stream_socket.settimeout(self._deadline.remaining())
                stream_socket.connect(sockaddr)
                self._deadline.remaining()
                return _DeadlineSocketStream(stream_socket, self._deadline)
            except (TimeoutError, socket.timeout):
                timed_out = True
                stream_socket.close()
            except OSError:
                stream_socket.close()
        if timed_out:
            raise httpcore.ConnectTimeout("HTTP connect deadline exceeded") from None
        raise httpcore.ConnectError("HTTP connect failed") from None

    def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[tuple[Any, ...]] | None = None,
    ) -> httpcore.NetworkStream:
        del path, timeout, socket_options
        raise httpcore.UnsupportedProtocol("UNIX sockets are unsupported")

    def sleep(self, seconds: float) -> None:
        time.sleep(min(seconds, self._deadline.remaining()))
        self._deadline.remaining()


class _DeadlineResponseStream(httpx.SyncByteStream):
    def __init__(self, stream: Iterable[bytes]) -> None:
        self._stream = stream

    def __iter__(self) -> Iterator[bytes]:
        try:
            yield from self._stream
        except Exception:
            raise httpx.TransportError("HTTP response read failed") from None

    def close(self) -> None:
        try:
            close = getattr(self._stream, "close", None)
            if close is not None:
                close()
        except Exception:
            return


class DeadlineTransport(httpx.BaseTransport):
    def __init__(self, deadline: Deadline) -> None:
        self._pool = httpcore.ConnectionPool(
            ssl_context=httpx.create_ssl_context(trust_env=False),
            max_connections=1,
            max_keepalive_connections=0,
            http1=True,
            http2=False,
            retries=0,
            network_backend=_DeadlineNetworkBackend(deadline),
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if not isinstance(request.stream, httpx.SyncByteStream):
            raise httpx.TransportError("invalid HTTP request stream")
        core_request = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        try:
            response = self._pool.handle_request(core_request)
        except Exception:
            raise httpx.TransportError("HTTP request failed", request=request) from None
        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=_DeadlineResponseStream(cast(Iterable[bytes], response.stream)),
            extensions=response.extensions,
        )

    def close(self) -> None:
        self._pool.close()
