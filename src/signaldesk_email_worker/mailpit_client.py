from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from signaldesk_email_worker._deadline_http import Deadline, DeadlineTransport


class MailpitTransientError(RuntimeError):
    pass


class MailpitAnomalyError(RuntimeError):
    pass


@dataclass(frozen=True)
class _BoundedResponse:
    status_code: int
    content: bytes = b""


class _Address(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    name: str = Field(alias="Name")
    address: str = Field(alias="Address")


class _MessageSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    database_id: str = Field(alias="ID")
    message_id: str = Field(alias="MessageID")
    read: bool = Field(alias="Read")
    sender: _Address = Field(alias="From")
    to: list[_Address] = Field(alias="To")
    cc: list[_Address] = Field(alias="Cc")
    bcc: list[_Address] = Field(alias="Bcc")
    reply_to: list[_Address] = Field(alias="ReplyTo")
    subject: str = Field(alias="Subject")
    created: datetime = Field(alias="Created")
    tags: list[str] = Field(alias="Tags")
    size: int = Field(alias="Size", ge=0)
    attachments: int = Field(alias="Attachments", ge=0)
    snippet: str = Field(alias="Snippet")
    username: str = Field(default="", alias="Username")


class _SearchResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    messages: list[_MessageSummary]
    count: int = Field(ge=0)
    messages_count: int = Field(ge=0)
    messages_unread: int = Field(ge=0)
    start: int = Field(ge=0)
    tags: list[str]
    total: int = Field(ge=0)
    unread: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_page(self) -> "_SearchResponse":
        if self.start != 0 or self.count != len(self.messages):
            raise ValueError("inconsistent Mailpit search page")
        if (
            self.count > 2
            or not self.count <= self.messages_count <= self.total
            or self.messages_unread > self.messages_count
            or self.unread > self.total
        ):
            raise ValueError("invalid Mailpit search counts")
        return self


class MailpitClient:
    """Bounded client for Mailpit's official GET /api/v1/search endpoint.

    Mailpit exposes the MessageID summary without RFC angle brackets, so this client
    searches and compares the exact inner addr-spec of the deterministic header.
    """

    def __init__(
        self,
        *,
        base_url: str,
        timeout: float,
        max_response_bytes: int = 16_384,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if base_url.rstrip("/") != "http://mailpit:8025":
            raise ValueError("Mailpit API is confined to http://mailpit:8025")
        if not 256 <= max_response_bytes <= 65_536:
            raise ValueError("max_response_bytes must be between 256 and 65536")
        if not 0 < timeout <= 30:
            raise ValueError("timeout must be between 0 and 30 seconds")
        self._timeout = timeout
        self._max_response_bytes = max_response_bytes
        self._client = (
            httpx.Client(
                base_url="http://mailpit:8025",
                timeout=httpx.Timeout(timeout),
                transport=transport,
                headers={"Accept-Encoding": "identity"},
                follow_redirects=False,
                trust_env=False,
            )
            if transport is not None
            else None
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def contains_message_id(self, expected_message_id: str) -> bool:
        inner = self._validate_expected(expected_message_id)
        response = self._request(
            "GET",
            "/api/v1/search",
            params={"query": f"message-id:{inner}", "start": "0", "limit": "2"},
        )
        if response.status_code != 200:
            raise MailpitTransientError("Mailpit search unavailable")
        parsed: _SearchResponse | None = None
        try:
            parsed = _SearchResponse.model_validate_json(response.content)
        except (ValidationError, ValueError):
            pass
        if parsed is None:
            raise MailpitAnomalyError("invalid Mailpit search response")
        if parsed.messages_count != parsed.count:
            raise MailpitAnomalyError("truncated Mailpit search response")
        if len(parsed.messages) > 1:
            raise MailpitAnomalyError("duplicate deterministic Message-ID")
        if not parsed.messages:
            return False
        if parsed.messages[0].message_id != inner:
            raise MailpitAnomalyError("Mailpit Message-ID mismatch")
        return True

    @staticmethod
    def _validate_expected(value: str) -> str:
        if not value.startswith("<") or not value.endswith("@signaldesk.local>"):
            raise ValueError("invalid deterministic Message-ID")
        inner = value[1:-1]
        identifier, separator, domain = inner.partition("@")
        if (
            separator != "@"
            or domain != "signaldesk.local"
            or str(UUID(identifier)) != identifier
        ):
            raise ValueError("invalid deterministic Message-ID")
        return inner

    def _request(self, method: str, path: str, **kwargs: Any) -> _BoundedResponse:
        deadline = Deadline.after(self._timeout)
        try:
            if self._client is not None:
                return self._request_with_client(
                    self._client, deadline, method, path, **kwargs
                )
            with httpx.Client(
                base_url="http://mailpit:8025",
                timeout=httpx.Timeout(deadline.remaining()),
                transport=DeadlineTransport(deadline),
                headers={"Accept-Encoding": "identity"},
                follow_redirects=False,
                trust_env=False,
            ) as client:
                return self._request_with_client(
                    client, deadline, method, path, **kwargs
                )
        except MailpitTransientError:
            raise
        except (httpx.HTTPError, TimeoutError):
            pass
        raise MailpitTransientError("Mailpit search unavailable") from None

    def _request_with_client(
        self,
        client: httpx.Client,
        deadline: Deadline,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> _BoundedResponse:
        deadline.remaining()
        with client.stream(
            method, path, timeout=deadline.remaining(), **kwargs
        ) as response:
            deadline.remaining()
            if response.status_code != 200:
                return _BoundedResponse(response.status_code)
            encoding = response.headers.get("content-encoding", "").strip().lower()
            if encoding and encoding != "identity":
                raise MailpitTransientError("unsupported Mailpit response encoding")
            declared = response.headers.get("content-length")
            length: int | None = None
            if declared is not None:
                if (
                    len(declared) > 20
                    or not declared.isascii()
                    or not declared.isdigit()
                ):
                    raise MailpitTransientError("invalid Mailpit response")
                length = int(declared)
                if length > self._max_response_bytes:
                    raise MailpitTransientError("Mailpit response too large")
            content = bytearray()
            chunks = (
                (response.content,)
                if response.is_stream_consumed
                else response.iter_raw()
            )
            iterator = iter(chunks)
            while True:
                deadline.remaining()
                try:
                    chunk = next(iterator)
                except StopIteration:
                    deadline.remaining()
                    break
                deadline.remaining()
                if len(content) + len(chunk) > self._max_response_bytes:
                    raise MailpitTransientError("Mailpit response too large")
                content.extend(chunk)
            if length is not None and length != len(content):
                raise MailpitTransientError("invalid Mailpit response")
            return _BoundedResponse(response.status_code, bytes(content))
