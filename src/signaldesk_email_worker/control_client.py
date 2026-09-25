from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal, Self
from uuid import UUID

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    StringConstraints,
    ValidationError,
    model_validator,
)

from signaldesk_email_worker._deadline_http import Deadline, DeadlineTransport


class ControlError(RuntimeError):
    pass


class FatalCredentialError(ControlError):
    pass


class NotFoundError(ControlError):
    pass


class StateConflict(ControlError):
    pass


class TransientControlError(ControlError):
    pass


@dataclass(frozen=True)
class _BoundedResponse:
    status_code: int
    content: bytes = b""


Recipient = Annotated[str, StringConstraints(min_length=3, max_length=320)]
TemplateName = Literal["diagnostic_completed", "export_completed"]


class DeliveryScope(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    email_delivery_id: UUID
    organization_id: UUID
    recipient_email: Recipient
    template_name: TemplateName
    template_data: dict[str, JsonValue]
    correlation_id: UUID
    status: Literal["pending", "sending", "sent", "failed"]
    message_id: str
    attempted_at: datetime | None
    sent_at: datetime | None
    observed_at: datetime

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        for timestamp in (self.attempted_at, self.sent_at, self.observed_at):
            if timestamp is not None and (
                timestamp.tzinfo is None or timestamp.utcoffset() is None
            ):
                raise ValueError("delivery timestamps must be timezone-aware")
        if self.message_id != f"<{self.email_delivery_id}@signaldesk.local>":
            raise ValueError("message ID does not match delivery")
        if self.status in {"sending", "failed"} and (
            self.attempted_at is None or self.sent_at is not None
        ):
            raise ValueError("invalid unsent timestamps")
        if self.status == "sent" and (
            self.attempted_at is None or self.sent_at is None
        ):
            raise ValueError("invalid sent timestamps")
        if self.status == "pending" and (
            self.attempted_at is not None or self.sent_at is not None
        ):
            raise ValueError("invalid pending timestamps")
        if self.attempted_at is not None and self.observed_at < self.attempted_at:
            raise ValueError("observation predates attempt")
        if self.sent_at is not None and self.observed_at < self.sent_at:
            raise ValueError("observation predates sent state")
        return self


class ControlApiClient:
    def __init__(
        self,
        *,
        base_url: str,
        credential: str,
        timeout: float,
        max_response_bytes: int = 16_384,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not 256 <= max_response_bytes <= 65_536:
            raise ValueError("max_response_bytes must be between 256 and 65536")
        if not 0 < timeout <= 30:
            raise ValueError("timeout must be between 0 and 30 seconds")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_response_bytes = max_response_bytes
        self._headers = {
            "Accept-Encoding": "identity",
            "X-SignalDesk-Service-Credential": credential,
        }
        self._client = (
            httpx.Client(
                base_url=self._base_url,
                timeout=httpx.Timeout(timeout),
                transport=transport,
                headers=self._headers,
                follow_redirects=False,
                trust_env=False,
            )
            if transport is not None
            else None
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def claim(self, delivery_id: UUID) -> DeliveryScope:
        return self._scope_request(
            "POST", f"/internal/email-deliveries/{delivery_id}/claim", delivery_id
        )

    def fetch(self, delivery_id: UUID) -> DeliveryScope:
        return self._scope_request(
            "GET", f"/internal/email-deliveries/{delivery_id}", delivery_id
        )

    def mark_sent(self, delivery_id: UUID) -> DeliveryScope:
        scope = self._scope_request(
            "POST", f"/internal/email-deliveries/{delivery_id}/sent", delivery_id
        )
        if scope.status != "sent":
            raise TransientControlError("invalid control API response")
        return scope

    def mark_failed(self, delivery_id: UUID) -> DeliveryScope:
        scope = self._scope_request(
            "POST", f"/internal/email-deliveries/{delivery_id}/failed", delivery_id
        )
        if scope.status != "failed":
            raise TransientControlError("invalid control API response")
        return scope

    def _scope_request(
        self, method: str, path: str, delivery_id: UUID
    ) -> DeliveryScope:
        response = self._request(method, path)
        self._raise_status(response)
        scope: DeliveryScope | None = None
        try:
            scope = DeliveryScope.model_validate_json(response.content)
        except (ValidationError, ValueError):
            pass
        if scope is None:
            raise TransientControlError("invalid control API response")
        if scope.email_delivery_id != delivery_id:
            raise TransientControlError("invalid control API response")
        return scope

    def _request(self, method: str, path: str, **kwargs: Any) -> _BoundedResponse:
        deadline = Deadline.after(self._timeout)
        try:
            if self._client is not None:
                return self._request_with_client(
                    self._client, deadline, method, path, **kwargs
                )
            with httpx.Client(
                base_url=self._base_url,
                timeout=httpx.Timeout(deadline.remaining()),
                transport=DeadlineTransport(deadline),
                headers=self._headers,
                follow_redirects=False,
                trust_env=False,
            ) as client:
                return self._request_with_client(
                    client, deadline, method, path, **kwargs
                )
        except TransientControlError:
            raise
        except (httpx.HTTPError, TimeoutError):
            pass
        raise TransientControlError("control API unavailable") from None

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
                raise TransientControlError(
                    "control API response has unsupported content encoding"
                )
            declared = response.headers.get("content-length")
            length: int | None = None
            if declared is not None:
                if (
                    len(declared) > 20
                    or not declared.isascii()
                    or not declared.isdigit()
                ):
                    raise TransientControlError("invalid control API response")
                length = int(declared)
                if length > self._max_response_bytes:
                    raise TransientControlError("control API response too large")
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
                    raise TransientControlError("control API response too large")
                content.extend(chunk)
            if length is not None and length != len(content):
                raise TransientControlError("invalid control API response")
            return _BoundedResponse(response.status_code, bytes(content))

    @staticmethod
    def _raise_status(response: _BoundedResponse) -> None:
        if response.status_code == 200:
            return
        if response.status_code in {401, 403}:
            raise FatalCredentialError("control API credential denied")
        if response.status_code == 404:
            raise NotFoundError("email delivery not found")
        if response.status_code == 409:
            raise StateConflict("email delivery state conflict")
        raise TransientControlError("control API request failed")
