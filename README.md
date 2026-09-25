# SignalDesk Email Worker

A least-privilege Redis Streams worker that resolves every recipient, template, and template-data value from the SignalDesk control API and delivers fixture mail only to `mailpit:1025`.

## Delivery protocol

1. Consume an exact `EmailRequestedV1` identifier envelope from `signaldesk:emails`.
2. Claim the authoritative delivery in PostgreSQL through the control API. The claim freezes the currently authorized recipient and assigns the deterministic RFC Message-ID `<{delivery_uuid}@signaldesk.local>`. Export object keys are rendered only when their embedded organization UUID exactly matches that claimed delivery scope.
3. Search Mailpit for that Message-ID **before every possible SMTP send**.
4. If Mailpit already contains it, record `sent` without SMTP. If an authoritative search proves absence, atomically verify/renew the exact Redis owner and delivery generation immediately before one absolute-deadline plain-text SMTP transaction.
5. Definite SMTP rejection is durably recorded as `failed` before a fenced DLQ/ACK. Disconnects and timeouts remain acceptance-unknown and are never marked failed. ACK through an owner-and-delivery-generation-fenced Redis Lua operation only after durable terminal state is confirmed.

Redis alone does not make SMTP exactly-once. A process can crash after Mailpit accepts SMTP but before PostgreSQL records `sent`. Recovery handles that window by claiming/fetching the frozen `sending` record, reconciling its deterministic Message-ID in Mailpit, and marking it sent without a duplicate. Search failure or ambiguity never permits a resend. A successfully empty recovery search permits a resend only after `recovery_age_seconds`.

## Mailpit API contract

Reconciliation uses Mailpit's official v1 endpoint:

```text
GET /api/v1/search?query=message-id:{uuid}@signaldesk.local&start=0&limit=2
```

Sources inspected for Task 9:

- Official API documentation: <https://mailpit.axllent.org/docs/api-v1/>
- Official OpenAPI 2.0 source pinned at commit `e219c7379345a0e826f6f216cccc51e1328008d7`: <https://raw.githubusercontent.com/axllent/mailpit/e219c7379345a0e826f6f216cccc51e1328008d7/server/ui/api/v1/swagger.json>
- Official search grammar: <https://mailpit.axllent.org/docs/usage/search-filters/>

The real JSON response at that commit includes the legacy lower-case `count` key on every search response in addition to the documented `MessagesSummary` keys (`messages`, `messages_count`, `messages_unread`, `start`, `tags`, `total`, `unread`), with message summaries containing capitalized `MessageID`. The worker requires exactly that shape, enforces `count == len(messages)`, `count <= messages_count <= total`, the unread bounds, `start == 0`, and a page of at most two. Mailpit exposes `MessageID` as the inner addr-spec without RFC angle brackets; the worker therefore searches and compares the exact inner `{uuid}@signaldesk.local` while SMTP emits the RFC header with brackets. Duplicate, truncated, non-exact, inconsistent, extra, compressed, or oversized responses are fail-closed and cannot trigger SMTP.

The deployment task must pin Mailpit commit `e219c7379345a0e826f6f216cccc51e1328008d7` (or an image proven byte-shape compatible with its v1 search response). Unpinned `master` is not an accepted runtime dependency.

## Runtime confinement

Required settings use the `SIGNALDESK_EMAIL_WORKER_` prefix:

- `REDIS_URL` (masked, unauthenticated plain `redis://` DSN for database 0)
- `CONTROL_API_BASE_URL`
- `EMAIL_WORKER_SERVICE_CREDENTIAL` (at least 32 non-whitespace ASCII characters)
- `CONSUMER_NAME`

SMTP host/port, Mailpit API origin, sender, stream, group, and DLQ are literal-confined defaults and cannot be overridden to alternate destinations. Control and Mailpit HTTP operations ignore ambient proxy variables and use one monotonic deadline across isolated/reaped DNS, connect, TLS, write, headers, and every response-body read; they disable redirects, retries, keepalive, and HTTP/2. SMTP construction performs no DNS or connection work; each later SMTP transaction bounds DNS, connect, greeting, EHLO/HELO, envelope, DATA write, and final response under one deadline with bounded response lines and bytes. The worker performs no SMTP authentication, TLS, STARTTLS, MX lookup, relay, or redirect following.

`CONSUMER_NAME` is only a validated operator label (maximum 128 ASCII characters). Each `EmailConsumer` appends one fresh `uuid4().hex`, producing a non-secret process-incarnation ownership identity of at most 161 ASCII bytes. That identity is used for reads, reclaims, pending checks, renewals, ACK, and DLQ fencing, so a restarted same-label process cannot impersonate an earlier pending owner.

The batch size is exactly one; every other configured value is rejected. Redis uses RESP2 and DB 0 with health checks, `CLIENT SETINFO`, and automatic retries disabled (`health_check_interval=0`, `lib_name=None`, `lib_version=None`, `protocol=2`, `Retry(NoBackoff(), 0)`, `retry_on_timeout=False`, and `retry_on_error=[]`). One connect deadline covers isolated DNS plus every candidate address. If `C` is the Redis connect timeout, `S` the Redis socket timeout, `A` the control API deadline, `M` the Mailpit deadline, and `T` the SMTP deadline, the validated single-entry network-work budget is:

```text
W = S + 4A + M + T + 4(C + 2S)
stale_idle_ms > ceil(1000W) + 100ms
```

The leading `S` covers the acquisition response after Redis makes the entry pending. Each of the four later Redis commands separately budgets connect, socket write, and its sole response; the four control calls cover both possible claim/fetch and mark/fetch pairs. At defaults, `W = 10 + 20 + 5 + 10 + 4(3 + 20) = 137s`, so the default `150000ms` stale window exceeds the required `137100ms` threshold.

Run one bounded poll with:

```bash
signaldesk-email-worker --once
```
