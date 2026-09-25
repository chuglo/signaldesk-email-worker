FROM python:3.11.15-slim-bookworm@sha256:b18992999dbe963a45a8a4da40ac2b1975be1a776d939d098c647482bcad5cba

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/tmp \
    PATH=/app/.venv/bin:$PATH
WORKDIR /app
COPY --from=contracts pyproject.toml README.md /build/signaldesk-contracts/
COPY --from=contracts src /build/signaldesk-contracts/src
COPY pyproject.toml README.md uv.lock /build/signaldesk-email-worker/
COPY src /build/signaldesk-email-worker/src
RUN python -m pip install uv==0.11.31 \
    && cd /build/signaldesk-email-worker \
    && UV_PROJECT_ENVIRONMENT=/app/.venv uv sync --locked --no-dev --no-editable \
    && rm -rf /build
COPY --from=deploy scripts/wait-for-health.py /opt/signaldesk/wait-for-health.py
RUN chmod 0555 /opt/signaldesk/*.py
USER 10001:10001
CMD ["signaldesk-email-worker"]
