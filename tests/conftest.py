from __future__ import annotations

import os
import subprocess
import time
from uuid import uuid4

import pytest
import redis

REDIS_IMAGE = (
    "redis@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99"
)


@pytest.fixture(scope="session")
def redis_url() -> str:
    configured = os.environ.get("SIGNALDESK_TEST_REDIS_URL")
    if configured:
        yield configured
        return
    name = f"signaldesk-email-test-{uuid4().hex[:12]}"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--publish",
            "127.0.0.1::6379",
            REDIS_IMAGE,
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "no",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    try:
        port_output = subprocess.run(
            ["docker", "port", name, "6379/tcp"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        port = port_output.rsplit(":", 1)[1]
        url = f"redis://127.0.0.1:{port}/0"
        client = redis.Redis.from_url(url, socket_connect_timeout=1, socket_timeout=1)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                if client.ping():
                    break
            except redis.RedisError:
                time.sleep(0.1)
        else:
            raise RuntimeError("pinned Redis fixture did not become ready")
        yield url
    finally:
        subprocess.run(
            ["docker", "rm", "--force", name],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )


@pytest.fixture
def redis_client(redis_url: str):
    client = redis.Redis.from_url(
        redis_url, decode_responses=False, socket_connect_timeout=1, socket_timeout=2
    )
    client.flushdb()
    yield client
    client.flushdb()
    client.close()
