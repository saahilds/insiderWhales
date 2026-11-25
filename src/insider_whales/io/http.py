"""Polite async HTTP client for SEC EDGAR: global rate limit, backoff, 429 handling."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

from insider_whales.config import SecConfig

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {403, 429, 500, 502, 503, 504}  # SEC answers 403 when throttling

Sleep = Callable[[float], Awaitable[None]]


class SecRequestError(RuntimeError):
    def __init__(self, url: str, reason: str):
        super().__init__(f"{url}: {reason}")
        self.url = url
        self.reason = reason


class RateLimiter:
    """Spaces request starts at least 1/rate seconds apart across all concurrent tasks."""

    def __init__(
        self,
        rate_per_second: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ):
        if rate_per_second <= 0:
            raise ValueError("rate must be positive")
        self.interval = 1.0 / rate_per_second
        self._clock = clock
        self._sleep = sleep
        self._next_slot = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = self._clock()
            wait = self._next_slot - now
            if wait > 0:
                await self._sleep(wait)
            self._next_slot = max(now, self._next_slot) + self.interval


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    sha256: str
    size: int
    last_modified: str | None
    etag: str | None


def _retry_after_seconds(resp: httpx.Response) -> float | None:
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
            return max(0.0, when.timestamp() - time.time())
        except (TypeError, ValueError):
            return None


class SecClient:
    """All SEC traffic goes through one instance so the rate limit is global."""

    def __init__(
        self,
        cfg: SecConfig,
        user_agent: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Sleep = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        seed: int = 0,
    ):
        self.cfg = cfg
        self.limiter = RateLimiter(cfg.max_requests_per_second, clock=clock, sleep=sleep)
        self._sleep = sleep
        self._rng = random.Random(seed)
        self._client = httpx.AsyncClient(
            headers={"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=cfg.timeout_seconds,
            follow_redirects=True,
            transport=transport,
        )
        self.requests_made = 0

    async def __aenter__(self) -> SecClient:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    def _backoff(self, attempt: int) -> float:
        base = min(self.cfg.backoff_max_seconds, self.cfg.backoff_base_seconds * 2**attempt)
        return base * (0.5 + self._rng.random() / 2)

    async def _send(self, method: str, url: str, stream_to: Path | None = None):
        last_reason = "no attempt made"
        for attempt in range(self.cfg.max_retries + 1):
            await self.limiter.acquire()
            self.requests_made += 1
            try:
                if stream_to is None:
                    resp = await self._client.request(method, url)
                    if resp.status_code < 400:
                        return resp, None
                else:
                    result = await self._stream(url, stream_to)
                    if isinstance(result, DownloadResult):
                        return None, result
                    resp = result
            except httpx.TransportError as e:
                last_reason = f"{type(e).__name__}: {e}"
                delay = self._backoff(attempt)
            else:
                last_reason = f"HTTP {resp.status_code}"
                if resp.status_code not in RETRYABLE_STATUS:
                    raise SecRequestError(url, last_reason)
                retry_after = _retry_after_seconds(resp) if resp.status_code == 429 else None
                delay = retry_after if retry_after is not None else self._backoff(attempt)
            if attempt < self.cfg.max_retries:
                log.warning(
                    "SEC %s %s failed (%s); retrying in %.1fs", method, url, last_reason, delay
                )
                await self._sleep(delay)
        raise SecRequestError(
            url, f"gave up after {self.cfg.max_retries + 1} attempts: {last_reason}"
        )

    async def _stream(self, url: str, dest: Path) -> DownloadResult | httpx.Response:
        tmp = dest.with_name(dest.name + ".part")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        h = hashlib.sha256()
        size = 0
        async with self._client.stream("GET", url) as resp:
            if resp.status_code >= 400:
                await resp.aread()
                return resp
            with tmp.open("wb") as f:
                async for chunk in resp.aiter_bytes(1 << 20):
                    f.write(chunk)
                    h.update(chunk)
                    size += len(chunk)
            expected = resp.headers.get("Content-Length")
            plain = expected is not None and "content-encoding" not in resp.headers
            if plain and int(expected) != size:
                tmp.unlink(missing_ok=True)
                raise httpx.ReadError(f"truncated download: {size} of {expected} bytes")
            tmp.replace(dest)
            return DownloadResult(
                path=dest,
                sha256=h.hexdigest(),
                size=size,
                last_modified=resp.headers.get("Last-Modified"),
                etag=resp.headers.get("ETag"),
            )

    async def get(self, url: str) -> httpx.Response:
        resp, _ = await self._send("GET", url)
        return resp

    async def head(self, url: str) -> httpx.Response:
        resp, _ = await self._send("HEAD", url)
        return resp

    async def download(self, url: str, dest: Path) -> DownloadResult:
        _, result = await self._send("GET", url, stream_to=dest)
        return result
