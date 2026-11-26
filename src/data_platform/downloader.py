"""Synchronous HTTP downloads with a shared, API-independent retry policy."""

from __future__ import annotations

import logging
import math
import random
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from types import TracebackType

import httpx

logger = logging.getLogger(__name__)
USER_AGENT = "data-platform"

QueryValue = str | int | float | bool | None
QueryParams = dict[str, QueryValue | Sequence[QueryValue]]


@dataclass(frozen=True)
class RetryPolicy:
    """max_attempts includes the initial request; delays are measured in seconds."""

    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 30.0
    retry_statuses: tuple[int, ...] = (408, 429, 500, 502, 503, 504)

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if not math.isfinite(self.base_delay) or self.base_delay <= 0:
            raise ValueError("base_delay must be finite and greater than zero")
        if not math.isfinite(self.max_delay) or self.max_delay < self.base_delay:
            raise ValueError("max_delay must be finite and at least base_delay")
        if any(not 400 <= status < 600 for status in self.retry_statuses):
            raise ValueError("retry_statuses must contain HTTP error status codes")


class Downloader:
    """Reuse one connection pool across GET downloads; close it after use.

    Responses are fully buffered. Parsing, pagination, and persistence belong to
    the caller. Injected clients remain owned by the caller. Their transport
    should not add its own retries, which would multiply this policy's attempts.
    """

    def __init__(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        uniform: Callable[[float, float], float] = random.uniform,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be finite and greater than zero")
        # Transport logs can include API keys in URLs and authentication headers.
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        self.retry_policy = retry_policy or RetryPolicy()
        self.timeout = timeout
        self._owns_client = client is None
        self._client = client if client is not None else httpx.Client()
        self._sleep = sleep
        self._uniform = uniform
        self._now = now or (lambda: datetime.now(timezone.utc))

    def get(
        self,
        url: str,
        *,
        params: QueryParams | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Return a successful response or raise the final HTTPX exception.

        Retry transient timeouts, network errors, remote protocol errors, and
        configured status codes. Redirects and other HTTP errors fail immediately.
        Only GET is exposed so write requests cannot be retried accidentally.
        """
        request_headers = httpx.Headers({"User-Agent": USER_AGENT})
        request_headers.update(headers or {})
        policy = self.retry_policy
        ceiling = policy.base_delay
        for attempt in range(1, policy.max_attempts + 1):
            logger.debug("GET attempt %d/%d", attempt, policy.max_attempts)
            retry_after = None
            try:
                response = self._client.get(
                    url, params=params, headers=request_headers, timeout=self.timeout
                )
            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.RemoteProtocolError,
            ):
                if attempt == policy.max_attempts:
                    logger.error("GET failed after %d attempts: transient network error", attempt)
                    raise
                reason = "transient network error"
            else:
                if response.status_code not in policy.retry_statuses:
                    if not response.is_success:
                        logger.error("GET failed: HTTP %d", response.status_code)
                    response.raise_for_status()
                    logger.debug("GET succeeded: HTTP %d; attempt %d", response.status_code, attempt)
                    return response
                if attempt == policy.max_attempts:
                    logger.error("GET failed after %d attempts: HTTP %d", attempt, response.status_code)
                    response.raise_for_status()
                retry_after = self._retry_after(response.headers.get("Retry-After"))
                reason = f"HTTP {response.status_code}"
                response.close()

            # Full jitter: uniform(0, min(max_delay, base_delay * 2**retry_index)).
            delay = self._uniform(0.0, ceiling)
            if retry_after is not None:
                # Server guidance is a minimum, even when it exceeds max_delay.
                delay = max(delay, retry_after)
            # Deliberately omit URLs, parameters, headers, and response bodies.
            logger.warning(
                "GET retry after %s: attempt %d/%d; sleeping %.3fs",
                reason, attempt, policy.max_attempts, delay,
            )
            self._sleep(delay)
            ceiling = min(policy.max_delay, ceiling * 2)

        raise AssertionError("Retry loop must return or raise")

    def _retry_after(self, value: str | None) -> float | None:
        if value is None:
            return None
        try:
            seconds = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                seconds = (retry_at - self._now()).total_seconds()
            except (ValueError, TypeError, OverflowError):
                return None
        return max(0.0, seconds) if math.isfinite(seconds) else None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> Downloader:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
