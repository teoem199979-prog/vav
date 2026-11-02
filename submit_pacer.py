"""Per-site submit pacing using monotonic clocks (no network, no CAPTCHA workarounds).

The pacer reserves slots before a browser submission; a server/business throttle
pauses both the affected account and (for that site) the domain. Keeping the
cooldown in one shared object prevents parallel workers from racing into a burst.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


# PATCH_MARKER_2026-10-28-outcome-db-http-pacer
@dataclass
class _Penalty:
    strikes: int = 0
    last_at: float = 0.0


class SubmitPacer:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._account_ready: dict[tuple[str, str], float] = {}
        self._domain_ready: dict[str, float] = {}
        self._penalties: dict[str, _Penalty] = {}
        # Số liệu để chỉnh khoảng cách gửi: số lần đã đặt slot kể từ lần throttle trước
        # và khoảng thời gian giữa hai lần throttle liên tiếp, theo domain.
        self._sent_since_throttle: dict[str, int] = {}
        self._last_throttle_at: dict[str, float] = {}
        self._last_throttle_stats: dict[str, tuple[int, float | None]] = {}

    async def wait_ready(self, domain: str, account: str = "") -> float:
        """Wait for an existing account/site cooldown without booking a slot.

        Called before resource semaphores to avoid holding a browser worker while
        the website has instructed us to stop. ``reserve`` must still run just
        before submission because another worker can extend the cooldown.
        """
        start = time.monotonic()
        key = (domain, account) if account else None
        while True:
            async with self._lock:
                now = time.monotonic()
                account_ready = self._account_ready.get(key, 0.0) if key else 0.0
                wait = max(account_ready, self._domain_ready.get(domain, 0.0)) - now
                if wait <= 0:
                    return max(0.0, now - start)
            # The deadlines only move forward (reserve/throttle), so it is safe
            # to sleep until the current deadline. The loop re-checks any
            # throttle extension on wake, without 2 wake-ups/second/worker.
            await asyncio.sleep(wait)

    async def reserve(
        self, domain: str, account: str, *, account_gap: float, domain_gap: float,
    ) -> float:
        """Wait until both limits permit an attempt, then reserve the slot atomically.

        Re-evaluates after every wait: a throttle arriving in another worker
        must extend this wait rather than be ignored by an already-sleeping task.
        Returns total time spent waiting, in seconds.
        """
        key = (domain, account)
        account_gap = max(0.0, float(account_gap))
        domain_gap = max(0.0, float(domain_gap))
        start = time.monotonic()
        while True:
            async with self._lock:
                now = time.monotonic()
                ready = max(
                    self._account_ready.get(key, 0.0),
                    self._domain_ready.get(domain, 0.0),
                )
                remaining = ready - now
                if remaining <= 0:
                    self._account_ready[key] = now + account_gap
                    self._domain_ready[domain] = now + domain_gap
                    self._sent_since_throttle[domain] = self._sent_since_throttle.get(domain, 0) + 1
                    return max(0.0, now - start)
            # Re-check on wake because a different worker may have extended
            # the account/domain cooldown while this task was sleeping.
            await asyncio.sleep(remaining)

    async def throttle(
        self, domain: str, account: str | float = "", *,
        initial: float | None = None, maximum: float | None = None,
        seconds: float | None = None,
    ) -> float:
        """Close the account/domain gate after an explicit site throttle result."""
        if seconds is not None:
            initial = float(seconds)
        if initial is None:
            initial = float(account)
            account = ""
        if maximum is None:
            maximum = initial
        initial = max(1.0, float(initial))
        maximum = max(initial, float(maximum))
        async with self._lock:
            now = time.monotonic()
            state = self._penalties.setdefault(domain, _Penalty())
            state.strikes = (state.strikes + 1) if now - state.last_at < 120.0 else 1
            state.last_at = now
            prev = self._last_throttle_at.get(domain)
            self._last_throttle_stats[domain] = (
                self._sent_since_throttle.get(domain, 0),
                None if prev is None else now - prev,
            )
            self._sent_since_throttle[domain] = 0
            self._last_throttle_at[domain] = now
            delay = min(maximum, initial * (2 ** min(state.strikes - 1, 5)))
            until = now + delay
            if account:
                key = (domain, str(account))
                self._account_ready[key] = max(self._account_ready.get(key, 0.0), until)
            self._domain_ready[domain] = max(self._domain_ready.get(domain, 0.0), until)
            return delay

    def last_throttle_stats(self, domain: str | None = None):
        """(số lần gửi kể từ throttle trước, giây kể từ throttle trước hoặc None)."""
        if domain is None:
            return dict(self._last_throttle_stats)
        return self._last_throttle_stats.get(domain, (0, None))

    async def remaining(self, domain: str, account: str = "") -> float:
        async with self._lock:
            now = time.monotonic()
            account_ready = self._account_ready.get((domain, account), 0.0) if account else 0.0
            return max(0.0, max(
                self._domain_ready.get(domain, 0.0),
                account_ready,
            ) - now)

    async def accepted_response(self, domain: str) -> None:
        """A decisive non-throttle business response clears the escalation count."""
        async with self._lock:
            self._penalties.pop(domain, None)


pacer = SubmitPacer()
