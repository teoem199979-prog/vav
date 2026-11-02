"""Random code selection, verification readiness and early OCR dispatch.

FRESHEST_FIRST_DOMAINS is a compatibility name for bounded-queue selection;
all active domain queues draw codes randomly. VERIFICATION_GATE_DOMAINS
controls waiting for a usable tab before taking another queued code.
EarlyDispatch reconciles early and final OCR batches without duplicate sends.
"""
from __future__ import annotations

import asyncio
import random

from config import Config
from code_identity import normalize_code_domain

# PATCH_MARKER_2026-10-28-queue-bounded-priority


def random_codes(items, limit=None):
    """Randomize available candidates before any per-message code limit."""
    values = list(items)
    count = len(values) if limit is None else min(len(values), max(0, int(limit)))
    return random.sample(values, count)

def _domains(name: str) -> tuple:
    return tuple(getattr(Config, name, ()) or ())


def freshest_first_enabled(domain: str) -> bool:
    canonical = normalize_code_domain(domain)
    return canonical == "xx88code.com" or canonical in {
        normalize_code_domain(item) for item in _domains("FRESHEST_FIRST_DOMAINS")
    }


def verification_gate_enabled(domain: str) -> bool:
    canonical = normalize_code_domain(domain)
    return bool(canonical) and canonical in {
        normalize_code_domain(item) for item in _domains("VERIFICATION_GATE_DOMAINS")
    }


async def hold_until_verification_ready(domain: str, engine, timeout: float = 1.0) -> bool:
    """Return True after waiting, so callers recheck before dequeuing.

    Missing engine hooks retain compatibility. A wait error after confirmed
    blocking does not make the queued code eligible for dispatch.
    """
    if engine is None or not verification_gate_enabled(domain):
        return False

    was_blocked = False
    try:
        blocked = getattr(engine, "verification_blocked", None)
        wait_ready = getattr(engine, "wait_for_verification_ready", None)

        if blocked is None or wait_ready is None:
            return False

        if not blocked(domain):
            return False
        was_blocked = True

        inner = max(0.05, float(timeout))
        # The engine enforces `inner` itself and returns normally on timeout.
        # The outer guard is only a safety net for a hung engine, so it must be
        # longer than `inner`; otherwise both fire together and we wrongly
        # report "did not wait" while the tabs are still verification-blocked.
        await asyncio.wait_for(wait_ready(domain, timeout=inner), timeout=inner + 0.5)
        return True
    except Exception:
        # A failed/timed-out wait does not prove the blocked tab became ready.
        if was_blocked:
            await asyncio.sleep(0.05)
        return was_blocked


class EarlyDispatch:
    """Gửi mã ngay khi OCR video phát hiện (on_code), rồi đối chiếu với kết quả cuối.

    on_code(item) chạy trong luồng worker OCR và an toàn với luồng; việc gửi thật (coroutine submit)
    chạy trên event loop. Mã đã gửi sớm được loại khỏi lô cuối để không bị gửi hai lần (hai tài
    khoản khác nhau); mã mà lần gửi sớm bị lỗi vẫn nằm trong lô cuối nên không bị mất.
    """

    def __init__(self, loop, submit):
        self._loop = loop
        self._submit = submit
        self._futures = []
        self._done = set()
        self.errors = []

    async def _run(self, item):
        await self._submit(item)
        code = str(item.get("code") or "").strip()
        if code:
            self._done.add(code)

    def on_code(self, item) -> None:
        import asyncio
        self._futures.append(asyncio.run_coroutine_threadsafe(self._run(dict(item)), self._loop))

    async def settle(self, codes, emitted):
        """Chờ các lần gửi sớm xong; trả (codes còn lại cho lô cuối, tập mã đã gửi sớm thành công)."""
        pending = list(self._futures)
        if pending:
            outcomes = await asyncio.gather(*(asyncio.wrap_future(f) for f in pending), return_exceptions=True)
            self.errors = [o for o in outcomes if isinstance(o, BaseException)]

        skipped = {str(code).strip() for code in (emitted or ()) if str(code).strip() in self._done}
        remaining = [item for item in (codes or []) if str(item.get("code") or "").strip() not in skipped]
        return remaining, skipped
