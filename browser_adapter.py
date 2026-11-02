

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from browser_site_profiles import get_site_profile
from logger_setup import logger
from config import is_domain_enabled
from code_identity import normalize_code


_RATE_LIMIT_RE = re.compile(
    r"(rate[\s_-]?limit|too\s+many\s+requests|quota\s+exceeded|"
    r"không\s+thao\s+tác\s+liên\s+tục|khong\s+thao\s+tac\s+lien\s+tuc|"
    r"thao\s+tác\s+quá\s+nhanh|"
    r"(?<![\d.,])\b429\b(?!\s*(?:ĐIỂM|DIEM|XU|COIN|POINT)))",
    re.IGNORECASE,
)


class BrowserResultKind(str, Enum):
    SUCCESS = "SUCCESS"
    BUSINESS_FAILURE = "BUSINESS_FAILURE"
    RATE_LIMITED = "RATE_LIMITED"
    TECHNICAL_FAILURE = "TECHNICAL_FAILURE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class BrowserResult:
    kind: BrowserResultKind
    success: bool
    message: str = ""
    elapsed_seconds: float = 0.0
    evidence: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


class BrowserEngineAdapter:

    def __init__(self, engine: Any):
        self.engine = engine

    async def submit_for_target(
        self, user: str, code: str, target_url: str, systems: dict, *, sync_expected: int = 1
    ) -> BrowserResult:
        if not is_domain_enabled(target_url):
            return BrowserResult(BrowserResultKind.BUSINESS_FAILURE, False, "Site disabled", raw={"success": False, "status": "SITE_DISABLED"})
        profile = get_site_profile(target_url)
        if profile is None:
            return BrowserResult(BrowserResultKind.TECHNICAL_FAILURE, False, "No browser profile")
        code = normalize_code(code, target_url)
        started = time.monotonic()
        try:
            raw = await self.engine.submit_code_browser(
                user, code, target_url, systems, sync_expected=sync_expected
            )
        except asyncio.TimeoutError as exc:
            elapsed = time.monotonic() - started
            return BrowserResult(
                BrowserResultKind.TECHNICAL_FAILURE,
                False,
                f"timeout: {exc}",
                elapsed,
            )
        except ValueError as exc:
            elapsed = time.monotonic() - started
            return BrowserResult(
                BrowserResultKind.BUSINESS_FAILURE,
                False,
                f"invalid input: {exc}",
                elapsed,
            )
        except (KeyError, AttributeError, TypeError) as exc:
            logger.exception("Browser adapter internal error")
            elapsed = time.monotonic() - started
            return BrowserResult(
                BrowserResultKind.TECHNICAL_FAILURE,
                False,
                f"internal: {exc}",
                elapsed,
            )
        except Exception as exc:
            elapsed = time.monotonic() - started
            return BrowserResult(
                BrowserResultKind.TECHNICAL_FAILURE,
                False,
                str(exc),
                elapsed,
            )
        elapsed = time.monotonic() - started
        if raw.get("_infra_failure"):
            return BrowserResult(BrowserResultKind.TECHNICAL_FAILURE, False, str(raw.get("message", "browser transport failure")), elapsed, raw=raw)
        message = str(raw.get("message", ""))
        if raw.get("rate_limited") or _RATE_LIMIT_RE.search(message):
            return BrowserResult(BrowserResultKind.RATE_LIMITED, False, message or "rate limited", elapsed, raw=raw)
        if raw.get("success"):
            return BrowserResult(BrowserResultKind.SUCCESS, True, str(raw.get("message", "")), elapsed, raw=raw)
        if raw.get("is_wrong_code"):
            return BrowserResult(BrowserResultKind.BUSINESS_FAILURE, False, str(raw.get("message", "business failure")), elapsed, raw=raw)
        return BrowserResult(BrowserResultKind.UNKNOWN, False, str(raw.get("message", "unknown browser result")), elapsed, raw=raw)


__all__ = ["BrowserEngineAdapter", "BrowserResult", "BrowserResultKind"]
