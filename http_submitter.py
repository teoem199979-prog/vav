"""Submit explicitly supported token-free APIs without a browser.

POSTs are never replayed here. A response that cannot confirm the business
outcome is held for review by the existing submission queue.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from browser_site_profiles import get_site_profile, uses_direct_http
from code_identity import normalize_code
from config import Config, is_domain_enabled
from logger_setup import logger
from submission_outcomes import ResultStatus, classify_result, record_outcome
from submit_pacer import pacer as submit_pacer


# PATCH_MARKER_2026-10-28-outcome-db-http-pacer
def classify_api_response(
    response: httpx.Response, user: str, code: str, domain: str,
) -> tuple[ResultStatus, str]:
    """Require the API response to echo this exact account and normalized code."""
    if response.status_code == 429:
        return ResultStatus.RATE_LIMITED, "HTTP 429: Too many requests"
    try:
        payload = response.json()
    except (ValueError, UnicodeError):
        return ResultStatus.AMBIGUOUS, f"HTTP {response.status_code}: API returned non-JSON content"
    if not isinstance(payload, dict):
        return ResultStatus.AMBIGUOUS, "Unexpected API response shape"
    message = payload.get("message")
    message = message.strip() if isinstance(message, str) else ""
    data = payload.get("data")
    echo = data if isinstance(data, dict) else payload
    account = echo.get("account", echo.get("Account", echo.get("username", echo.get("user"))))
    echoed_code = echo.get("code", echo.get("Code", echo.get("giftcode")))
    if (account is None or echoed_code is None
            or str(account).strip().casefold() != user.strip().casefold()
            or normalize_code(str(echoed_code), domain) != normalize_code(code, domain)):
        return ResultStatus.AMBIGUOUS, "API response account/code does not match the submitted request"
    if not (200 <= response.status_code < 300 or response.status_code in {400, 422}):
        return ResultStatus.AMBIGUOUS, f"HTTP {response.status_code}: {message or 'unconfirmed API result'}"
    business_code = str(payload.get("code", "")).strip()
    status = classify_result(message, api_result=message)
    if business_code == "429" or status == ResultStatus.RATE_LIMITED:
        return ResultStatus.RATE_LIMITED, message or "API rate limit"
    if status in {ResultStatus.ACCOUNT_LIMIT, ResultStatus.CAPTCHA_EXPIRED}:
        # Verification changes require operator review, not a browser replay.
        if status == ResultStatus.CAPTCHA_EXPIRED:
            return ResultStatus.AMBIGUOUS, message
        return status, message
    if business_code == "200" and 200 <= response.status_code < 300:
        if status in {ResultStatus.SUCCESS_POINTS, ResultStatus.SUCCESS_NO_POINTS}:
            return status, message
    elif business_code == "400" and status == ResultStatus.FAILED:
        return status, message
    return ResultStatus.AMBIGUOUS, message or "API returned no decisive business result"


async def _no_screenshot(*args, **kwargs) -> str:
    return ""


def _retry_after_seconds(response: httpx.Response) -> float:
    value = response.headers.get("Retry-After", "")
    try:
        seconds = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return 0.0
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        seconds = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return min(3600.0, max(0.0, seconds))


class HttpSubmitter:
    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client
        self._account_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._dispatched: set[tuple[str, str, str]] = set()

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            limit = Config.DIRECT_HTTP_MAX_CONNECTIONS
            # Reuse TCP/TLS connections; HTTPX does not retry POSTs by default.
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(max_connections=limit, max_keepalive_connections=limit,
                                   keepalive_expiry=60.0),
                follow_redirects=False,
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._account_locks.clear()

    async def submit_for_target(
        self, user: str, code: str, target_url: str, systems: dict, *,
        callbacks: dict[str, Any] | None = None,
    ) -> dict:
        profile = get_site_profile(target_url)
        if (profile is None or profile.domain != "getcodexx8bonus.pages.dev"
                or not uses_direct_http(target_url)):
            raise ValueError("Direct HTTP is not configured for this site")
        domain = profile.domain
        if not is_domain_enabled(domain):
            return {"success": False, "status": "SITE_DISABLED", "message": "Site disabled"}
        user = str(user or "").strip()
        code = normalize_code(code, domain)
        if not user or not code:
            raise ValueError("Account and code are required")
        lock = self._account_locks.setdefault((domain, user.casefold()), asyncio.Lock())
        async with lock:
            return await self._submit(user, code, target_url, systems, profile, callbacks)

    async def _submit(self, user, code, target_url, systems, profile, callbacks) -> dict:
        started = time.perf_counter()
        domain = profile.domain
        timings: dict[str, float | int] = {}
        identity = (domain, user.casefold(), code)
        db = systems.get("db")

        async def database_call(name: str, *args):
            return await asyncio.get_running_loop().run_in_executor(
                systems.get("db_executor"), getattr(db, name),
                *(args if args else (domain, user, code)),
            )

        async def release_guard(token: str, ok: bool) -> None:
            if db is not None:
                await database_call(
                    "release_http_submission", domain, user, code, token, ok,
                )
            if not ok:
                self._dispatched.discard(identity)

        def finish(result: dict) -> dict:
            elapsed = time.perf_counter() - started
            timings["total_submit_ms"] = round(elapsed * 1000, 2)
            result.update(route="http", elapsed_seconds=elapsed, latency_ms=dict(timings))
            return result

        def unconfirmed(message: str) -> dict:
            return finish({"success": False, "message": message, "status": "AMBIGUOUS",
                           "failure_kind": "UNKNOWN_AFTER_SUBMIT", "is_wrong_code": False})

        sent = False
        response_received = False
        claim_token = ""
        guard_claimed = False
        retain_guard = False
        try:
            if identity in self._dispatched:
                return unconfirmed("Previous HTTP dispatch is confirmed or awaiting review; no repeat POST")
            client = self._get_client()
            account_gap, domain_gap = Config.SITE_SUBMIT_GAPS.get(domain, (0.0, 0.0))
            gap_wait = await submit_pacer.reserve(
                domain, user, account_gap=account_gap, domain_gap=domain_gap,
            )
            timings["submit_gap_wait_ms"] = round(gap_wait * 1000, 2)
            timeout = Config.DIRECT_HTTP_TIMEOUT_SECONDS
            connect_timeout = min(timeout, Config.DIRECT_HTTP_CONNECT_TIMEOUT_SECONDS)
            account_field, code_field = profile.direct_http_fields
            origin_parts = urlsplit(profile.origin)
            origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
            # Commit intent before any network write. A restarted bot or a new
            # Telegram message must not repeat a possibly redeemed request.
            if db is not None:
                claim_token = await database_call("claim_http_submission")
                if claim_token is None:
                    return unconfirmed("Previous HTTP dispatch is confirmed or awaiting review; no repeat POST")
            self._dispatched.add(identity)
            guard_claimed = True
            api_started = time.perf_counter()
            sent = True
            retain_guard = True
            # One POST only: no browser, redirect following, probe, or automatic replay.
            async with asyncio.timeout(timeout):
                response = await client.post(
                    profile.public_api_endpoint,
                    json={account_field: user, code_field: code},
                    headers={"Accept": "application/json", "Origin": origin,
                             "Referer": profile.origin},
                    timeout=httpx.Timeout(timeout, connect=connect_timeout, pool=connect_timeout),
                    follow_redirects=False,
                )
            response_received = True
            timings["api_ms"] = round((time.perf_counter() - api_started) * 1000, 2)
            timings["api_status"] = response.status_code
            status, message = classify_api_response(response, user, code, domain)
            retry_after = _retry_after_seconds(response) if status == ResultStatus.RATE_LIMITED else 0.0
            if status == ResultStatus.FAILED and domain == "getcodexx8bonus.pages.dev":
                logger.warning("[HTTP] %s rejected code: %s", domain, message[:160])
            outcome_callbacks = dict(callbacks or {})
            outcome_callbacks["take_screenshot"] = _no_screenshot
            outcome = await record_outcome(
                systems=systems, page=None, user=user, code=code, target_url=target_url,
                domain=domain, elapsed=time.perf_counter() - started,
                status=status, result_text=message, callbacks=outcome_callbacks,
                debug_info={"transport": "http", "http_status": response.status_code},
            )
            result = dict(outcome.result)
            retain_guard = status not in {
                ResultStatus.FAILED, ResultStatus.ACCOUNT_LIMIT, ResultStatus.RATE_LIMITED,
            }
            if status == ResultStatus.RATE_LIMITED:
                result["retry_after_seconds"] = max(
                    float(result.get("retry_after_seconds", 0)),
                    retry_after,
                )
            return finish(result)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            if response_received:
                return unconfirmed(f"HTTP result recording failed: {type(exc).__name__}")
            # These failures occur before this POST can reach the API.
            if guard_claimed:
                retain_guard = False
            return finish({"success": False, "message": f"HTTP connection unavailable: {type(exc).__name__}",
                           "_infra_failure": True, "failure_kind": "HTTP_CONNECT_FAILED"})
        except asyncio.CancelledError:
            if sent:
                # The outer submission timeout must not turn a possibly sent POST
                # into an infrastructure failure that the durable worker replays.
                return unconfirmed("HTTP request cancelled after dispatch; result unconfirmed")
            raise
        except Exception as exc:
            logger.warning("[HTTP] %s submission failed (%s)", domain, type(exc).__name__)
            if sent:
                return unconfirmed(f"HTTP result unconfirmed: {type(exc).__name__}")
            return finish({"success": False, "message": f"HTTP setup failed: {type(exc).__name__}",
                           "_infra_failure": True, "failure_kind": "HTTP_SETUP_FAILED"})
        finally:
            if guard_claimed and claim_token is not None:
                await release_guard(claim_token, retain_guard)
