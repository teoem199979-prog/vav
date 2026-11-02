

from __future__ import annotations

import json
import re
import time
from uuid import uuid4
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from config import Config
from logger_setup import logger


def normalize_domain(url: str) -> str:

    parsed = urlparse(url or "")
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "").strip("/")


_LAST_SHOT_AT: dict = {}
_ACTIVE_SCREENSHOT_GROUPS: set[str] = set()


def _screenshot_throttled(domain: str, status: str) -> bool:
    interval = float(getattr(Config, "SCREENSHOT_MIN_INTERVAL_SECONDS", 60.0))
    if interval <= 0:
        return False
    key = (domain, status)
    now = time.monotonic()
    last = _LAST_SHOT_AT.get(key)
    if last is not None and now - last < interval:
        return True
    _LAST_SHOT_AT[key] = now
    return False


def _prune_screenshots(shot_dir) -> None:
    """Retain up to SCREENSHOT_MAX_FILES complete evidence groups."""
    limit = int(getattr(Config, "SCREENSHOT_MAX_FILES", 300))
    if limit <= 0:
        return
    try:
        groups: dict[str, list[Path]] = {}
        for path in shot_dir.iterdir():
            if not path.is_file():
                continue
            if path.name.endswith(".debug.json"):
                base_name = path.name[:-len(".debug.json")]
            elif path.suffix in {".png", ".html"}:
                base_name = path.stem
            else:
                continue
            groups.setdefault(base_name, []).append(path)
        overflow = len(groups) - limit
        if overflow <= 0:
            return
        eligible = [files for base, files in groups.items() if base not in _ACTIVE_SCREENSHOT_GROUPS]
        eligible.sort(key=lambda files: max(path.stat().st_mtime for path in files))
        for files in eligible[:overflow]:
            for path in files:
                try:
                    path.unlink()
                except OSError:
                    pass
    except Exception:
        pass


def _safe_artifact_part(value: str, fallback: str = "unknown", limit: int = 80) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "")).strip("._-")
    return (cleaned or fallback)[:limit]


async def take_result_screenshot(
    page,
    user: str,
    code: str,
    target_url: str,
    status: str,
    debug_info: dict | None = None,
) -> str:

    enabled = "SCREENSHOT_ON_FAILED" if status == "FAILED" else "SCREENSHOT_ON_UNKNOWN"
    if not bool(getattr(Config, enabled, False)):
        return ""
    try:
        if _screenshot_throttled(normalize_domain(target_url), str(status)):
            return ""
        shot_dir = Path("logs/screenshots")
        shot_dir.mkdir(parents=True, exist_ok=True)
        safe_domain = _safe_artifact_part(normalize_domain(target_url).replace(".", "_"))
        safe_user = _safe_artifact_part(user)
        safe_code = _safe_artifact_part(code)
        safe_status = _safe_artifact_part(status)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        base_name = f"{safe_domain}_{safe_user}_{safe_code}_{safe_status}_{ts}_{uuid4().hex[:8]}"
        path = shot_dir / f"{base_name}.png"
        _ACTIVE_SCREENSHOT_GROUPS.add(base_name)
        try:
            await page.screenshot(path=str(path), full_page=False)

            try:
                html = await page.content()
                html_path = shot_dir / f"{base_name}.html"
                html_path.write_text(html, encoding="utf-8")
            except Exception as e:
                logger.debug(f"⚠️ Cannot save page HTML: {e}")

            if debug_info:
                try:
                    debug_path = shot_dir / f"{base_name}.debug.json"
                    debug_path.write_text(
                        json.dumps(debug_info, ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8",
                    )
                except Exception as e:
                    logger.debug(f"⚠️ Cannot save debug JSON: {e}")
            return str(path)
        finally:
            _ACTIVE_SCREENSHOT_GROUPS.discard(base_name)
            _prune_screenshots(shot_dir)
    except Exception as e:
        logger.debug(f"⚠️ Cannot take screenshot: {e}")
        return ""
