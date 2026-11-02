

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


# PATCH_MARKER_2026-10-28-queue-bounded-priority

def _to_utc(value: Any) -> datetime | None:

    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_seconds(value: Any, now: datetime | None = None) -> float | None:

    if isinstance(value, dict):
        message = value.get("message")
        value = (
            value.get("msg_ts")
            or value.get("message_date")
            or value.get("date")
            or (getattr(message, "date", None) if message is not None else None)
        )
    dt = _to_utc(value)
    if dt is None:
        return None
    ref = _to_utc(now) if now is not None else datetime.now(timezone.utc)
    if ref is None:
        raise ValueError("now must be a valid datetime")
    return max(0.0, (ref - dt).total_seconds())


def is_stale(value: Any, max_age: float, now: datetime | None = None) -> bool:

    if max_age is None or float(max_age) <= 0:
        return False
    age = age_seconds(value, now)
    return age is not None and age > float(max_age)


def first_known_age(candidates, max_age: float | None = None, now: datetime | None = None):
    """Return the first parseable age, or the legacy (stale, age) pair if capped."""
    if isinstance(candidates, (dict, datetime, str)):
        candidates = (candidates,)
    for value in candidates:
        age = age_seconds(value, now)
        if age is not None:
            if max_age is None:
                return age
            return age > float(max_age) if max_age > 0 else False, age
    return None if max_age is None else (False, None)
