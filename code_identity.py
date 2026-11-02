"""Shared code identity policy for extraction, queues, browser and persistence."""
from urllib.parse import urlsplit


# PATCH_MARKER_2026-10-28-validation-normalize-unified
CASE_SENSITIVE_CODE_DOMAINS = frozenset({
    "getcodexx8bonus.pages.dev",
})
_ALIASES = {"xx8": "getcodexx8bonus.pages.dev"}


def normalize_code_domain(domain: str) -> str:
    raw = str(domain or "").strip().lower()
    if not raw:
        return ""
    try:
        host = urlsplit(raw if "://" in raw else "//" + raw).hostname or ""
    except ValueError:
        host = raw.split("/", 1)[0].split(":", 1)[0]
    host = host.removeprefix("www.").rstrip(".")
    return _ALIASES.get(host, host)


def normalize_code(code: str, domain: str = "", *, force_uppercase: bool = False) -> str:
    """Preserve sensitive codes; fold other domains to uppercase.

    Without domain context, preserve case to avoid merging distinct codes.
    force_uppercase=True also folds codes without domain context, but never
    overrides a sensitive site's policy. Character cleaning is separate.
    """
    value = str(code or "").strip()
    canonical = normalize_code_domain(domain)
    if canonical in CASE_SENSITIVE_CODE_DOMAINS:
        return value
    return value.upper() if canonical or force_uppercase else value
