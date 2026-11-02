
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Mapping
from urllib.parse import urlsplit


def normalize_domain(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlsplit(raw if "://" in raw else f"//{raw}")
    host = (parsed.hostname or "").lower().rstrip(".")
    return host.removeprefix("www.")


@dataclass(frozen=True)
class SiteProfile:
    domain: str
    origin: str
    username_selectors: tuple[str, ...]
    code_selectors: tuple[str, ...]
    submit_selector: str
    result_selectors: tuple[str, ...] = ()
    success_markers: tuple[str, ...] = ()
    failure_markers: tuple[str, ...] = ()
    timeout_seconds: float = 5.0
    navigation_timeout_seconds: float = 12.0

    tab_slots: int = 2
    form_settle_seconds: float = 0.03
    result_timeout_ms: int = 5000
    result_poll_interval: float = 0.10
    selector_fast_window_seconds: float = 0.80
    full_reload_every: int = 300

    public_api_endpoint: str | None = None
    api_response_timeout_ms: int = 4500
    api_capture_phase: str = "submit"
    # Explicit request schema for an API that needs no browser verification.
    direct_http_fields: tuple[str, str] | None = None

    popup_dismiss_selectors: tuple[str, ...] = ()
    popup_dismiss_texts: tuple[str, ...] = ()
    # PATCH_MARKER_2026-10-25-v8-bugfix (#5: shell-text guard doc tu profile. Domain khong khai
    # bao gi -> _is_site_shell_result_candidate tra False nhu cu.)
    shell_text_min_length: int = 180
    shell_text_prefixes: tuple[str, ...] = ()
    shell_text_contains: tuple[tuple[str, str], ...] = ()

    def validate(self) -> None:
        errors: list[str] = []
        domain = normalize_domain(self.domain)
        if not domain or "." not in domain:
            errors.append("domain is missing or invalid")
        origin = urlsplit(self.origin)
        if origin.scheme.lower() != "https" or not origin.hostname:
            errors.append("origin must use https")
        elif normalize_domain(self.origin) != domain:
            errors.append("origin hostname must match domain")
        if not self.username_selectors or not all(self.username_selectors):
            errors.append("username_selectors is empty or invalid")
        if not self.code_selectors or not all(self.code_selectors):
            errors.append("code_selectors is empty or invalid")
        if not self.submit_selector.strip():
            errors.append("submit_selector is empty")
        if (self.timeout_seconds <= 0 or self.navigation_timeout_seconds <= 0
                or self.api_response_timeout_ms <= 0):
            errors.append("timeouts must be positive")
        if (self.tab_slots < 1 or self.form_settle_seconds < 0
                or self.result_timeout_ms <= 0 or self.result_poll_interval <= 0
                or self.selector_fast_window_seconds < 0 or self.full_reload_every < 1):
            errors.append("tab_slots/form_settle_seconds are invalid")
        if self.api_capture_phase not in {"submit", "verification"}:
            errors.append("api_capture_phase must be submit or verification")
        if self.public_api_endpoint is not None:
            endpoint = urlsplit(self.public_api_endpoint)
            if endpoint.scheme.lower() != "https" or not endpoint.hostname:
                errors.append("public_api_endpoint must be an HTTPS URL")
        if self.direct_http_fields is not None:
            if (self.domain != "getcodexx8bonus.pages.dev"
                    or not self.public_api_endpoint
                    or len(self.direct_http_fields) != 2
                    or not all(isinstance(field, str) and field.strip()
                               for field in self.direct_http_fields)):
                errors.append("direct HTTP is allowed only for XX8 with an HTTPS endpoint and account/code fields")
        if errors:
            raise ValueError(f"{self.domain}: {', '.join(errors)}")


_COMMON_SUCCESS = ("thành công", "success", "điểm", "received", "approved")
_COMMON_FAILURE = ("sai", "invalid", "đã dùng", "expired", "not found", "error")


SITE_PROFILES: dict[str, SiteProfile] = {
    "llwincode.com": SiteProfile(
        "llwincode.com", "https://llwincode.com/",
        ("input[placeholder='Nhập tên tài khoản']", "input[autocomplete='username']"),
        ("input[placeholder='Nhập mã code']", "input[autocomplete='one-time-code']"),
        "button[aria-label='Nhận code ngay'], button[type='submit']",
        result_selectors=("[role='dialog']", "form [role='alert']", "[role='status']", "[data-sonner-toast]"),
        success_markers=_COMMON_SUCCESS,
        failure_markers=_COMMON_FAILURE + ("code đã sử dụng hết", "code_not_active"),
        tab_slots=1, result_timeout_ms=6000, result_poll_interval=.05,
        public_api_endpoint="https://api.llwincode.com/codes/use-code-public",
        # PATCH_MARKER_2026-10-26-llwin-slider-logs2: dong ho API tinh tu
        # luc bam submit, ma lan keo (~1-2s, toi da 2 attempt) xay ra
        # truoc khi POST duoc gui. 5000ms cu de het budget tren lan keo
        # cham; 8000ms + result 6000ms (tu 4500ms) du cho keo + POST.
        # tab_slots GIU 1 (khong lay 2 cua ban slider cu) vi
        # PARALLEL_DOMAIN_TAB_LIMITS trong config logs-2 dat llwin = 1.
        api_response_timeout_ms=8000, api_capture_phase="verification",
        popup_dismiss_selectors=("button[aria-label='Dong popup']",),
        popup_dismiss_texts=("đóng", "xác nhận"),
    ),
    "getcodexx8bonus.pages.dev": SiteProfile(
        "getcodexx8bonus.pages.dev", "https://getcodexx8bonus.pages.dev/",

        ("#txtAccount", "input[placeholder='Nhập tên đăng nhập']"),
        ("#txtCode", "input[placeholder='Nhập mã code']"),

        "#btnCheckCode",
        result_selectors=(
            ".swal2-popup .swal2-html-container", ".swal2-popup .swal2-title",
            ".swal2-popup", "[role='alert']", "[role='status']",
        ),
        success_markers=_COMMON_SUCCESS,
        failure_markers=_COMMON_FAILURE,
        tab_slots=2,
        form_settle_seconds=0.01,
        result_timeout_ms=7000,
        result_poll_interval=0.08,
        selector_fast_window_seconds=0.18,
        full_reload_every=200,

        public_api_endpoint="https://api.xx8web.vip/Promotion/GetCODEXX8Bonus",
        direct_http_fields=("Account", "Code"),
        api_response_timeout_ms=7000,
        api_capture_phase="submit",

        popup_dismiss_selectors=(
            ".swal2-popup.swal2-show button.swal2-confirm",
            ".swal2-popup button.swal2-confirm",
        ),

    ),
    "livemm88.net": SiteProfile(
        "livemm88.net", "https://livemm88.net/nhap-code",
        ("#enter-code-username", "#account-code", "input[name='username']"),
        ("#enter-code-code", "input[name='code']", "input[name='giftcode']",
         "input[autocomplete='one-time-code']", "input[placeholder*='mã code' i]",
         "input[placeholder*='nhập mã' i]", "input[id*='code' i]:not(#enter-code-username)",
         "input[id*='promo' i]"),
        "img[alt='KIỂM TRA'], button[type='submit'], form button",
        result_selectors=("[data-sonner-toast]", "#enter-code-code ~ [role='alert']", "form [role='alert']", ".swal2-container", ".toast", "[class*='result' i]"),
        success_markers=_COMMON_SUCCESS, failure_markers=_COMMON_FAILURE,
        tab_slots=1, form_settle_seconds=0.05, result_timeout_ms=4000,
        result_poll_interval=0.10, selector_fast_window_seconds=0.60,
        full_reload_every=300,
        # Preserve the browser's correlated response while a slower request
        # is still running; decisive results do not wait for this deadline.
        api_response_timeout_ms=10000,
    ),
    "liverr88.net": SiteProfile(
        "liverr88.net", "https://liverr88.net/nhap-code",

        ("#enter-code-username", "input[aria-label='Tên tài khoản']",
         "input[placeholder='Tên tài khoản']", "#username", "#account-code",
         "input[name='username']"),

        ("#enter-code-code", "input[aria-label='Mã code']",
         "input[placeholder='Nhập mã code']", "#code", "input[name='code']"),

        "button[type='submit'], form button, button.code-submit-btn",
        result_selectors=(
            "[data-sonner-toast]", "#enter-code-code ~ [role='alert']",
            ".alert-modal-overlay", ".alert-modal-wc", ".Toastify__toast",
            "form [role='alert']", "form [role='status']", ".swal2-container",
            ".toast", "[class*='result' i]",
        ),
        success_markers=_COMMON_SUCCESS, failure_markers=_COMMON_FAILURE,

        tab_slots=1, form_settle_seconds=0.03, result_timeout_ms=3500,
        result_poll_interval=0.10, selector_fast_window_seconds=0.60,
        full_reload_every=300,
        popup_dismiss_selectors=("button.alert-modal-wc-accept-btn", "button[aria-label='Xác nhận']"),
    ),
    "xx88code.com": SiteProfile(
        "xx88code.com", "https://xx88code.com",
        (
            "input[placeholder='Nhập tài khoản']",
            "input[placeholder='Nhập tên tài khoản']",
            "#account-code",
            "input[name='username']",
        ),
        (
            "input[placeholder='Mã code']",
            "input[placeholder*='mã code' i]",
            "input[autocomplete='one-time-code']",
        ),
        "button[aria-label='Kiểm tra ngay'], button[type='submit'], form button",
        result_selectors=(
            "img[src$='/images/ui-aquarium/btn-accept.png']",
            "img[src$='/images/ui-aquarium/btn-retry.png']",
            "form [role='alert']", "form [role='status']", ".swal2-container",
            ".toast", "[class*='result' i]",
        ),
        success_markers=_COMMON_SUCCESS,
        failure_markers=_COMMON_FAILURE + (
            "code đã sử dụng hết",
            "code đã hết hạn",
            "mã đã được sử dụng",
            "mã đã hết hạn",
            "code used up",
            "code has expired",
        ),

        tab_slots=2, form_settle_seconds=0.01, result_timeout_ms=3500,  # PATCH_MARKER_2026-10-25-v5-xx88-tabs-queue
        result_poll_interval=0.06, selector_fast_window_seconds=0.15,

        full_reload_every=1,
        public_api_endpoint="https://api.xx88code.com/codes/use-code-public",
        api_response_timeout_ms=4500,

        popup_dismiss_selectors=(
            "button[type='button'][aria-label='Xác nhận']:has(img[src*='/images/ui-aquarium/btn-accept.png'])",
            "button[type='button'][aria-label='Thử lại']:has(img[src*='/images/ui-aquarium/btn-retry.png'])",
            "img[src$='/images/ui-aquarium/btn-retry.png']",
            "img[alt='Nút Thử lại']",
            "button[aria-label='Thử lại']",
            "button[aria-label='Xác nhận']",
            "img[src$='/images/ui-aquarium/btn-accept.png']",
            "button[aria-label='Đóng']",
        ),
        popup_dismiss_texts=("thử lại", "đóng", "xác nhận"),
    ),
    "gg88live.tv": SiteProfile(
        "gg88live.tv", "https://gg88live.tv/nhap-code",
        ("#enter-code-username", "input[name='username']", "input[placeholder='Nhập tên người dùng']"),
        ("#enter-code-code", "input[name='code']", "input[placeholder='Nhập mã code']", "input[autocomplete='one-time-code']"),
        "button[aria-label='Kiểm tra'], button[type='submit'], form button",
        result_selectors=("[data-sonner-toast]", "form [role='alert']", "form [role='status']", ".modal-content", ".swal2-container", ".toast", "[class*='result' i]"),
        success_markers=_COMMON_SUCCESS, failure_markers=_COMMON_FAILURE,
        tab_slots=1, form_settle_seconds=0.02, result_timeout_ms=3500,
        result_poll_interval=0.10, selector_fast_window_seconds=0.50,
        full_reload_every=300,

        public_api_endpoint="https://api.gg88code.com/codes/use-code-public",
        api_response_timeout_ms=4500,
        api_capture_phase="submit",
    ),
    "v79code.com": SiteProfile(
        "v79code.com", "https://v79code.com",

        ("input[placeholder='Nhập tài khoản']", "input[autocomplete='username']"),

        ("input[placeholder='Mã code']", "input[autocomplete='one-time-code']",
         "input[placeholder*='mã code' i]"),

        "img[alt='Kiểm tra ngay'], button[aria-label='Kiểm tra ngay'], button[type='submit']",
        result_selectors=("[data-sonner-toast]", "form [role='alert']", "form [role='status']",
                          ".modal-content", ".swal2-container", ".toast", "[class*='result' i]"),
        success_markers=_COMMON_SUCCESS,
        failure_markers=_COMMON_FAILURE + (

            "code đã sử dụng hết",
            "code đã hết hạn",
            "code_not_active",
        ),
        tab_slots=2, form_settle_seconds=0.03, result_timeout_ms=3500,
        result_poll_interval=0.05, selector_fast_window_seconds=0.60,
        full_reload_every=300,

        public_api_endpoint="https://api.v79code.com/codes/use-code-public",
        api_response_timeout_ms=4500,
        api_capture_phase="submit",

        popup_dismiss_selectors=(
            "img[src$='/images/imgvf79/btnXacNhan.png']",
            "img[alt='Xác nhận']",
            "button[aria-label='Xác nhận']",
        ),
        popup_dismiss_texts=("xác nhận",),
    ),
}


@lru_cache(maxsize=64)
def get_site_profile(domain: str) -> SiteProfile:
    host = normalize_domain(domain)
    if host in SITE_PROFILES:
        return SITE_PROFILES[host]
    for profile in SITE_PROFILES.values():
        if normalize_domain(profile.origin) == host:
            return profile
    raise KeyError(host)


def validate_profiles(profiles: Mapping[str, SiteProfile] = SITE_PROFILES) -> None:
    errors = []
    seen_domains: set[str] = set()
    for key, profile in profiles.items():
        try:
            profile.validate()
        except ValueError as exc:
            errors.append(str(exc))
            continue
        normalized_key = normalize_domain(key)
        normalized_domain = normalize_domain(profile.domain)
        if normalized_key != normalized_domain:
            errors.append(f"{key}: mapping key does not match profile domain {profile.domain}")
        if normalized_domain in seen_domains:
            errors.append(f"{key}: duplicate profile domain {normalized_domain}")
        seen_domains.add(normalized_domain)
    if errors:
        raise ValueError(f"SITE_PROFILES invalid: {errors}")


validate_profiles()


def uses_direct_http(domain: str) -> bool:
    from config import Config

    profile = get_site_profile(domain)
    return bool(profile and profile.direct_http_fields
                and profile.domain in Config.DIRECT_HTTP_DOMAINS)


def browser_domains() -> frozenset[str]:
    return frozenset(d for d in SITE_PROFILES if not uses_direct_http(d))


__all__ = [
    "SiteProfile", "SITE_PROFILES", "normalize_domain", "get_site_profile",
    "validate_profiles", "browser_domains", "uses_direct_http",
]
