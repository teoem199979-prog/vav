import logging
import math
import os
from pathlib import Path
from collections.abc import Iterable
from dotenv import load_dotenv
from urllib.parse import urlsplit
from functools import lru_cache
from datetime import datetime, timedelta, timezone

try:
    load_dotenv()
except (OSError, UnicodeError, ValueError) as exc:
    logging.getLogger("autobot." + __name__).warning(
        "Could not load dotenv configuration; using process environment: %s", exc
    )


def get_int_env(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        logging.getLogger("autobot." + __name__).warning(
            "Invalid integer environment value %s=%r; using default %s",
            key, value, default,
        )
        return default


def get_float_env(key: str, default: float) -> float:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    try:
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("value must be finite")
        return result
    except (TypeError, ValueError, OverflowError):
        logging.getLogger("autobot." + __name__).warning(
            "Invalid float environment value %s=%r; using default %s",
            key, value, default,
        )
        return default


def get_positive_int_env(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None or not str(value).strip():
        return default
    try:
        parsed = int(value)
        if parsed <= 0:
            raise ValueError("value must be greater than zero")
        return parsed
    except (TypeError, ValueError, OverflowError):
        logging.getLogger("autobot." + __name__).warning(
            "Invalid positive integer environment value %s=%r; using default %s",
            key, value, default,
        )
        return default


def get_positive_float_env(key: str, default: float) -> float:
    value = os.getenv(key)
    if value is None or not str(value).strip():
        return default
    try:
        parsed = float(value)
        if not math.isfinite(parsed) or parsed <= 0:
            raise ValueError("value must be finite and greater than zero")
        return parsed
    except (TypeError, ValueError, OverflowError):
        logging.getLogger("autobot." + __name__).warning(
            "Invalid positive float environment value %s=%r; using default %s",
            key, value, default,
        )
        return default


def get_nonnegative_float_env(key: str, default: float) -> float:
    value = os.getenv(key)
    if value is None or not str(value).strip():
        return default
    try:
        parsed = float(value)
        if not math.isfinite(parsed) or parsed < 0:
            raise ValueError("value must be finite and nonnegative")
        return parsed
    except (TypeError, ValueError, OverflowError):
        logging.getLogger("autobot." + __name__).warning(
            "Invalid nonnegative float environment value %s=%r; using default %s",
            key, value, default,
        )
        return default


def get_bool_env(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    normalized = str(value).strip().lower()
    if normalized in ("true", "1", "yes", "y", "on"):
        return True
    if normalized in ("false", "0", "no", "n", "off"):
        return False
    logging.getLogger("autobot." + __name__).warning(
        "Invalid boolean environment value %s=%r; using default %s",
        key, value, default,
    )
    return default


getenv_bool = get_bool_env


def get_int_list_env(key: str, default: tuple[int, ...]) -> tuple[int, ...]:

    value = os.getenv(key)
    if value is None or not str(value).strip():
        return tuple(default)
    result = []
    for item in str(value).split(","):
        try:
            result.append(int(item.strip()))
        except (TypeError, ValueError, OverflowError):
            logging.getLogger("autobot." + __name__).warning(
                "Invalid integer list item in %s=%r; ignoring item",
                key, item,
            )
            continue
    return tuple(dict.fromkeys(result)) or tuple(default)


def floored_interval(name: str, requested: float, floor_key: str,
                     floor_default: float, env_keys: tuple) -> float:
    """max(sàn, giá trị yêu cầu), nhưng cảnh báo khi sàn đè giá trị người dùng đã đặt.

    Giữ nguyên kết quả cũ (max của hai giá trị); chỉ thêm cảnh báo để việc xóa dòng
    *_MIN_INTERVAL trong .env không âm thầm kéo nhịp poll về mặc định.
    """
    floor = get_float_env(floor_key, floor_default)
    result = max(floor, requested)
    explicit = any(str(os.getenv(k) or "").strip() for k in env_keys)
    if explicit and requested < floor:
        logging.getLogger("autobot." + __name__).warning(
            "⚠️ %s=%.2fs thấp hơn sàn %s=%.2fs (mặc định %.1fs) → thực tế dùng %.2fs. "
            "Muốn poll nhanh hơn thì đặt %s=%s trong .env.",
            name, requested, floor_key, floor, floor_default, result,
            floor_key, f"{requested:g}",
        )
    return result


# PATCH_MARKER_2026-10-28-config-infra
def _existing_edge_debug_port(user_data_dir: str) -> int:
    lines = (Path(user_data_dir).expanduser() / "DevToolsActivePort").read_text(encoding="utf-8").splitlines()
    if len(lines) < 2 or not lines[1].startswith("/devtools/browser/"):
        raise ValueError("Edge DevToolsActivePort is invalid; enable remote debugging in the requested profile")
    port = int(lines[0])
    if not 1 <= port <= 65535:
        raise ValueError("Edge DevToolsActivePort has an invalid port")
    return port


class Config:

    SOURCE_AWARE_DISPATCH = get_bool_env("SOURCE_AWARE_DISPATCH", True)

    API_ID       = get_int_env("API_ID", 0)
    API_HASH     = os.getenv("API_HASH", "")
    SESSION_NAME = os.getenv("SESSION_NAME", "session_bot")

    ALERT_BOT_TOKEN = os.getenv("ALERT_BOT_TOKEN", "").strip()
    ALERT_CHAT_ID = get_int_env(
        "ALERT_CHAT_ID",
        get_int_env("TELEGRAM_ADMIN_ID", 0),
    )
    ALERT_HTTP_TIMEOUT = get_float_env("ALERT_HTTP_TIMEOUT", 10.0)
    SUBMIT_SUCCESS_NOTIFY = get_bool_env("SUBMIT_SUCCESS_NOTIFY", True)
    SUBMIT_SUCCESS_CHAT_ID = get_int_env(
        "SUBMIT_SUCCESS_CHAT_ID",
        ALERT_CHAT_ID,
    )

    SESSION_AUTO_BACKUP = get_bool_env("SESSION_AUTO_BACKUP", True)
    SESSION_DELETE_AFTER_BACKUP = get_bool_env(
        "SESSION_DELETE_AFTER_BACKUP",
        False,
    )
    SESSION_BACKUP_DIR = os.getenv(
        "SESSION_BACKUP_DIR",
        "backups/sessions",
    ).strip()
    TELEGRAM_AUTO_RECONNECT = get_bool_env(
        "TELEGRAM_AUTO_RECONNECT",
        True,
    )
    TELEGRAM_CONNECTION_RETRIES = get_int_env(
        "TELEGRAM_CONNECTION_RETRIES",
        5,
    )
    TELEGRAM_MAX_RECONNECT_ATTEMPTS = get_int_env(
        "TELEGRAM_MAX_RECONNECT_ATTEMPTS",
        5,
    )

    TELEGRAM_REQUEST_TIMEOUT = get_positive_float_env(
        "TELEGRAM_REQUEST_TIMEOUT",
        20.0,
    )
    TELEGRAM_CONNECT_TIMEOUT = get_positive_float_env(
        "TELEGRAM_CONNECT_TIMEOUT",
        TELEGRAM_REQUEST_TIMEOUT,
    )
    TELEGRAM_HEARTBEAT_TIMEOUT = get_positive_float_env(
        "TELEGRAM_HEARTBEAT_TIMEOUT",
        8.0,
    )
    TELEGRAM_CHANNEL_TIMEOUT = get_positive_float_env(
        "TELEGRAM_CHANNEL_TIMEOUT",
        10.0,
    )

    TELEGRAM_SYNC_DIALOGS = get_bool_env("TELEGRAM_SYNC_DIALOGS", True)
    ALLOW_PARTIAL_CHANNELS = get_bool_env("ALLOW_PARTIAL_CHANNELS", True)

    TELEGRAM_DIALOG_SYNC_TIMEOUT = get_positive_float_env(
        "TELEGRAM_DIALOG_SYNC_TIMEOUT",
        120.0,
    )
    # PATCH_MARKER_2026-10-25-v4-poll-ocr: giới hạn dialog sync lúc startup
    # (trước đây limit=None = toàn bộ dialog). Kênh thiếu vẫn direct-resolve.
    TELEGRAM_DIALOG_SYNC_LIMIT = max(100, get_int_env("TELEGRAM_DIALOG_SYNC_LIMIT", 500))
    TELEGRAM_RECONNECT_BASE_DELAY = get_float_env(
        "TELEGRAM_RECONNECT_BASE_DELAY",
        30.0,
    )
    TELEGRAM_RECONNECT_MAX_DELAY = get_float_env(
        "TELEGRAM_RECONNECT_MAX_DELAY",
        300.0,
    )
    TELEGRAM_RECONNECT_JITTER = get_float_env(
        "TELEGRAM_RECONNECT_JITTER",
        1.0,
    )
    TELEGRAM_STABLE_CONNECTION_SECONDS = get_float_env(
        "TELEGRAM_STABLE_CONNECTION_SECONDS",
        30.0,
    )

    # Session Telethon chịu khoá SQLite (ResilientSQLiteSession trong main_script.py).
    # TELEGRAM_SESSION_RESILIENT=0 -> quay lại session mặc định của Telethon.
    # TELEGRAM_SESSION_WAL=0 -> tắt WAL (dùng khi thư mục bot nằm trên OneDrive/ổ mạng).
    TELEGRAM_SESSION_RESILIENT = get_bool_env("TELEGRAM_SESSION_RESILIENT", True)
    TELEGRAM_SESSION_WAL = get_bool_env("TELEGRAM_SESSION_WAL", True)
    TELEGRAM_SESSION_BUSY_TIMEOUT = get_float_env("TELEGRAM_SESSION_BUSY_TIMEOUT", 1.0)
    TELEGRAM_SESSION_CRITICAL_TIMEOUT = get_float_env("TELEGRAM_SESSION_CRITICAL_TIMEOUT", 10.0)

    LOG_LEVEL    = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE     = os.getenv("LOG_FILE", "logs/bot_activity.log")
    MAX_LOG_SIZE = get_int_env("MAX_LOG_SIZE", 10485760)
    BACKUP_COUNT = get_int_env("BACKUP_COUNT", 5)

    DATABASE_PATH = os.getenv("DATABASE_PATH", "data/code_history.db")

    CODE_MIN_LENGTH = 6
    CODE_MAX_LENGTH = 15

    SPECIAL_CODE_CHARS_30 = r"""~!@#$%^&*()_+{}|:\"<>?`=[]\\;',./»«"""

    CODE_FILTER_GROUPS = {

        "xx8": {
            "description": "Kênh XX8 getcodexx8bonus — OCR video riêng",
            "url_keywords": ["getcodexx8bonus.pages.dev"],
            "allowed_sites": ["getcodexx8bonus"],
            "allow_numeric": False,
            "allow_random_mix": True,
            "require_uppercase": False,
            "force_uppercase": False,
            "remove_spaces": True,
            "prefer_spoiler": True,
            "marker_scan_lines": 3,
            "allow_fallback": False,
            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "min_clean_length": 10,
            "max_clean_length": 10,
        },
        "multi_site_strict": {
            "description": "Dùng chung cho XX88, RR88, GG88",
            "url_keywords": ["xx88", "rr88", "gg88"],
            "allowed_sites": ["xx88", "rr88", "gg88"],
            "allow_numeric": False,
            "allow_random_mix": False,
            "require_uppercase": True,
            "force_uppercase": True,
            "remove_spaces": True,
            "prefer_spoiler": True,
            "marker_scan_lines": 3,
            "allow_fallback": False,
            "special_chars_group": "SPECIAL_CODE_CHARS_30",
            "special_chars": SPECIAL_CODE_CHARS_30,
            "min_special_chars": 0,
            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
            "min_clean_length": 6,
            "max_clean_length": 8,
        },
        "mm88": {
            "description": "Nhóm riêng cho MM88",
            "url_keywords": ["mm88"],
            "allowed_sites": ["mm88"],
            "allow_numeric": False,
            "allow_random_mix": False,
            "require_uppercase": True,
            "force_uppercase": True,
            "remove_spaces": True,
            "prefer_spoiler": True,
            "marker_scan_lines": 3,
            "allow_fallback": False,
            "special_chars_group": "SPECIAL_CODE_CHARS_30",
            "special_chars": SPECIAL_CODE_CHARS_30,
            "min_special_chars": 0,
            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
            "min_clean_length": 6,
            "max_clean_length": 8,
        },
        "v79": {
            "description": "Nhóm riêng cho V79 (v79code.com) — ưu tiên spoiler",
            "url_keywords": ["v79", "v79code"],
            "allowed_sites": ["v79"],
            "allow_numeric": False,
            "allow_random_mix": True,
            "require_uppercase": False,
            "force_uppercase": False,
            "remove_spaces": False,
            "prefer_spoiler": True,
            "marker_scan_lines": 3,
            "allow_fallback": False,
            "special_chars_group": "SPECIAL_CODE_CHARS_30",
            "special_chars": "",
            "min_special_chars": 0,
            "min_entropy": 2.0,
            "uppercase_min_entropy": 2.5,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
        },
        "default": {
            "description": "Fallback nếu URL chưa thuộc nhóm nào",
            "url_keywords": [],
            "allowed_sites": [],
            "allow_numeric": True,
            "allow_random_mix": True,
            "require_uppercase": False,
            "force_uppercase": False,
            "remove_spaces": False,
            "prefer_spoiler": True,
            "min_entropy": 2.3,
            "uppercase_min_entropy": 2.9,
            "soft_blacklist": ["CODE", "GAME", "FREE", "VIP", "NAP", "RUT"],
        },
    }

    KJC_SPECIAL_CHANNEL_IDS = {
        -1002528908352,
        -1003503954906,
    }

    KJC_SPECIAL_MODE = get_bool_env("KJC_SPECIAL_MODE", True)
    KJC_BRAND_MAPPING = {
        "HhLjwN7KZVE3ZGM9": "gg88live.tv",
        "xF7vXI5Qsa0wYTU1": "xx88code.com",
        "wnFJDEObopQ5ZWE1": "livemm88.net",
        "ZXisp_1yVstiOTI1": "liverr88.net",
        "faeWLaGqJxc5Nzk1": "llwincode.com",
    }
    # Legacy Telegram document IDs remain supported for existing posts.
    KJC_CUSTOM_EMOJI_DOMAINS = {
        6285147951467600075: "xx88code.com", 6285183891753932851: "xx88code.com",
        6287281201593980410: "liverr88.net", 6285157975921268644: "liverr88.net",
        6287358794473149914: "livemm88.net", 6287521161416808416: "livemm88.net",
        6210699568179321091: "gg88live.tv", 6212720152723594167: "gg88live.tv",
        6287555735903542738: "llwincode.com", 6289653024268754427: "llwincode.com",
    }
    KJC_STICKER_DOMAINS = {}
    KJC_CODE_SEPARATOR_EMOJI_IDS = frozenset((5215677360774324968, 5798495467834707975))
    LLWIN_CODE_FILTER_GROUP = {
        **CODE_FILTER_GROUPS["multi_site_strict"],
        "description": "LLwin giftcode", "url_keywords": ["llwincode"],
        "allowed_sites": ["llwin"], "max_clean_length": 15,
    }
    CODE_FILTER_GROUPS["llwin"] = LLWIN_CODE_FILTER_GROUP

    OCR_ALLOWED_CHANNEL_IDS = set(
        get_int_list_env(
            "OCR_ALLOWED_CHANNEL_IDS",
            (-1002817093108, -1004395536862, -1004435825431),
        )
    )

    OCR_MEDIA_ONLY_CHANNEL_IDS = get_int_list_env(
        "OCR_MEDIA_ONLY_CHANNEL_IDS",
        (-1002817093108, -1004395536862),
    )

    OCR_MEDIA_FALLBACK_CHANNEL_IDS = get_int_list_env(
        "OCR_MEDIA_FALLBACK_CHANNEL_IDS",
        (-1002817093108, -1004395536862, -1004435825431, -1003933844700),
    )
    OCR_FALLBACK_ALL_MEDIA = get_bool_env("OCR_FALLBACK_ALL_MEDIA", False)


    KJC_BROADCAST_DOMAINS = (
        "xx88code.com",
        "livemm88.net",
        "gg88live.tv",
        "liverr88.net",
    )

    KJC_BROADCAST_URLS = {
        "xx88code.com": "https://xx88code.com",
        "livemm88.net": "https://livemm88.net/nhap-code",
        "gg88live.tv": "https://gg88live.tv/nhap-code",
        "liverr88.net": "https://liverr88.net/nhap-code",
    }

    DOMAIN_ACCOUNT_OVERRIDES = {
        "llwincode.com": [{"username": "dad1", "priority": 1}],
        "getcodexx8bonus.pages.dev": [
            {"username": "kaoboy012", "priority": 1},
            {"username": "conve99sau", "priority": 2},
            {"username": "minichan011", "priority": 3},
        ],
        "xx88code.com": [
            {"username": "dad131", "priority": 1},
            {"username": "hugolan", "priority": 2},
            {"username": "dad123", "priority": 3},
        ],
        "gg88live.tv": [
            {"username": "kaoboy012", "priority": 1},
            {"username": "conve99sau", "priority": 2},

        ],
        "livemm88.net": [
            {"username": "kaoboy012", "priority": 1},
            {"username": "dad131", "priority": 2},
            {"username": "ola12", "priority": 3},

            {"username": "dad123", "priority": 4},
        ],
        "liverr88.net": [
            {"username": "kaoboy012", "priority": 1},
            {"username": "miniichan", "priority": 2},
        ],
        "v79code.com": [
            {"username": "kaoboy012", "priority": 1},
            {"username": "conve99sau", "priority": 2},
        ],
    }

    CHANNEL_CONFIG = {
        -1003873843218: {
            "name": "LLwin PHÁT CODE MIỄN PHÍ", "username": "llwincodefree",
            "url": "https://llwincode.com/", "filter_group": "llwin",
            "priority": 41, "has_video": False, "enabled": True,
            "max_ocr_codes_per_batch": 10, "max_codes_per_message": 10,
            "accounts": [{"username": "dad1", "priority": 1}],
        },
        -1003506071961: {
            "name": "LLwin DỊCH VỤ GIAI NHÂN", "username": "LLWINDICHVUGIAINHAN",
            "url": "https://llwincode.com/", "filter_group": "llwin",
            "priority": 42, "has_video": False, "enabled": True,
            "accounts": [{"username": "dad1", "priority": 1}],
        },
        -1003903971775: {
            "name": "LLwin GIẢI TRÍ 24H", "username": "LLwingiaitri24h",
            "url": "https://llwincode.com/", "filter_group": "llwin",
            "priority": 43, "has_video": False, "enabled": True,
            "accounts": [{"username": "dad1", "priority": 1}],
        },
        -1004395536862: {
            "name": "XX8 - PHÁT CODE MỖI NGÀY",
            "url": "https://getcodexx8bonus.pages.dev/",
            "filter_group": "xx8",
            "priority": 5,
            "has_video": True,
            "ocr_video_enabled": True,

            "ocr_frame_seconds": list(get_int_list_env(
                "XX8_VIDEO_FRAME_SECONDS", (1, 2)
            )),

            # XX8: quét toàn lưới mã, không chỉ ô đầu tiên.
            "ocr_crops": [(0.0, 1.0, 0.0, 1.0)],
            "ocr_single_code_mode": False,
            "ocr_require_consensus": False,
            "max_ocr_codes_per_batch": max(1, get_int_env("XX8_MAX_OCR_CODES_PER_BATCH", 20)),
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
                # PATCH_MARKER_2026-11-01-xx8-rotate: them minichan011 lam
                # tai khoan XX8 thu 3 de xoay vong (user chot 01/11).
                {"username": "minichan011", "priority": 3},
            ],
        },

        -1002528908352: {
            "name": "KJC GÁI XINH",
            "url": "https://xx88code.com",
            "filter_group": "multi_site_strict",
            "priority": 100,
            "has_video": False,
            "enabled": True,
            "accounts": [
                {"username": "dad131", "priority": 1},
                {"username": "hugolan", "priority": 2},
                {"username": "dad123", "priority": 3},
            ],
        },

        -1003503954906: {
            "name": "KJC - ĐỒNG HÀNH THỂ THAO",
            "url": "https://xx88code.com",
            "filter_group": "multi_site_strict",
            "priority": 101,
            "has_video": False,
            "enabled": True,
            "accounts": [
                {"username": "dad131", "priority": 1},
                {"username": "hugolan", "priority": 2},
                {"username": "dad123", "priority": 3},
            ],
        },

        -1002817093108: {
            "name": "PHÁT CODE XX88",
            "url": "https://xx88code.com/",
            "filter_group": "multi_site_strict",
            "priority": 6,
            "has_video": True,

            # Regions are detected from every artwork; no fixed code position.
            "ocr_crops": [None],

            "ocr_frame_seconds": [10, 9, 8, 11, 7],
            "ocr_debug_save_failed_frame": True,
            "ocr_single_code_mode": False,

            "ocr_require_consensus": False,
            "max_ocr_codes_per_batch": 10,
            "accounts": [
                {"username": "dad131", "priority": 1},
                {"username": "hugolan", "priority": 2},
                {"username": "dad123", "priority": 3},
            ],
        },
        -1002768264448: {
            "name": "XX88 THỂ THAO ESPORT",
            "url": "https://xx88code.com/",
            "filter_group": "multi_site_strict",
            "priority": 8,
            "has_video": False,
            "accounts": [
                {"username": "dad131", "priority": 1},
                {"username": "hugolan", "priority": 2},
                {"username": "dad123", "priority": 3},
            ],
        },
        -1002730903277: {
            "name": "XX88 DỊCH VỤ GIAI NHÂN",
            "url": "https://xx88code.com/",
            "filter_group": "multi_site_strict",
            "priority": 9,
            "has_video": False,
            "accounts": [
                {"username": "dad131", "priority": 1},
                {"username": "hugolan", "priority": 2},
                {"username": "dad123", "priority": 3},
            ],
        },
        -1003731231345: {
            "name": "G88 DỊCH VỤ GIAI NHÂN",
            "url": "https://gg88live.tv/nhap-code",
            "filter_group": "multi_site_strict",
            "priority": 10,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
                {"username": "hugolan012", "priority": 3},
            ],
        },
        -1003134541072: {
            "name": "MM88VIP Dịch Vụ Giai Nhân",
            "url": "https://livemm88.net/nhap-code",
            "filter_group": "mm88",
            "priority": 13,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "dad131", "priority": 2},
                {"username": "ola12", "priority": 3},
                {"username": "dad123", "priority": 4},
            ],
        },
        -1002386905514: {
            "name": "RR88 DỊCH VỤ GIAI NHÂN",
            "url": "https://liverr88.net/nhap-code",
            "filter_group": "multi_site_strict",
            "priority": 21,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "miniichan", "priority": 2},
            ],
        },
        -1003936595246: {
            "name": "GÁI 18+",
            "url": "https://livemm88.net/nhap-code",
            "filter_group": "mm88",
            "priority": 31,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "dad131", "priority": 2},
                {"username": "ola12", "priority": 3},
                {"username": "dad123", "priority": 4},
            ],
        },
        -1003939163957: {
            "name": "MM88 GIRL DANCE",
            "url": "https://livemm88.net/nhap-code",
            "filter_group": "mm88",
            "priority": 32,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "dad131", "priority": 2},
                {"username": "ola12", "priority": 3},
                {"username": "dad123", "priority": 4},
            ],
        },
        -1004411105242: {
            "name": "KÈO BÓNG GG88",
            "url": "https://gg88live.tv/nhap-code",
            "filter_group": "multi_site_strict",
            "priority": 36,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
                {"username": "hugolan012", "priority": 3},
            ],
        },
        -1003912975699: {
            "name": "SOI KÈO MM88",
            "url": "https://livemm88.net/nhap-code",
            "filter_group": "mm88",
            "priority": 37,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "dad131", "priority": 2},
                {"username": "ola12", "priority": 3},
                {"username": "dad123", "priority": 4},
            ],
        },
        -1004406362195: {
            "name": "RR88 SOI KÈO",
            "url": "https://liverr88.net/nhap-code",
            "filter_group": "multi_site_strict",
            "priority": 38,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "miniichan", "priority": 2},
                {"username": "hugolan0123", "priority": 3},
            ],
        },
        -1004352437280: {
            "name": "SOI KÈO CÙNG XX88",
            "url": "https://xx88code.com",
            "filter_group": "multi_site_strict",
            "priority": 39,
            "has_video": False,
            "accounts": [
                {"username": "dad131", "priority": 1},
                {"username": "hugolan", "priority": 2},
                {"username": "dad123", "priority": 3},
            ],
        },
        -1004361482833: {
            "name": "V79 PHÁT CODE NỔ HŨ - BẮN CÁ",
            "url": "https://v79code.com/",
            "filter_group": "v79",
            "priority": 40,
            "has_video": False,
            "accounts": [
                {"username": "kaoboy012", "priority": 1},
                {"username": "conve99sau", "priority": 2},
                {"username": "minichan011", "priority": 3},
            ],
        },
    }

    CODE_BLACKLIST = [
        "COM", "HTTP", "HTTPS", "WWW",
        "FACEBOOK", "TELEGRAM",
        "CHECK", "CLIP", "VUI", "BOT",
        "DAILY", "TRUYCAP", "BANCA", "NOHU",
        "ONLINE", "FREE", "CODE", "GIFTCODE",
        "MINIGAME", "GAME", "THETHAO",
        "O8THETHAO", "BONGDA", "TROLL",
    ]

    ENABLE_MONITORING = True
    TELEGRAM_ADMIN_ID = get_int_env("TELEGRAM_ADMIN_ID", 0)
    LLWIN_MANUAL_SLIDER_MODE = get_bool_env("LLWIN_MANUAL_SLIDER_MODE", False)
    SKIP_BRING_TO_FRONT_DOMAINS = tuple(
        domain.strip().lower()
        for domain in os.getenv("SKIP_BRING_TO_FRONT_DOMAINS", "").split(",")
        if domain.strip() and domain.strip().lower() != "llwincode.com"
    )

    SITE_CODE_DEDUP_TTL = get_float_env("SITE_CODE_DEDUP_TTL", 30.0)
    PAGE_NAVIGATION_TIMEOUT = get_positive_int_env("PAGE_NAVIGATION_TIMEOUT", 10000)
    FORM_SETTLE_SECONDS = get_float_env("FORM_SETTLE_SECONDS", 0.03)
    CF_WAIT_SECONDS = get_float_env("CF_WAIT_SECONDS", 1.0)

    CF_POLL_INTERVAL = get_float_env("CF_POLL_INTERVAL", 0.10)
    # Legacy compatibility setting; readiness now honors its full deadline.
    CF_STALL_SECONDS = get_float_env("CF_STALL_SECONDS", 1.2)

    XX88_TURNSTILE_READY_TIMEOUT_SECONDS = get_float_env(
        "XX88_TURNSTILE_READY_TIMEOUT_SECONDS", 15.0
    )
    VERIFICATION_BUTTON_WAIT_SECONDS = get_float_env("VERIFICATION_BUTTON_WAIT_SECONDS", 15.0)
    TURNSTILE_SOLVE_WAIT_SECONDS = get_float_env("TURNSTILE_SOLVE_WAIT_SECONDS", 1.5)

    MANUAL_VERIFICATION_FOREGROUND = get_bool_env("MANUAL_VERIFICATION_FOREGROUND", True)
    MANUAL_VERIFICATION_FOREGROUND_DOMAINS = os.getenv(
        "MANUAL_VERIFICATION_FOREGROUND_DOMAINS",
        "",
    )
    FOREGROUND_ON_VERIFICATION_SUBMIT = get_bool_env("FOREGROUND_ON_VERIFICATION_SUBMIT", True)
    BROWSER_FOREGROUND_ON_SUBMIT = get_bool_env("BROWSER_FOREGROUND_ON_SUBMIT", True)
    PENDING_VERIFY_WATCH_SECONDS = get_float_env("PENDING_VERIFY_WATCH_SECONDS", 180.0)
    PENDING_VERIFY_WORKER_WAIT_SECONDS = get_float_env("PENDING_VERIFY_WORKER_WAIT_SECONDS", 210.0)
    VERIFY_BUTTON_POST_SOLVE_WAIT_SECONDS = get_float_env("VERIFY_BUTTON_POST_SOLVE_WAIT_SECONDS", 4.0)
    # submit-opt: floor 10s -> 5s. The watcher breaks out as soon as the API
    # or DOM yields a decisive result (api_ms ~2.2s), so the floor only ever
    # taxed the no-result tail of every manual verification.
    VERIFICATION_RESULT_POPUP_WAIT_SECONDS = max(5.0, get_float_env("VERIFICATION_RESULT_POPUP_WAIT_SECONDS", 10.0))
    LLWIN_RESULT_POPUP_HOLD_SECONDS = max(0.0, min(3.0, get_float_env("LLWIN_RESULT_POPUP_HOLD_SECONDS", 0.8)))

    TURNSTILE_TOKEN_MAX_AGE_SECONDS = get_float_env("TURNSTILE_TOKEN_MAX_AGE_SECONDS", 240.0)
    SPOILER_FAST_PATH = get_bool_env("SPOILER_FAST_PATH", True)

    FLOW_LATENCY_LOG_ENABLED = get_bool_env("FLOW_LATENCY_LOG_ENABLED", True)
    # Console logging with lightweight counters for /status.
    FLOW_SLOW_WAIT_WARN_MS = get_float_env("FLOW_SLOW_WAIT_WARN_MS", 300.0)
    FLOW_SLOW_BROWSER_WARN_MS = get_float_env("FLOW_SLOW_BROWSER_WARN_MS", 2500.0)
    DOMAIN_QUEUE_SLOW_WARN_MS = get_float_env("DOMAIN_QUEUE_SLOW_WARN_MS", 500.0)
    TELEGRAM_INBOX_IDLE_DRAIN_INTERVAL = get_float_env("TELEGRAM_INBOX_IDLE_DRAIN_INTERVAL", 0.50)
    MAX_CONCURRENT_BROWSERS = max(1, min(2, get_int_env("MAX_CONCURRENT_BROWSERS", 2)))
    # Submission slots are independent of the number of Chrome processes.
    MAX_CONCURRENT_SUBMITS            = get_positive_int_env("MAX_CONCURRENT_SUBMITS", 4)
    MAX_CONCURRENT_SUBMITS_PER_DOMAIN = get_positive_int_env("MAX_CONCURRENT_SUBMITS_PER_DOMAIN", 3)

    SINGLE_ROUND_PER_BATCH            = get_bool_env("SINGLE_ROUND_PER_BATCH", True)

    PENDING_VERIFICATION_RETRY        = get_bool_env("PENDING_VERIFICATION_RETRY", False)
    PENDING_DOMAIN_COOLDOWN_SECONDS   = get_float_env("PENDING_DOMAIN_COOLDOWN_SECONDS", 45.0)
    MAX_RETRIES_PER_ACCOUNT           = get_int_env("MAX_RETRIES_PER_ACCOUNT", 1)

    UNKNOWN_RESULT_GRACE_SECONDS = get_float_env("UNKNOWN_RESULT_GRACE_SECONDS", 0.6)

    XX88_CAPTCHA_RETRIES              = get_int_env("XX88_CAPTCHA_RETRIES", 1)
    RETRY_ON_TIMEOUT                  = get_bool_env("RETRY_ON_TIMEOUT", True)

    SUBMIT_TIMEOUT_SECONDS            = get_float_env("SUBMIT_TIMEOUT_SECONDS", 45.0)
    XX88_SUBMIT_TIMEOUT_SECONDS       = get_float_env("XX88_SUBMIT_TIMEOUT_SECONDS", 15.0)

    RATE_LIMIT_BACKOFF_SECONDS = get_float_env("RATE_LIMIT_BACKOFF_SECONDS", 5.0)
    # Chỉ XX8: business throttle quan sát được; các giá trị là mức thử ban
    # đầu, không phải giới hạn do server công bố.
    XX8_ACCOUNT_MIN_INTERVAL_SECONDS = get_float_env("XX8_ACCOUNT_MIN_INTERVAL_SECONDS", 3.0)
    XX8_DOMAIN_MIN_INTERVAL_SECONDS = get_float_env("XX8_DOMAIN_MIN_INTERVAL_SECONDS", 1.5)
    XX8_RATE_LIMIT_COOLDOWN_SECONDS = get_float_env("XX8_RATE_LIMIT_COOLDOWN_SECONDS", 10.0)
    XX8_RATE_LIMIT_COOLDOWN_MAX_SECONDS = get_float_env("XX8_RATE_LIMIT_COOLDOWN_MAX_SECONDS", 60.0)

    # Thống nhất chính sách pacing cho 8 site. Mặc định 0 = không chèn
    # độ trễ chưa có bằng chứng; chỉ XX8 có nhịp mặc định dựa trên log throttle.
    # Có thể override riêng theo site mà không sửa code hoặc profile.
    SITE_SUBMIT_GAPS = {
        "getcodexx8bonus.pages.dev": (
            XX8_ACCOUNT_MIN_INTERVAL_SECONDS, XX8_DOMAIN_MIN_INTERVAL_SECONDS),
        "xx88code.com": (
            get_float_env("XX88_ACCOUNT_MIN_INTERVAL_SECONDS", 0.0),
            get_float_env("XX88_DOMAIN_MIN_INTERVAL_SECONDS", 0.0)),
        "gg88live.tv": (
            get_float_env("GG88_ACCOUNT_MIN_INTERVAL_SECONDS", 0.0),
            get_float_env("GG88_DOMAIN_MIN_INTERVAL_SECONDS", 0.0)),
        "livemm88.net": (
            get_float_env("MM88_ACCOUNT_MIN_INTERVAL_SECONDS", 0.0),
            get_float_env("MM88_DOMAIN_MIN_INTERVAL_SECONDS", 0.0)),
        "liverr88.net": (
            get_float_env("RR88_ACCOUNT_MIN_INTERVAL_SECONDS", 0.0),
            get_float_env("RR88_DOMAIN_MIN_INTERVAL_SECONDS", 0.0)),
        "v79code.com": (
            get_float_env("V79_ACCOUNT_MIN_INTERVAL_SECONDS", 0.0),
            get_float_env("V79_DOMAIN_MIN_INTERVAL_SECONDS", 0.0)),
    }
    SITE_RATE_LIMIT_COOLDOWNS = {
        "xx88code.com": (
            get_float_env("XX88_RATE_LIMIT_COOLDOWN_SECONDS", 5.0),
            get_float_env("XX88_RATE_LIMIT_COOLDOWN_MAX_SECONDS", 60.0)),
        "gg88live.tv": (
            get_float_env("GG88_RATE_LIMIT_COOLDOWN_SECONDS", 5.0),
            get_float_env("GG88_RATE_LIMIT_COOLDOWN_MAX_SECONDS", 60.0)),
        "livemm88.net": (
            get_float_env("MM88_RATE_LIMIT_COOLDOWN_SECONDS", 5.0),
            get_float_env("MM88_RATE_LIMIT_COOLDOWN_MAX_SECONDS", 60.0)),
        "liverr88.net": (
            get_float_env("RR88_RATE_LIMIT_COOLDOWN_SECONDS", 5.0),
            get_float_env("RR88_RATE_LIMIT_COOLDOWN_MAX_SECONDS", 60.0)),
        "v79code.com": (
            get_float_env("V79_RATE_LIMIT_COOLDOWN_SECONDS", 5.0),
            get_float_env("V79_RATE_LIMIT_COOLDOWN_MAX_SECONDS", 60.0)),
    }
    SITE_RATE_LIMIT_COOLDOWNS["getcodexx8bonus.pages.dev"] = (
        XX8_RATE_LIMIT_COOLDOWN_SECONDS, XX8_RATE_LIMIT_COOLDOWN_MAX_SECONDS)

    REQUESTS_PER_MINUTE = get_int_env("REQUESTS_PER_MINUTE", 60)
    MAX_BURST           = get_int_env("MAX_BURST", 6)

    DOMAIN_RATE_LIMITS = {
        "getcodexx8bonus.pages.dev": (get_int_env("XX8_REQUESTS_PER_MINUTE", REQUESTS_PER_MINUTE), get_int_env("XX8_MAX_BURST", MAX_BURST)),
        "livemm88.net": (get_int_env("MM88_REQUESTS_PER_MINUTE", REQUESTS_PER_MINUTE), get_int_env("MM88_MAX_BURST", MAX_BURST)),
        "liverr88.net": (get_int_env("RR88_REQUESTS_PER_MINUTE", REQUESTS_PER_MINUTE), get_int_env("RR88_MAX_BURST", MAX_BURST)),
        "xx88code.com": (get_int_env("XX88_REQUESTS_PER_MINUTE", REQUESTS_PER_MINUTE), get_int_env("XX88_MAX_BURST", MAX_BURST)),
        "gg88live.tv": (get_int_env("GG88_REQUESTS_PER_MINUTE", REQUESTS_PER_MINUTE), get_int_env("GG88_MAX_BURST", MAX_BURST)),
        "v79code.com": (get_int_env("V79_REQUESTS_PER_MINUTE", REQUESTS_PER_MINUTE), get_int_env("V79_MAX_BURST", MAX_BURST)),
    }

    MESSAGE_QUEUE_MAXSIZE      = get_positive_int_env("MESSAGE_QUEUE_MAXSIZE", 2000)
    MESSAGE_WORKERS            = get_positive_int_env("MESSAGE_WORKERS", 6)
    MAX_CONCURRENT_PROCESSING  = get_positive_int_env("MAX_CONCURRENT_PROCESSING", 16)
    # Download ahead while native OCR remains capped by MAX_CONCURRENT_OCR.
    OCR_MEDIA_WORKERS          = get_int_env("OCR_MEDIA_WORKERS", 4)
    OCR_MEDIA_QUEUE_MAXSIZE    = get_int_env("OCR_MEDIA_QUEUE_MAXSIZE", 32)

    XX88_OCR_CODE_LENGTH       = get_int_env("XX88_OCR_CODE_LENGTH", 6)
    XX88_OCR_MIN_CODES_PER_FRAME = get_int_env("XX88_OCR_MIN_CODES_PER_FRAME", 2)

    HEARTBEAT_INTERVAL            = get_float_env("HEARTBEAT_INTERVAL", 300.0)
    TELEGRAM_CATCH_UP             = get_bool_env("TELEGRAM_CATCH_UP", False)

    INGRESS_FIX_V2 = getenv_bool("INGRESS_FIX_V2", False)
    # Sàn nhịp poll: v2 mặc định 2s (trước là 10s cứng, làm POLL_INTERVAL=3/4 trong
    POLL_MIN_INTERVAL = get_float_env("POLL_MIN_INTERVAL", 2.0 if INGRESS_FIX_V2 else 10.0)
    POLL_INTERVAL = (
        min(15.0, floored_interval(
            "POLL_INTERVAL",
            get_float_env("POLL_INTERVAL", get_float_env("CHANNEL_POLL_INTERVAL", 12.0)),
            "POLL_MIN_INTERVAL", 2.0,
            ("POLL_INTERVAL", "CHANNEL_POLL_INTERVAL"),
        ))
        if INGRESS_FIX_V2 else min(
            15.0,
            floored_interval(
                "POLL_INTERVAL",
                get_float_env("POLL_INTERVAL", get_float_env("CHANNEL_POLL_INTERVAL", 12.0)),
                "POLL_MIN_INTERVAL", 10.0,
                ("POLL_INTERVAL", "CHANNEL_POLL_INTERVAL"),
            ),
        )
    )
    CHANNEL_POLL_INTERVAL = POLL_INTERVAL
    TELEGRAM_POLL_FALLBACK_ENABLED = get_bool_env("TELEGRAM_POLL_FALLBACK_ENABLED", True)

    CHANNEL_POLL_REQUEST_TIMEOUT  = get_float_env("CHANNEL_POLL_REQUEST_TIMEOUT", 5.0)

    CHANNEL_POLL_FETCH_TIMEOUT    = get_float_env("CHANNEL_POLL_FETCH_TIMEOUT", 8.0)

    POLL_STALL_RECONNECT_SECONDS    = get_float_env("POLL_STALL_RECONNECT_SECONDS", 45.0)
    POLL_RECONNECT_COOLDOWN_SECONDS = get_float_env("POLL_RECONNECT_COOLDOWN_SECONDS", 120.0)

    CHANNEL_POLL_MAX_INTERVAL = max(CHANNEL_POLL_INTERVAL, get_float_env("CHANNEL_POLL_MAX_INTERVAL", 60.0))
    CHANNEL_POLL_FLOOD_DECAY_AFTER = get_int_env("CHANNEL_POLL_FLOOD_DECAY_AFTER", 10)
    # v4.9.2: hệ số hạ nhịp poll sau FloodWait (0.50–0.99). Nhỏ hơn = hồi nhanh hơn.
    CHANNEL_POLL_FLOOD_DECAY_FACTOR = get_float_env("CHANNEL_POLL_FLOOD_DECAY_FACTOR", 0.90)
    # v4.9.2: thời gian nghỉ trước khi thử lại mã bị hoãn (TOKEN_NOT_READY / tài khoản bận).
    DISPATCH_RETRY_DELAY_SECONDS = get_float_env("DISPATCH_RETRY_DELAY_SECONDS", 0.35)

    # false: tắt hẳn vòng hot, chỉ chạy một vòng poll chính cho tất cả kênh
    # (giảm số lần gọi GetPeerDialogs/phút => ít FloodWait hơn).
    TELEGRAM_DIALOG_BATCH_SIZE = min(10, max(1, get_int_env("TELEGRAM_DIALOG_BATCH_SIZE", 8)))
    TELEGRAM_DIALOG_BATCH_RETRIES = min(3, max(1, get_int_env("TELEGRAM_DIALOG_BATCH_RETRIES", 3)))
    TELEGRAM_DIALOG_RETRY_JITTER_SEC = max(0.0, get_float_env("TELEGRAM_DIALOG_RETRY_JITTER_SEC", 1.0))
    TELEGRAM_INGRESS_DEDUPE_ENABLED = get_bool_env("TELEGRAM_INGRESS_DEDUPE_ENABLED", True)
    TELEGRAM_INGRESS_DEDUPE_TTL_SEC = max(1.0, get_float_env("TELEGRAM_INGRESS_DEDUPE_TTL_SEC", 600.0))
    TELEGRAM_INGRESS_AGE_DROP_ENABLED = get_bool_env("TELEGRAM_INGRESS_AGE_DROP_ENABLED", True)
    INGRESS_MAX_AGE_SEC = get_positive_float_env("INGRESS_MAX_AGE_SEC", 120.0)
    INGRESS_XX88_MAX_AGE_SEC = get_positive_float_env("INGRESS_XX88_MAX_AGE_SEC", 40.0)
    INGRESS_OCR_MAX_AGE_SEC = get_positive_float_env("INGRESS_OCR_MAX_AGE_SEC", 45.0)
    TELEGRAM_INBOX_RECOVERY_MODE  = os.getenv(
        "TELEGRAM_INBOX_RECOVERY_MODE",
        "live_only",
    ).strip().lower()
    if TELEGRAM_INBOX_RECOVERY_MODE not in {"at_least_once", "live_only"}:
        logging.getLogger("autobot." + __name__).warning(
            "⚠️ TELEGRAM_INBOX_RECOVERY_MODE=%r không hợp lệ; dùng live_only",
            TELEGRAM_INBOX_RECOVERY_MODE,
        )
        TELEGRAM_INBOX_RECOVERY_MODE = "live_only"

    TELEGRAM_FILTER_AT_SOURCE     = get_bool_env("TELEGRAM_FILTER_AT_SOURCE", True)

    TELEGRAM_PUSH_UNFILTERED      = get_bool_env("TELEGRAM_PUSH_UNFILTERED", True)

    TELEGRAM_RAW_DIAGNOSTICS      = get_bool_env("TELEGRAM_RAW_DIAGNOSTICS", True)
    TELEGRAM_PUSH_HEALTH_ENABLED = get_bool_env("TELEGRAM_PUSH_HEALTH_ENABLED", True)
    TELEGRAM_PUSH_STALL_SECONDS = max(60.0, get_float_env("TELEGRAM_PUSH_STALL_SECONDS", 120.0))
    TELEGRAM_PUSH_RECOVERY_COOLDOWN = max(300.0, get_float_env("TELEGRAM_PUSH_RECOVERY_COOLDOWN", 300.0))

    DOMAIN_QUEUE_MAXSIZE = get_positive_int_env("DOMAIN_QUEUE_MAXSIZE", 500)
    TELEGRAM_INBOX_DB_PATH = os.getenv("TELEGRAM_INBOX_DB_PATH", "data/telegram_inbox.db")
    TELEGRAM_INBOX_LEASE_SECONDS = get_positive_int_env("TELEGRAM_INBOX_LEASE_SECONDS", 300)
    TELEGRAM_INBOX_DRAIN_BATCH = get_positive_int_env("TELEGRAM_INBOX_DRAIN_BATCH", 250)

    TELEGRAM_INBOX_DRAIN_INTERVAL = get_positive_float_env("TELEGRAM_INBOX_DRAIN_INTERVAL", 0.05)
    TELEGRAM_INBOX_ENQUEUE_RETRIES = get_positive_int_env("TELEGRAM_INBOX_ENQUEUE_RETRIES", 3)
    TELEGRAM_INGRESS_MAXSIZE = max(1, get_int_env("TELEGRAM_INGRESS_MAXSIZE", 4096))
    # PATCH_MARKER_2026-10-26-v6-async-ingest: additive settings only.
    INGEST_QUEUE_MAXSIZE = max(1, get_int_env("INGEST_QUEUE_MAXSIZE", 512))
    OCR_MAX_CONCURRENCY = max(1, get_int_env("OCR_MAX_CONCURRENCY", max(2, (os.cpu_count() or 1) // 2)))
    XX88_TURNSTILE_READY_TIMEOUT_MS = max(0, get_int_env("XX88_TURNSTILE_READY_TIMEOUT_MS", 2000))
    TURNSTILE_WIDGET_SELECTOR = os.getenv(
        "TURNSTILE_WIDGET_SELECTOR",
        '.cf-turnstile, iframe[src*="challenges.cloudflare.com"], '
        'input[name="cf-turnstile-response"], textarea[name="cf-turnstile-response"]',
    )
    SHUTDOWN_MESSAGE_DRAIN_TIMEOUT = get_float_env("SHUTDOWN_MESSAGE_DRAIN_TIMEOUT", 8.0)

    MAX_INBOX_ATTEMPTS = max(3, get_int_env("MAX_INBOX_ATTEMPTS", 3))
    INBOX_RETRY_BASE_DELAY = get_float_env("INBOX_RETRY_BASE_DELAY", 2.0)
    INBOX_RETRY_MAX_DELAY = get_float_env("INBOX_RETRY_MAX_DELAY", 120.0)

    INBOX_MAX_PENDING_AGE_SECONDS = get_float_env("INBOX_MAX_PENDING_AGE_SECONDS", 900.0)
    INBOX_PENDING_SWEEP_INTERVAL_SECONDS = get_float_env("INBOX_PENDING_SWEEP_INTERVAL_SECONDS", 300.0)

    CODE_MAX_AGE_SECONDS = get_nonnegative_float_env("CODE_MAX_AGE_SECONDS", 120.0)
    # Tuổi tối đa của mã khi tới lượt nhập, theo từng domain (giây). Mã chờ hàng đợi
    # quá lâu thường đã hết lượt/hết hạn phía site; nhập nó chỉ tốn hạn mức mỗi phút
    # của mã mới. Domain không khai báo dùng CODE_MAX_AGE_SECONDS. Đặt 0 = không giới hạn.
    # (Viết dict literal thay vì comprehension: trong thân class, comprehension
    # không nhìn thấy biến CODE_MAX_AGE_SECONDS.)
    SITE_CODE_MAX_AGE_SECONDS = {
        "getcodexx8bonus.pages.dev": get_nonnegative_float_env("XX8_CODE_MAX_AGE_SECONDS", CODE_MAX_AGE_SECONDS),
        "livemm88.net": get_nonnegative_float_env("MM88_CODE_MAX_AGE_SECONDS", CODE_MAX_AGE_SECONDS),
        "liverr88.net": get_nonnegative_float_env("RR88_CODE_MAX_AGE_SECONDS", CODE_MAX_AGE_SECONDS),
        # Turnstile is retried asynchronously; allow queued codes to survive
        # verification delays while each submit attempt remains time-bounded.
        "xx88code.com": get_nonnegative_float_env("XX88_CODE_MAX_AGE_SECONDS", 120.0),
        "gg88live.tv": get_nonnegative_float_env("GG88_CODE_MAX_AGE_SECONDS", CODE_MAX_AGE_SECONDS),
        "v79code.com": get_nonnegative_float_env("V79_CODE_MAX_AGE_SECONDS", CODE_MAX_AGE_SECONDS),
    }

    # ── Lập lịch theo độ tươi + pull-when-ready (patch scheduling) ──────────────
    # Danh sách domain, phân tách bằng dấu phẩy; để trống = tắt tính năng đó.
    # Hàng đợi miền lấy mã TƯƠI NHẤT trước (thay cho FIFO).
    FRESHEST_FIRST_DOMAINS = tuple(
        d.strip().lower() for d in os.getenv("FRESHEST_FIRST_DOMAINS", "xx88code.com").split(",") if d.strip()
    )
    # Worker không lấy mã khi mọi tab đang chờ xác minh; chờ sự kiện "tab sẵn sàng".
    VERIFICATION_GATE_DOMAINS = tuple(
        d.strip().lower() for d in os.getenv("VERIFICATION_GATE_DOMAINS", "xx88code.com").split(",") if d.strip()
    )
    VERIFICATION_GATED_DEFER_SECONDS = get_float_env("VERIFICATION_GATED_DEFER_SECONDS", 0.3)
    # Ngân sách defer cho BROWSER_VERIFICATION_WAIT / TAB_UNAVAILABLE (thay cho defer vô hạn).
    DEFER_BUDGET_DOMAINS = tuple(
        d.strip().lower() for d in os.getenv("DEFER_BUDGET_DOMAINS", "xx88code.com").split(",") if d.strip()
    )
    DEFER_BUDGET_MAX_CYCLES = get_int_env("DEFER_BUDGET_MAX_CYCLES", 4)
    DEFER_BUDGET_MAX_SECONDS = get_float_env("DEFER_BUDGET_MAX_SECONDS", 30.0)
    # PATCH_MARKER_2026-10-25-v6-xx88-spoiler-nolock: XX88 spoiler SUCCESS
    # khong khoa account hom nay (ma spoiler khong gioi han luot nhap).
    # OCR/text giu nguyen: SUCCESS van khoa account hom nay.
    # Dat false trong .env de ve hanh vi cu (khoa moi SUCCESS).
    XX88_SPOILER_NO_ACCOUNT_LOCK = get_bool_env("XX88_SPOILER_NO_ACCOUNT_LOCK", True)

    INPUT_CACHE_CLEANUP_INTERVAL = get_float_env("INPUT_CACHE_CLEANUP_INTERVAL", 30.0)

    OCR_CONFIDENCE_THRESHOLD = get_float_env("OCR_CONFIDENCE_THRESHOLD", 0.70)

    OCR_CROP_FALLBACK = get_bool_env("OCR_CROP_FALLBACK", False)

    OCR_MAX_IMAGE_SIDE = get_int_env("OCR_MAX_IMAGE_SIDE", 1280)
    OCR_MIN_CROP_WIDTH = get_int_env("OCR_MIN_CROP_WIDTH", 300)
    OCR_VIDEO_SAMPLE_SECONDS = max(0.15, get_float_env("OCR_VIDEO_SAMPLE_SECONDS", 0.40))
    OCR_VIDEO_MAX_SAMPLES = max(1, min(10, get_int_env("OCR_VIDEO_MAX_SAMPLES", 10)))
    OCR_VIDEO_MAX_DURATION_SEC = max(
        0.1, min(7.0, get_float_env("OCR_VIDEO_MAX_DURATION_SEC", 7.0))
    )
    XX88_VIDEO_MAX_DURATION_SEC = 12.0
    XX88_VIDEO_MAX_SAMPLES = 20
    XX8_VIDEO_MAX_DURATION_SEC = max(0.1, get_float_env("XX8_VIDEO_MAX_DURATION_SEC", 12.0))
    XX8_VIDEO_MAX_SAMPLES = max(1, get_int_env("XX8_VIDEO_MAX_SAMPLES", 20))
    LLWIN_VIDEO_MAX_DURATION_SEC = 12.0
    LLWIN_VIDEO_MAX_SAMPLES = 20
    OCR_VIDEO_FILE_CACHE_DIR = os.getenv("OCR_VIDEO_FILE_CACHE_DIR", "data/ocr_video_cache")
    # PATCH_MARKER_2026-10-26-streaming-ctc: additive settings for XX8 video.
    OCR_STREAMING_ENABLED = get_bool_env("OCR_STREAMING_ENABLED", True)
    OCR_STREAMING_SAMPLE_SECONDS = max(0.15, get_float_env("OCR_STREAMING_SAMPLE_SECONDS", 0.30))
    OCR_STREAMING_WINDOW = max(2, min(32, get_int_env("OCR_STREAMING_WINDOW", 12)))
    OCR_STREAMING_MARGIN = max(0.70, min(1.0, get_float_env("OCR_STREAMING_MARGIN", 0.90)))
    # Dừng quét video sớm: sau khi đã có vùng mã ổn định, nếu liên tiếp N khung mẫu không có
    # vùng mã mới / không có vùng đang hiện dở thì thôi quét phần còn lại. 0 = tắt (mặc định).
    # Chọn N lớn hơn metrics max_quiet_gap trong dòng [OCR-DYNAMIC] (đo trước khi bật).
    OCR_VIDEO_EARLY_STOP_SAMPLES = get_int_env("OCR_VIDEO_EARLY_STOP_SAMPLES", 0)
    OCR_VIDEO_EARLY_STOP_DOMAINS = tuple(
        d.strip().lower() for d in os.getenv("OCR_VIDEO_EARLY_STOP_DOMAINS", "xx88code.com").split(",") if d.strip()
    )
    # Phát mã sớm: khi một vùng mã có hai khung độc lập đọc ra cùng một mã thì gọi on_code ngay
    # (extract_site_codes(..., on_code=...)) và quét tiếp, thay vì gửi cả lô sau khi quét hết khung.
    # Không có tác dụng nếu người gọi không truyền on_code. OCR_VIDEO_EARLY_EMIT_ENABLED=false để tắt nhanh.
    OCR_VIDEO_EARLY_EMIT_ENABLED = get_bool_env("OCR_VIDEO_EARLY_EMIT_ENABLED", True)
    # Retained for configuration compatibility; strict early emission always
    # requires two consecutive samples regardless of this legacy setting.
    OCR_VIDEO_FIRST_FRAME_EMIT = get_bool_env("OCR_VIDEO_FIRST_FRAME_EMIT", True)
    # Số worker xử lý song song các khung mẫu trong _collect_video khi
    # coarse=1 (bước lấy mẫu cố định nên vị trí khung không phụ thuộc
    # khung trước). 1 = tuần tự như cũ. Detector toàn ảnh ở fallback vẫn
    # chạy tuần tự vì RapidOCR detector dùng chung không an toàn luồng.
    OCR_VIDEO_PANEL_WORKERS = max(1, min(8, get_int_env("OCR_VIDEO_PANEL_WORKERS", 4)))
    OCR_VIDEO_EARLY_EMIT_DOMAINS = tuple(
        d.strip().lower() for d in os.getenv("OCR_VIDEO_EARLY_EMIT_DOMAINS", "xx88code.com,llwincode.com").split(",") if d.strip()
    )
    # Lấy mẫu thưa-rồi-dày: khung không có vùng mã thì nhảy xa gấp N lần bước thường (1 = tắt, mặc định);
    # thấy vùng mã thì về bước thường. Chỉ bật sau khi đã có số liệu từ dòng [OCR-DYNAMIC]
    # (mã hiện ngắn hơn N*bước có thể bị lỡ). Đổi thang đo của max_quiet_gap => đo lại ngưỡng dừng sớm.
    OCR_VIDEO_COARSE_FACTOR = max(1, min(4, get_int_env("OCR_VIDEO_COARSE_FACTOR", 1)))
    OCR_VIDEO_COARSE_DOMAINS = tuple(
        d.strip().lower() for d in os.getenv("OCR_VIDEO_COARSE_DOMAINS", "xx88code.com").split(",") if d.strip()
    )
    OCR_RECOGNITION_BATCH_SIZE = max(1, min(16, get_int_env("OCR_RECOGNITION_BATCH_SIZE", 8)))

    MAX_CONCURRENT_OCR = max(1, min(2, get_int_env("MAX_CONCURRENT_OCR", 2)))
    VIDEO_OCR_MAX_FRAMES = get_int_env("VIDEO_OCR_MAX_FRAMES", 3)

    XX8_OCR_EXTRA_FRAME_SECONDS = get_int_list_env("XX8_OCR_EXTRA_FRAME_SECONDS", (3, 5, 8))
    XX8_OCR_LATE_FRAME_LIMIT = max(1, min(4, get_int_env("XX8_OCR_LATE_FRAME_LIMIT", 4)))
    XX8_OCR_ALWAYS_GRID = get_bool_env("XX8_OCR_ALWAYS_GRID", True)
    XX8_OCR_GRID_MIN_CODES = max(1, get_int_env("XX8_OCR_GRID_MIN_CODES", 2))
    XX8_OCR_GRID_FRAME_LIMIT = max(1, min(5, get_int_env("XX8_OCR_GRID_FRAME_LIMIT", 3)))
    XX8_OCR_CODE_LENGTH = 10
    XX8_OCR_CODE_LENGTHS = (10,)
    XX8_OCR_PANEL_SIDE = max(320, min(960, get_int_env("XX8_OCR_PANEL_SIDE", 640)))
    XX8_OCR_MIN_LENGTH = 10
    XX8_OCR_MAX_LENGTH = 10

    MAX_CONCURRENT_MEDIA_DOWNLOADS = get_int_env("MAX_CONCURRENT_MEDIA_DOWNLOADS", 4)
    FAST_TELEGRAM_DOWNLOAD = get_bool_env("FAST_TELEGRAM_DOWNLOAD", False)
    FAST_DOWNLOAD_MIN_BYTES = get_int_env("FAST_DOWNLOAD_MIN_BYTES", 10 * 1024 * 1024)
    XX8_FAST_DOWNLOAD_MIN_BYTES = 8 * 1024 * 1024
    FAST_DOWNLOAD_WORKERS = get_int_env("FAST_DOWNLOAD_WORKERS", 6)
    FAST_DOWNLOAD_CHUNK_KB = get_int_env("FAST_DOWNLOAD_CHUNK_KB", 512)
    FAST_DOWNLOAD_CHUNK_TIMEOUT_SECONDS = get_positive_float_env(
        "FAST_DOWNLOAD_CHUNK_TIMEOUT_SECONDS", 40.0
    )
    MEDIA_DC_SETUP_TIMEOUT_SECONDS = get_positive_float_env("MEDIA_DC_SETUP_TIMEOUT_SECONDS", 20.0)
    MEDIA_DC_MAX_PARALLEL_REQUESTS = max(1, min(4, get_int_env("MEDIA_DC_MAX_PARALLEL_REQUESTS", 2)))
    MEDIA_DC_ROUTE_STATE_PATH = os.getenv("MEDIA_DC_ROUTE_STATE_PATH", "data/media_dc_routes.json")

    XX8_FAST_TELEGRAM_DOWNLOAD = get_bool_env("XX8_FAST_TELEGRAM_DOWNLOAD", True)
    XX8_FAST_DOWNLOAD_WORKERS = max(2, min(8, get_int_env("XX8_FAST_DOWNLOAD_WORKERS", 8)))

    OCR_DET_LIMIT_SIDE = get_int_env("OCR_DET_LIMIT_SIDE", 640)
    OCR_FAST_PATH = get_bool_env("OCR_FAST_PATH", True)
    OCR_FAST_MIN_CHARS = get_int_env("OCR_FAST_MIN_CHARS", 8)
    OCR_FAST_VARIANTS = get_int_env("OCR_FAST_VARIANTS", 1)
    OCR_SKIP_CLS = get_bool_env("OCR_SKIP_CLS", True)

    OCR_CACHE_TTL_SECONDS = get_float_env("OCR_CACHE_TTL_SECONDS", 600.0)
    OCR_CACHE_MAX_ENTRIES = get_int_env("OCR_CACHE_MAX_ENTRIES", 128)
    OCR_VIDEO_FILE_CACHE_TTL_SECONDS = get_positive_float_env(
        "OCR_VIDEO_FILE_CACHE_TTL_SECONDS", 7 * 24 * 60 * 60
    )
    OCR_VIDEO_FILE_CACHE_MAX_ENTRIES = get_positive_int_env(
        "OCR_VIDEO_FILE_CACHE_MAX_ENTRIES", 128
    )
    OCR_VIDEO_FILE_CACHE_MAX_BYTES = get_positive_int_env(
        "OCR_VIDEO_FILE_CACHE_MAX_BYTES", 1024 * 1024 * 1024
    )
    OCR_VIDEO_PARTIAL_TTL_SECONDS = get_positive_float_env(
        "OCR_VIDEO_PARTIAL_TTL_SECONDS", 6 * 3600
    )
    OCR_DEBUG_SAVE_FRAMES = get_bool_env("OCR_DEBUG_SAVE_FRAMES", False)
    OCR_DEBUG_TIMING = get_bool_env("OCR_DEBUG_TIMING", False)
    MEDIA_DOWNLOAD_RETRIES = get_int_env("MEDIA_DOWNLOAD_RETRIES", 1)
    MEDIA_DOWNLOAD_RETRY_DELAY = get_float_env("MEDIA_DOWNLOAD_RETRY_DELAY", 0.5)
    MEDIA_DOWNLOAD_MAX_SIZE_MB = get_int_env("MEDIA_DOWNLOAD_MAX_SIZE_MB", 200)
    # Trần timeout tải media (giây). Thực tế được kẹp thêm theo max_age của domain.
    MEDIA_DOWNLOAD_TIMEOUT_SECONDS = get_float_env("MEDIA_DOWNLOAD_TIMEOUT_SECONDS", 180.0)

    MEDIA_CLEANUP_INTERVAL_SECONDS = get_int_env(
        "MEDIA_CLEANUP_INTERVAL_SECONDS", 3 * 24 * 60 * 60
    )
    OCR_TEMP_DIR_MAX_AGE_SECONDS = get_int_env(
        "OCR_TEMP_DIR_MAX_AGE_SECONDS", 3 * 24 * 60 * 60
    )
    MAX_OCR_CODES_PER_BATCH = get_int_env("MAX_OCR_CODES_PER_BATCH", 4)
    PAUSE_OCR_ON_HIGH_CPU = get_bool_env("PAUSE_OCR_ON_HIGH_CPU", True)
    OCR_CPU_THRESHOLD = get_int_env("OCR_CPU_THRESHOLD", 85)
    OCR_RAM_PAUSE_THRESHOLD = get_int_env("OCR_RAM_PAUSE_THRESHOLD", 90)
    OCR_RAM_MIN_AVAILABLE_MB = max(256, get_int_env("OCR_RAM_MIN_AVAILABLE_MB", 768))
    OCR_RAM_WARN_THRESHOLD = get_int_env("OCR_RAM_WARN_THRESHOLD", 85)
    HEALTH_CHECK_INTERVAL = get_float_env("HEALTH_CHECK_INTERVAL", 30.0)
    HEALTH_BREACH_CONFIRM_COUNT = get_int_env("HEALTH_BREACH_CONFIRM_COUNT", 2)
    QUEUE_SOFT_LIMIT_PCT = get_float_env("QUEUE_SOFT_LIMIT_PCT", 0.75)
    QUEUE_HARD_LIMIT_PCT = get_float_env("QUEUE_HARD_LIMIT_PCT", 0.95)
    QUEUE_CLEANUP_ON_SOFT_LIMIT = get_bool_env("QUEUE_CLEANUP_ON_SOFT_LIMIT", False)
    QUEUE_CLEANUP_TARGET_PCT = get_float_env("QUEUE_CLEANUP_TARGET_PCT", 0.50)
    QUEUE_CHECK_INTERVAL = get_float_env("QUEUE_CHECK_INTERVAL", 5.0)

    LOG_ROTATION_MAX_BYTES    = get_int_env("LOG_ROTATION_MAX_BYTES", 10_485_760)
    LOG_ROTATION_BACKUP_COUNT = get_int_env("LOG_ROTATION_BACKUP_COUNT", 3)

    # Only explicitly supported public, token-free APIs can use this transport.
    # Set DIRECT_HTTP_DOMAINS= to return every site to its browser workflow.
    DIRECT_HTTP_DOMAINS = frozenset(
        d.strip().lower() for d in os.getenv(
            "DIRECT_HTTP_DOMAINS", "getcodexx8bonus.pages.dev"
        ).split(",") if d.strip()
    )
    DIRECT_HTTP_TIMEOUT_SECONDS = max(1.0, get_float_env("DIRECT_HTTP_TIMEOUT_SECONDS", 12.0))
    DIRECT_HTTP_CONNECT_TIMEOUT_SECONDS = max(0.1, get_float_env("DIRECT_HTTP_CONNECT_TIMEOUT_SECONDS", 3.0))
    DIRECT_HTTP_MAX_CONNECTIONS = max(1, get_int_env("DIRECT_HTTP_MAX_CONNECTIONS", 6))
    BROWSER_ONLY = not bool(DIRECT_HTTP_DOMAINS)
    BROWSER_CIRCUIT_FAILURE_THRESHOLD = get_int_env("BROWSER_CIRCUIT_FAILURE_THRESHOLD", 3)
    BROWSER_CIRCUIT_COOLDOWN_SECONDS = get_float_env("BROWSER_CIRCUIT_COOLDOWN_SECONDS", 60.0)

    BROWSER_TYPE = "edge"
    BROWSER_NAME = "Microsoft Edge"
    BROWSER_EXE_NAME = "msedge.exe"
    EDGE_PATH = os.getenv("EDGE_PATH", os.getenv("EDGE_EXE", "")).strip()
    EDGE_AUTO_LAUNCH = get_bool_env("EDGE_AUTO_LAUNCH", True)
    EDGE_LAUNCH_TIMEOUT = max(0.5, get_float_env("EDGE_LAUNCH_TIMEOUT", 30.0))
    EDGE_KILL_ON_SHUTDOWN = get_bool_env("EDGE_KILL_ON_SHUTDOWN", True)
    EDGE_PROFILE_1 = os.getenv("EDGE_PROFILE_1", "edge_bot_profile_9222")
    EDGE_PROFILE_2 = os.getenv("EDGE_PROFILE_2", "edge_bot_profile_9223")
    EDGE_CDP_HOST = os.getenv("EDGE_CDP_HOST", "127.0.0.1").strip() or "127.0.0.1"
    EDGE_CDP_PORT = get_int_env("EDGE_CDP_PORT", 9222)
    EDGE_ATTACH_USER_DATA_DIR = os.getenv("EDGE_ATTACH_USER_DATA_DIR", "").strip()
    if EDGE_ATTACH_USER_DATA_DIR:
        EDGE_CDP_PORT = _existing_edge_debug_port(EDGE_ATTACH_USER_DATA_DIR)
        EDGE_AUTO_LAUNCH = False
        EDGE_KILL_ON_SHUTDOWN = False
    EDGE_CDP_CONNECT_TIMEOUT_SECONDS = get_float_env("EDGE_CDP_CONNECT_TIMEOUT_SECONDS", 45.0)
    EDGE_CDP_RECONNECT_COOLDOWN_SECONDS = get_float_env("EDGE_CDP_RECONNECT_COOLDOWN_SECONDS", 5.0)

    EDGE2_ENABLED = get_bool_env("EDGE2_ENABLED", False)
    EDGE2_CDP_HOST = os.getenv("EDGE2_CDP_HOST", "127.0.0.1").strip() or "127.0.0.1"
    EDGE2_CDP_PORT = get_int_env("EDGE2_CDP_PORT", 9223)
    EDGE2_DOMAINS = os.getenv(
        "EDGE2_DOMAINS",
        "",
    )
    EDGE2_AUTO_OPEN_TABS = get_bool_env("EDGE2_AUTO_OPEN_TABS", False)

    # Compatibility names for the existing CDP/TabPool API. These all target Edge;
    # old CHROME_* environment settings cannot select a Chrome profile by mistake.
    CHROME_PATH = EDGE_PATH
    CHROME_AUTO_LAUNCH = EDGE_AUTO_LAUNCH
    CHROME_LAUNCH_TIMEOUT = EDGE_LAUNCH_TIMEOUT
    CHROME_KILL_ON_SHUTDOWN = EDGE_KILL_ON_SHUTDOWN
    CHROME_PROFILE_1 = EDGE_PROFILE_1
    CHROME_PROFILE_2 = EDGE_PROFILE_2
    CHROME_CDP_HOST = EDGE_CDP_HOST
    CHROME_CDP_PORT = EDGE_CDP_PORT
    CHROME_CDP_CONNECT_TIMEOUT_SECONDS = EDGE_CDP_CONNECT_TIMEOUT_SECONDS
    CHROME_CDP_RECONNECT_COOLDOWN_SECONDS = EDGE_CDP_RECONNECT_COOLDOWN_SECONDS
    CHROME2_ENABLED = EDGE2_ENABLED
    CHROME2_CDP_HOST = EDGE2_CDP_HOST
    CHROME2_CDP_PORT = EDGE2_CDP_PORT
    CHROME2_DOMAINS = EDGE2_DOMAINS
    CHROME2_AUTO_OPEN_TABS = EDGE2_AUTO_OPEN_TABS

    COCCOC_ENABLED = get_bool_env("COCCOC_ENABLED", False)
    COCCOC_CDP_HOST = os.getenv("COCCOC_CDP_HOST", "127.0.0.1").strip() or "127.0.0.1"
    COCCOC_CDP_PORT = get_int_env("COCCOC_CDP_PORT", 9223)
    COCCOC_DOMAINS = os.getenv("COCCOC_DOMAINS", "")

    COCCOC_AUTO_OPEN_TABS = get_bool_env("COCCOC_AUTO_OPEN_TABS", True)

    XX88_MIN_TABS = max(0, min(3, get_int_env("XX88_MIN_TABS", 2)))  # stateless-entry: XX88 = 2 tab
    V79_MIN_TABS = max(0, min(2, get_int_env("V79_MIN_TABS", 2)))
    # PATCH_MARKER_2026-10-25-v5-xx88-tabs-queue: tran tab XX88 2->3
    # (3 tai khoan XX88: dad123/dad131/hugolan, moi tai khoan mot tab).
    # PATCH_MARKER_2026-10-27-stateless-entry
    # SỐ TAB CỐ ĐỊNH THEO DOMAIN — nguồn duy nhất. Số tab = số luồng nhập song song của
    # domain, KHÔNG phụ thuộc số tài khoản. Tab chỉ là nơi gõ tài khoản + mã rồi submit;
    # không còn "1 tab / 1 tài khoản" nên RAM không tăng theo số tài khoản.
    FIXED_DOMAIN_TABS = {
        "xx88code.com": 2,
        "v79code.com": 2,
        "liverr88.net": 1,            # RR88
        "gg88live.tv": 1,             # GG88
        "livemm88.net": 1,            # MM88
        "llwincode.com": 1,           # LLWIN
        "getcodexx8bonus.pages.dev": 2,  # XX8: chưa được chỉ định -> giữ mức cũ (HTTP, không mở tab)
    }
    DOMAIN_TAB_LIMITS = dict(FIXED_DOMAIN_TABS)
    PARALLEL_DOMAIN_TAB_LIMITS = dict(FIXED_DOMAIN_TABS)

    # Nhập "không trạng thái": bot chỉ biết gõ tài khoản + mã và submit. Không khóa tài khoản
    # "đã nhập hôm nay", không xoay vòng/ghi cursor DB, không đặt chỗ tài khoản (in-flight).
    #   STATELESS_ENTRY_ACCOUNTS=all -> mỗi mã nhập cho TẤT CẢ tài khoản của domain
    #                                   (song song giữa các tab, tuần tự trong 1 tab)
    #   STATELESS_ENTRY_ACCOUNTS=one -> mỗi mã chỉ nhập cho 1 tài khoản (vòng tròn trong RAM)
    # Đặt STATELESS_ACCOUNT_ENTRY=false trong .env để quay về cơ chế cũ.
    STATELESS_ACCOUNT_ENTRY = get_bool_env("STATELESS_ACCOUNT_ENTRY", True)
    STATELESS_ENTRY_ACCOUNTS = (
        "one" if (os.getenv("STATELESS_ENTRY_ACCOUNTS", "all") or "").strip().lower() == "one" else "all"
    )
    ADDITIONAL_CAPTCHA_SAFE_DOMAINS = frozenset(("llwincode.com",))
    TAB_POOL_SIZE = get_int_env("TAB_POOL_SIZE", 3)  # v5: tran pool 3
    TAB_ACQUIRE_WAIT_SECONDS = get_float_env("TAB_ACQUIRE_WAIT_SECONDS", 3.0)

    TAB_ACQUIRE_PROBE_CLOUDFLARE = get_bool_env("TAB_ACQUIRE_PROBE_CLOUDFLARE", False)

    DEFERRED_CLEAN_WAIT_SECONDS = get_float_env("DEFERRED_CLEAN_WAIT_SECONDS", 1.0)
    MAX_TAB_PER_DOMAIN_CAP = get_int_env("MAX_TAB_PER_DOMAIN_CAP", 3)  # v5: tran pool 3

    TAB_POOL_IDLE_TTL = get_float_env("TAB_POOL_IDLE_TTL", 900.0)
    TAB_POOL_MIN_TABS_PER_DOMAIN = get_int_env("TAB_POOL_MIN_TABS_PER_DOMAIN", 1)
    CHROME_IDLE_RELOAD_ENABLED = get_bool_env("CHROME_IDLE_RELOAD_ENABLED", False)

    TAB_POOL_MEMORY_COMPACT_IDLE_SECONDS = get_float_env(
        "TAB_POOL_MEMORY_COMPACT_IDLE_SECONDS", 120.0
    )
    TAB_POOL_MEMORY_RELOAD_IDLE_SECONDS = get_float_env(
        "TAB_POOL_MEMORY_RELOAD_IDLE_SECONDS", 900.0
    )
    TAB_POOL_MEMORY_RELOAD_MIN_GAP_SECONDS = get_float_env(
        "TAB_POOL_MEMORY_RELOAD_MIN_GAP_SECONDS", 45.0
    )
    TAB_POOL_MEMORY_MAX_RELOADS_PER_PASS = get_int_env(
        "TAB_POOL_MEMORY_MAX_RELOADS_PER_PASS", 1
    )
    TAB_POOL_MEMORY_MAX_COMPACTS_PER_PASS = get_int_env(
        "TAB_POOL_MEMORY_MAX_COMPACTS_PER_PASS", 2
    )
    TAB_POOL_MEMORY_PRESSURE_RELOAD_IDLE_SECONDS = get_float_env(
        "TAB_POOL_MEMORY_PRESSURE_RELOAD_IDLE_SECONDS", 300.0
    )
    EDGE_BLOCK_MEDIA_ASSETS = get_bool_env("EDGE_BLOCK_MEDIA_ASSETS", True)
    EDGE_IDLE_UNLOAD_MEDIA = get_bool_env("EDGE_IDLE_UNLOAD_MEDIA", True)

    BROWSER_CAPTCHA_SAFE_MODE = get_bool_env("BROWSER_CAPTCHA_SAFE_MODE", True)

    V79_CAPTCHA_LOAD_WAIT_SECONDS = get_float_env("V79_CAPTCHA_LOAD_WAIT_SECONDS", 1.5)
    V79_CAPTCHA_RELOAD_RETRIES = get_int_env("V79_CAPTCHA_RELOAD_RETRIES", 2)
    CAPTCHA_SAFE_DOMAINS = frozenset(
        d.strip().lower() for d in os.getenv(
            "CAPTCHA_SAFE_DOMAINS",
            "livemm88.net,liverr88.net,xx88code.com,v79code.com",
        ).split(",") if d.strip()
    )

    AUTO_OPEN_MISSING_TABS = get_bool_env("AUTO_OPEN_MISSING_TABS", False)
    TAB_MISSING_RETRY_SECONDS = get_float_env("TAB_MISSING_RETRY_SECONDS", 8.0)
    TAB_MISSING_WARNING_INTERVAL_SECONDS = get_float_env("TAB_MISSING_WARNING_INTERVAL_SECONDS", 30.0)

    TAB_POOL_MEMORY_MAINTENANCE_INTERVAL_SECONDS = get_float_env(
        "TAB_POOL_MEMORY_MAINTENANCE_INTERVAL_SECONDS", 90.0
    )

    CDP_PING_INTERVAL = get_float_env("CDP_PING_INTERVAL", 15.0)
    EDGE_CDP_WATCHDOG_INTERVAL = get_float_env("EDGE_CDP_WATCHDOG_INTERVAL", 5.0)

    RESULT_DETECTION_TIMEOUT = get_float_env("RESULT_DETECTION_TIMEOUT", 5000.0)
    RESULT_SELECTOR_FAST_WINDOW_SECONDS = get_float_env("RESULT_SELECTOR_FAST_WINDOW_SECONDS", 0.80)
    RESULT_POLL_INTERVAL = get_float_env("RESULT_POLL_INTERVAL", 0.05)
    MM88_FORM_SETTLE_SECONDS = get_float_env("MM88_FORM_SETTLE_SECONDS", 0.05)
    MM88_RESULT_DETECTION_TIMEOUT = get_float_env("MM88_RESULT_DETECTION_TIMEOUT", 3500.0)
    RESULT_DETECTION_TIMEOUT_BY_DOMAIN = {
        "livemm88.net": get_float_env("MM88_RESULT_DETECTION_TIMEOUT", 4000.0),
        "liverr88.net": get_float_env("RR88_RESULT_DETECTION_TIMEOUT", 3000.0),
        "xx88code.com": get_float_env("XX88_RESULT_DETECTION_TIMEOUT", 8000.0),
        "gg88live.tv": get_float_env("GG88_RESULT_DETECTION_TIMEOUT", 3500.0),
        "v79code.com": get_float_env("V79_RESULT_DETECTION_TIMEOUT", 2500.0),
        "getcodexx8bonus.pages.dev": get_float_env("XX8_RESULT_DETECTION_TIMEOUT", 7000.0),
    }
    PRESERVE_PREFILLED_USERNAME = get_bool_env("PRESERVE_PREFILLED_USERNAME", True)

    FULL_RELOAD_EVERY_N = get_int_env("FULL_RELOAD_EVERY_N", 300)
    BROWSER_REUSE_HEALTHY_FORMS = get_bool_env("BROWSER_REUSE_HEALTHY_FORMS", True)

    SCREENSHOT_ON_UNKNOWN = get_bool_env("SCREENSHOT_ON_UNKNOWN", True)
    SCREENSHOT_MIN_INTERVAL_SECONDS = get_float_env("SCREENSHOT_MIN_INTERVAL_SECONDS", 60.0)
    SCREENSHOT_MAX_FILES = get_int_env("SCREENSHOT_MAX_FILES", 300)

    SCREENSHOT_ON_FAILED = get_bool_env("SCREENSHOT_ON_FAILED", False)


@lru_cache(maxsize=64)
def _normalize_config_domain(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlsplit(raw if "://" in raw else f"//{raw}")
    return (parsed.hostname or "").lower().rstrip(".").removeprefix("www.")


def is_domain_enabled(domain: str) -> bool:

    canonical = _normalize_active_domain(domain)
    return canonical in getattr(Config, "DOMAIN_TO_CHANNEL_URL", {})


NIGHT_REST_DOMAINS = frozenset({"gg88live.tv", "livemm88.net", "liverr88.net"})
_VIETNAM_TIME = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")


def is_site_business_hours_open(domain: str, now: datetime | None = None) -> bool:
    """GG88/MM88/RR88 open daily 10:00 <= time < 21:00 in Vietnam."""
    if _normalize_active_domain(domain) not in NIGHT_REST_DOMAINS:
        return True
    local = now or datetime.now(_VIETNAM_TIME)
    if local.tzinfo is None:
        local = local.replace(tzinfo=_VIETNAM_TIME)
    local = local.astimezone(_VIETNAM_TIME)
    return 10 <= local.hour < 21


def _build_effective_domain_accounts(canonical_domain: str) -> tuple[tuple[tuple[str, object], ...], ...]:

    overrides = getattr(Config, "DOMAIN_ACCOUNT_OVERRIDES", {})
    source = None
    for key, value in overrides.items():
        if _normalize_active_domain(key) == canonical_domain:
            source = value
            break
    if source is None:
        merged: dict[str, dict] = {}
        for site in getattr(Config, "CHANNEL_CONFIG", {}).values():
            if _normalize_active_domain(site.get("url", "")) != canonical_domain:
                continue
            for account in site.get("accounts", []) or []:
                if not isinstance(account, dict):
                    continue
                username = str(account.get("username", "")).strip()
                if not username:
                    continue
                candidate = dict(account)
                candidate["username"] = username
                previous = merged.get(username)
                try:
                    candidate_priority = int(candidate.get("priority", 999))
                except (TypeError, ValueError):
                    candidate_priority = 999
                try:
                    previous_priority = int(previous.get("priority", 999)) if previous else 999
                except (TypeError, ValueError):
                    previous_priority = 999
                if previous is None or candidate_priority < previous_priority:
                    merged[username] = candidate
        source = merged.values()

    def _sort_key(account: dict) -> tuple[int, str]:
        try:
            priority = int(account.get("priority", 999))
        except (TypeError, ValueError):
            priority = 999
        return priority, str(account.get("username", "")).strip()

    rows = sorted(
        (
            dict(account)
            for account in source
            if isinstance(account, dict) and str(account.get("username", "")).strip()
        ),
        key=_sort_key,
    )
    return tuple(tuple(row.items()) for row in rows)


@lru_cache(maxsize=32)
def _effective_domain_accounts_cached(canonical_domain: str) -> tuple[tuple[tuple[str, object], ...], ...]:
    return _build_effective_domain_accounts(canonical_domain)


def get_effective_domain_accounts(domain: str) -> list[dict]:

    canonical_domain = _normalize_active_domain(domain)
    if not is_domain_enabled(canonical_domain):
        return []
    return [dict(items) for items in _effective_domain_accounts_cached(canonical_domain)]


def _normalize_active_domain(value: str) -> str:
    raw = _normalize_config_domain(value)
    aliases = {
        "llwin": "llwincode.com",
        "mm88": "livemm88.net",
        "rr88": "liverr88.net",
        "xx88": "xx88code.com",
        "xx8": "getcodexx8bonus.pages.dev",
        "gg88": "gg88live.tv",
        "v79": "v79code.com",
    }
    return aliases.get(raw, raw)


_REMOVED_SITE_DOMAINS = frozenset({
    "tangquaqq88.com", "hi88-freecode.pages.dev", "qq88", "hi88",
})


def _configured_domains_without_removed_sites(
    values: str | Iterable[str],
) -> tuple[str, ...]:
    if isinstance(values, str):
        values = values.split(",")
    domains = (
        _normalize_active_domain(str(value))
        for value in values
    )
    return tuple(dict.fromkeys(
        domain for domain in domains
        if domain and domain not in _REMOVED_SITE_DOMAINS
    ))


# Canonicalize domain-valued environment settings once so all runtime lookups
# use the same hostname representation as CHANNEL_CONFIG.
for _domain_setting in (
    "FRESHEST_FIRST_DOMAINS",
    "VERIFICATION_GATE_DOMAINS",
    "DEFER_BUDGET_DOMAINS",
    "OCR_VIDEO_EARLY_STOP_DOMAINS",
    "OCR_VIDEO_EARLY_EMIT_DOMAINS",
):
    setattr(
        Config,
        _domain_setting,
        _configured_domains_without_removed_sites(
            getattr(Config, _domain_setting, ())
        ),
    )
Config.DIRECT_HTTP_DOMAINS = frozenset(
    _configured_domains_without_removed_sites(Config.DIRECT_HTTP_DOMAINS)
)
Config.BROWSER_ONLY = not bool(Config.DIRECT_HTTP_DOMAINS)
Config.SKIP_BRING_TO_FRONT_DOMAINS = _configured_domains_without_removed_sites(
    Config.SKIP_BRING_TO_FRONT_DOMAINS
)
Config.MANUAL_VERIFICATION_FOREGROUND_DOMAINS = ",".join(
    _configured_domains_without_removed_sites(
        Config.MANUAL_VERIFICATION_FOREGROUND_DOMAINS
    )
)
Config.EDGE2_DOMAINS = ",".join(
    _configured_domains_without_removed_sites(Config.EDGE2_DOMAINS)
)
Config.CHROME2_DOMAINS = Config.EDGE2_DOMAINS
Config.COCCOC_DOMAINS = ",".join(
    _configured_domains_without_removed_sites(Config.COCCOC_DOMAINS)
)
Config.CAPTCHA_SAFE_DOMAINS = frozenset(
    _configured_domains_without_removed_sites(Config.CAPTCHA_SAFE_DOMAINS)
)

_active_domains_raw = os.getenv("ACTIVE_DOMAINS", "").strip()
if _active_domains_raw:
    _active_domains = {
        _normalize_active_domain(part)
        for part in _active_domains_raw.split(",")
        if str(part).strip()
        and _normalize_active_domain(part) not in _REMOVED_SITE_DOMAINS
    }
    if _active_domains:
        Config.CHANNEL_CONFIG = {
            chat_id: cfg
            for chat_id, cfg in Config.CHANNEL_CONFIG.items()
            if _normalize_active_domain(cfg.get("url", "")) in _active_domains
        }
else:
    _active_domains = set()
Config.ACTIVE_DOMAINS = tuple(sorted(_active_domains))


Config.DOMAIN_TO_CHANNEL_URL: dict[str, str] = {}
for _cfg in Config.CHANNEL_CONFIG.values():
    _raw_url = str(_cfg.get("url", ""))
    _domain = _normalize_config_domain(_raw_url)
    if _domain:
        Config.DOMAIN_TO_CHANNEL_URL.setdefault(_domain, _raw_url)


_config_logger = logging.getLogger("autobot." + __name__)
for _chat_id, _site in Config.CHANNEL_CONFIG.items():
    _domain = _normalize_config_domain(_site.get("url", ""))
    _canonical = get_effective_domain_accounts(_domain)
    if _canonical:
        _old_accounts = _site.get("accounts", [])
        _new_accounts = [dict(_account) for _account in _canonical]
        if _old_accounts != _new_accounts:
            _config_logger.debug(
                "Domain account override applied: chat=%s domain=%s old=%s new=%s",
                _chat_id,
                _domain,
                [a.get("username") for a in _old_accounts],
                [a.get("username") for a in _new_accounts],
            )
        _site["accounts"] = _new_accounts


for chat_id, site in Config.CHANNEL_CONFIG.items():
    group = site.get("filter_group")
    if group and group not in Config.CODE_FILTER_GROUPS:
        raise ValueError(
            f"LỖI CẤU HÌNH: filter_group '{group}' (site '{site.get('name', 'Unknown')}') "
            f"chưa được định nghĩa trong CODE_FILTER_GROUPS!"
        )