from __future__ import annotations

import asyncio
import contextlib
import copy
import csv
import gc
import hashlib
import json
import os
import random
import re
import shutil
import socket
import sqlite3
import sys
import tempfile
import time
import traceback
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache, partial
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from telethon import TelegramClient, events
from telethon.sessions import SQLiteSession
from telethon.utils import get_peer_id
from telethon.errors.rpcerrorlist import AuthKeyDuplicatedError
from telethon.errors import FloodWaitError
from telethon.network import ConnectionTcpAbridged
from telethon.tl.types import DocumentAttributeVideo, MessageEntitySpoiler, MessageMediaDocument

from config import Config, get_effective_domain_accounts, is_domain_enabled, is_site_business_hours_open
from browser_site_profiles import get_site_profile, uses_direct_http
from logger_setup import (
    logger, reset_log_context, set_log_context, warning_once, shutdown_logging,
)
from code_validator import CodeValidator
from code_identity import normalize_code
from ocr_pipeline import extract_site_codes, POLICY_VERSION as OCR_POLICY_VERSION, OCR_SEMAPHORE
from image_code_extractor import (
    _OCR_EXECUTOR,
    VIDEO_FORMATS,
    get_image_extractor,
    warmup_image_extractor,
    shutdown_ocr_executor,
)
from database import init_database
from durable_inbox import DurableInbox
# PATCH_MARKER_2026-10-28-ingest-bounded-dedup-full
from tg_ingest import (IngressStage, ReceiverConfig, TelegramReceiver, RAW_INGEST_QUEUE,
                       RawIngressItem)
from telegram_push_health import TelegramPushHealth, watchdog_backoff_seconds
from freshness import age_seconds, first_known_age, is_stale
from monitoring import (
    init_monitoring, init_performance_monitor, stop_monitoring,
    ocr_admission_paused,
)
from disk_guard import (
    _cleanup_stale_ocr_tmp,
    cleanup_daily,
    cleanup_weekly,
    ensure_disk_space,
    register_ocr_tmp,
    release_ocr_tmp,
)
from chrome_launcher import ensure_chromes_ready, shutdown_chromes, CHROME_PROCESSES
from features import (
    BOT_VERSION,
    command_registry,
    get_shutdown_handler,
    print_version_info,
    register_default_commands,
    setup_admin_commands,
)
from timing import RequestTimer, get_current_timer, reset_current_timer, set_current_timer
from media_download_manager import MediaDownloadManager
from media_identity import telegram_media_identity
from browser_adapter import BrowserEngineAdapter
from http_submitter import HttpSubmitter
from submit_pacer import pacer as submit_pacer
from queue_manager import get_queue_manager, init_queue_manager
from dashboard import (
    report_download_finished,
    report_download_started,
    report_ocr_result,
    update_dashboard,
)

from code_extraction import (
    get_filter_group_name, unique_keep_order,
    remove_noise_from_text, line_has_code_marker, line_is_noise,
    extract_tokens_from_line, extract_spoiler_codes,
    extract_marker_near_codes, _plain_text_token_is_safe,
    extract_plain_text_codes, extract_codes_from_message,
)
from kjc import is_kjc_special_channel, extract_kjc_routed_items
from xx88_priority_queue import RandomCodeQueue, RandomBoundedCodeQueue
import scheduling
from channel_rules import (
    _is_ocr_allowed_channel,
)


try:
    import browser_engine

    _BROWSER_ENGINE_OK = True
except Exception as _browser_import_err:
    browser_engine = None
    _BROWSER_ENGINE_OK = False
    logger.error(
        f"❌ [Browser] Không load được browser_engine.py ({_browser_import_err}) — "
        f"Các site dùng trình duyệt không thể submit khi browser_engine lỗi. "
        f"Kiểm tra: pip install playwright --break-system-packages"
    )

def _notify_browser_exhausted(domain: str, username: str) -> None:
    if uses_direct_http(domain):
        return

    if browser_engine is not None:
        try:
            browser_engine.mark_account_exhausted(domain, username)
        except Exception:
            pass


def _prefilled_accounts_for(domain: str) -> list[str]:
    if uses_direct_http(domain):
        return []
    if browser_engine is None:
        return []
    try:
        return [str(x).lower() for x in browser_engine.get_prefilled_accounts(domain)]
    except Exception:
        return []


_MEDIA_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="media-io")
_DB_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="db-io")


_INBOX_INGRESS_EXECUTOR = ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="inbox-ingress",
)
_INBOX_STATE_EXECUTOR = ThreadPoolExecutor(
    max_workers=6,
    thread_name_prefix="inbox-state",
)

_INBOX_EXECUTOR = _INBOX_STATE_EXECUTOR

_SITE_LOG_LABELS = {
    "llwincode.com": "LLWIN",
    "xx88code.com": "XX88",
    "getcodexx8bonus.pages.dev": "XX8",
    "liverr88.net": "RR88",
    "gg88live.tv": "GG88",
    "livemm88.net": "MM88",
    "v79code.com": "V79",
}
_PROMO_LABEL_INLINE_RE = re.compile(r"(?im)^\s*M(?:Ã|A|4){1,2}\s*[:\-]\s*[A-Za-z0-9_]{2,14}\s*$")
_PROMO_LABEL_ONLY_RE = re.compile(r"(?im)^\s*M(?:Ã|A|4){1,2}\s*[:\-]?\s*$")
_XX88_BIGWIN_RE = re.compile(r"^[A-Za-z0-9](\*[A-Za-z0-9]){4,}$")
_XX88_OCR_BANNER_TOKENS = frozenset(
    {
        "THUMAY", "TOPWIN", "MEGA", "LIVE", "MEGALIVE", "TANG", "QUA",
        "PHAT", "CODE", "FREE", "GIFTCODE", "GIFT", "BONUS", "PROMO",
        "PROMOTION", "KHUYENMAI", "VIP", "GAME", "WIN", "WINNER",
    }
)
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_WWW_RE = re.compile(r"www\.\S+", re.IGNORECASE)
_TME_RE = re.compile(r"t\.me/\S+", re.IGNORECASE)
_DOMAIN_RE = re.compile(r"\b[a-zA-Z0-9.-]+\.(?:com|net|org|vn|app|info)\b", re.IGNORECASE)
_HASHTAG_RE = re.compile(r"#\S+")
_CURRENCY_RE = re.compile(r"\d[\d.,]{3,}\s*(?:VND|VNĐ)\b", re.IGNORECASE)
_CODE_MARKER_RE = re.compile(
    r"NHẬN\s+CODE(?:\s+NGAY)?|NHAN\s+CODE(?:\s+NGAY)?|"
    r"NHẬP\s+CODE|NHAP\s+CODE|PHÁT\s+CODE|PHAT\s+CODE|"
    r"CODE\s+FREE|FREE\s+CODE|GIFT\s*CODE|GIFTCODE|"
    r"TẶNG\s+CODE|TANG\s+CODE",
    re.IGNORECASE,
)
_NOISE_RE = re.compile(
    r"HTTP|WWW|\.COM|FACEBOOK|TELEGRAM|TIKTOK|ZALO|CSKH|BOT|CHECK\s+LINK|LINK",
    re.IGNORECASE,
)
_TOKEN_RE = re.compile(
    rf"[A-Za-z0-9{re.escape(getattr(Config, 'SPECIAL_CODE_CHARS_30', ''))}]"
    rf"{{{getattr(Config, 'CODE_MIN_LENGTH', 6)},{getattr(Config, 'CODE_MAX_LENGTH', 15) + 30}}}"
)
_ALNUM_TOKEN_RE = re.compile(r"[a-zA-Z0-9]{6,15}")
_KJC_LABEL_RE = re.compile(
    r"(?:NHẬN\s+CODE|NHAN\s+CODE|CODE)\s*(?:NGAY|NGÀY)?\s*[:\-–—]?\s*"
    r"(MM88|RR88|XX88|GG88)",
    re.IGNORECASE,
)
_KJC_SITE_RE = {
    key: re.compile(rf"\b{re.escape(key)}\b", re.IGNORECASE)
    for key in ("MM88", "RR88", "XX88", "GG88")
}


_browser_adapter = None
_http_submitter = None
_durable_inbox = None
_inbox_drain_task = None
_channel_poll_task = None
_inbox_wakeup = None
_receiver = None
_ingress_stage = None
_ingress_stage_task = None
_inbox_message_cache: dict[int, object] = {}
_INBOX_MSG_CACHE_MAX = 4000


def _get_browser_adapter():
    global _browser_adapter
    if _browser_adapter is None:
        if not _BROWSER_ENGINE_OK:
            raise RuntimeError("browser_engine không sẵn sàng")
        _browser_adapter = BrowserEngineAdapter(browser_engine)
    return _browser_adapter


def _get_http_submitter():
    global _http_submitter
    if _http_submitter is None:
        _http_submitter = HttpSubmitter()
    return _http_submitter


class BotState:
    def __init__(self):
        self.is_running = True
        self._site_code_seen: dict = {}
        self.handler_registered = False
        self._last_cleanup_time = time.time()
        self.bg_tasks: set = set()
        self._channel_account_index: dict = {}
        self._domain_account_cursor: dict[str, int] = {}
        self._fanout_account_cursor: dict[str, int] = {}
        self._rotation_loaded_domains: set[str] = set()
        self._daily_used: set = set()
        self._account_exhausted_today: set = set()
        self._daily_date: str = datetime.now().strftime("%Y-%m-%d")
        self._inflight_codes: set = set()
        self._inflight_accounts: set = set()
        self._processed_message_hashes: dict = {}
        self._inbox_enqueued_ids: set[int] = set()
        self._work_item_enqueued_ids: set[int] = set()
        self.last_raw_update_at: float | None = None
        self.last_accepted_message_at: float | None = None
        self.raw_update_count: int = 0
        self.accepted_message_count: int = 0
        self.event_loop_lag_ms: float = 0.0
        # PATCH_MARKER_2026-10-26-v9-core-ocr-sqlite: reset only the heartbeat peak.
        self.event_loop_lag_peak_ms: float = 0.0


bot_state = BotState()
BOT_START_TIME: datetime = datetime.now(timezone.utc)

_media_download_manager = MediaDownloadManager(
    max_concurrent=getattr(Config, "MAX_CONCURRENT_MEDIA_DOWNLOADS", 4),
    retries=getattr(Config, "MEDIA_DOWNLOAD_RETRIES", 1),
    retry_delay=getattr(Config, "MEDIA_DOWNLOAD_RETRY_DELAY", 0.5),
    max_size_bytes=(int(getattr(Config, "MEDIA_DOWNLOAD_MAX_SIZE_MB", 200)) * 1024 * 1024 if getattr(Config, "MEDIA_DOWNLOAD_MAX_SIZE_MB", 200) else None),
    fast_download=getattr(Config, "FAST_TELEGRAM_DOWNLOAD", False),
    # Nâng ngưỡng kích hoạt multipart từ 4MB lên 8MB để file nhỏ tải luồng đơn ổn định hơn
    fast_min_bytes=getattr(Config, "FAST_DOWNLOAD_MIN_BYTES", 8 * 1024 * 1024),
    fast_workers=getattr(Config, "FAST_DOWNLOAD_WORKERS", 6),
    fast_chunk_kb=getattr(Config, "FAST_DOWNLOAD_CHUNK_KB", 512),
)


_xx8_media_download_manager = MediaDownloadManager(
    max_concurrent=1,
    retries=getattr(Config, "MEDIA_DOWNLOAD_RETRIES", 1),
    retry_delay=getattr(Config, "MEDIA_DOWNLOAD_RETRY_DELAY", 0.5),
    max_size_bytes=(int(getattr(Config, "MEDIA_DOWNLOAD_MAX_SIZE_MB", 200)) * 1024 * 1024 if getattr(Config, "MEDIA_DOWNLOAD_MAX_SIZE_MB", 200) else None),
    fast_download=getattr(Config, "XX8_FAST_TELEGRAM_DOWNLOAD", True),
    # Nâng ngưỡng của XX8 từ 1MB lên 4MB để tránh chia nhỏ quá mức các video ngắn gây nghẽn chunk
    fast_min_bytes=getattr(Config, "XX8_FAST_DOWNLOAD_MIN_BYTES", 8 * 1024 * 1024),
    fast_workers=getattr(Config, "XX8_FAST_DOWNLOAD_WORKERS", 8),
    fast_chunk_kb=getattr(Config, "FAST_DOWNLOAD_CHUNK_KB", 512),
)

# ── Telethon session chịu khoá SQLite (gộp vào main_script.py, không cần file riêng) ──
def _sqlite_is_locked(exc: BaseException) -> bool:
    """True nếu là lỗi khoá tạm thời của SQLite (database is locked / busy)."""
    return isinstance(exc, sqlite3.OperationalError) and any(
        word in str(exc).lower() for word in ("locked", "busy")
    )


class ResilientSQLiteSession(SQLiteSession):
    """SQLiteSession không làm đứng event loop và không văng "database is locked".

    Telethon ghi file .session ĐỒNG BỘ ngay trên event loop (mỗi update có entity,
    mỗi lần đổi update_state). Mặc định sqlite3 chờ khoá 5 giây rồi raise
    OperationalError => loop đứng 5s (loop_lag), push/poll tắc, bot tự dừng.

    Lớp này:
      - chờ khoá tối đa ``busy_timeout`` (mặc định 1s) thay vì 5s;
      - ghi "không quan trọng" (entity cache, update_state, commit) bị khoá thì bỏ qua
        và ghi lại ở lần sau, không raise;
      - không ghi lại entity khi dữ liệu không đổi (giảm hẳn số lần ghi vào đĩa);
      - ghi "quan trọng" (auth key, DC, takeout) chờ tối đa ``critical_timeout`` giây;
      - bật WAL để đọc/ghi không chặn nhau (tắt bằng TELEGRAM_SESSION_WAL=0 nếu thư mục
        bot nằm trên OneDrive/ổ mạng).
    """

    _ENTITY_REFRESH_SECONDS = 3600.0
    _ENTITY_CACHE_MAX = 50000

    def __init__(self, session_id=None, *, busy_timeout=1.0, critical_timeout=10.0, wal=True):
        # Phải đặt trước super().__init__(): SQLiteSession.__init__ gọi _cursor() ngay.
        self._busy_timeout = max(0.05, float(busy_timeout))
        self._critical_timeout = max(self._busy_timeout, float(critical_timeout))
        self._use_wal = bool(wal)
        self._critical = False
        self._critical_dirty = False
        self._entity_seen: dict = {}
        self._skipped_writes = 0
        self._skip_log_ts = 0.0
        # Lúc dựng session (import module) chặn vài giây vẫn ổn: chưa có event loop chạy.
        self._critical = True
        try:
            super().__init__(session_id)
        finally:
            self._critical = False
            self._apply_busy(self._busy_timeout)

    # ── hạ tầng ────────────────────────────────────────────────────────────────
    def _apply_busy(self, seconds: float) -> None:
        conn = self._conn
        if conn is None or self.filename == ":memory:":
            return
        try:
            conn.execute("PRAGMA busy_timeout=%d" % int(max(0.0, seconds) * 1000))
        except sqlite3.Error:
            pass

    @contextlib.contextmanager
    def _critical_section(self):
        previous = self._critical
        self._critical = True
        self._apply_busy(self._critical_timeout)
        try:
            yield
        finally:
            self._critical = previous
            if not previous:
                self._apply_busy(self._busy_timeout)

    def _cursor(self):
        if self._conn is None:
            timeout = self._critical_timeout if self._critical else self._busy_timeout
            conn = sqlite3.connect(self.filename, timeout=timeout, check_same_thread=False)
            self._conn = conn
            if self.filename != ":memory:":
                try:
                    conn.execute("PRAGMA busy_timeout=%d" % int(timeout * 1000))
                    if self._use_wal:
                        conn.execute("PRAGMA journal_mode=WAL")
                    else:
                        # WAL được lưu trong file: tắt cờ thì phải đưa file về chế độ thường.
                        current = conn.execute("PRAGMA journal_mode").fetchone()
                        if current and str(current[0]).lower() == "wal":
                            conn.execute("PRAGMA journal_mode=DELETE")
                    conn.execute("PRAGMA synchronous=NORMAL")
                except sqlite3.Error as exc:
                    logger.warning("⚠️ [Session] không đặt được PRAGMA (bỏ qua): %s", exc)
        return self._conn.cursor()

    def _note_skip(self, what: str) -> None:
        self._skipped_writes += 1
        now = time.monotonic()
        if now - self._skip_log_ts >= 30.0:
            self._skip_log_ts = now
            logger.warning(
                "⚠️ [Session] file session đang bị khoá — bỏ qua lần %s, sẽ ghi lại sau "
                "(tổng đã bỏ qua: %s)",
                what,
                self._skipped_writes,
            )

    def _soft(self, fn, what: str, default=None):
        """Chạy thao tác không quan trọng: bị khoá thì bỏ qua thay vì raise."""
        if self._critical:
            return fn()
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            if not _sqlite_is_locked(exc):
                raise
            self._note_skip(what)
            return default

    # ── ghi quan trọng: chờ lâu hơn, lỗi thật thì raise ────────────────────────
    def set_dc(self, dc_id, server_address, port):
        with self._critical_section():
            return super().set_dc(dc_id, server_address, port)

    @SQLiteSession.auth_key.setter
    def auth_key(self, value):
        with self._critical_section():
            self._auth_key = value
            self._update_session_table()
            self._critical_dirty = True

    @SQLiteSession.takeout_id.setter
    def takeout_id(self, value):
        with self._critical_section():
            self._takeout_id = value
            self._update_session_table()
            self._critical_dirty = True

    def get_update_states(self):
        with self._critical_section():
            return list(super().get_update_states())

    # ── ghi/đọc không quan trọng: khoá thì bỏ qua ──────────────────────────────
    def set_update_state(self, entity_id, state):
        return self._soft(
            lambda: super(ResilientSQLiteSession, self).set_update_state(entity_id, state),
            "ghi update_state",
        )

    def _execute(self, stmt, *values):
        return self._soft(lambda: super(ResilientSQLiteSession, self)._execute(stmt, *values), "đọc cache")

    def get_entity_rows_by_username(self, username):
        return self._soft(
            lambda: super(ResilientSQLiteSession, self).get_entity_rows_by_username(username),
            "đọc entity",
        )

    def process_entities(self, tlo):
        if not self.save_entities:
            return
        rows = self._entities_to_rows(tlo)
        if not rows:
            return
        now = time.time()
        seen = self._entity_seen
        changed = []
        for row in rows:
            previous = seen.get(row[0])
            if previous is None or previous[0] != row[1:] or now - previous[1] >= self._ENTITY_REFRESH_SECONDS:
                changed.append(row)
        if not changed:
            return  # entity không đổi => không ghi đĩa
        stamp = (int(now),)
        data = [row + stamp for row in changed]

        def _write():
            cursor = self._cursor()
            try:
                cursor.executemany("insert or replace into entities values (?,?,?,?,?,?)", data)
            finally:
                cursor.close()
            return True

        if self._soft(_write, "ghi entity", default=False):
            if len(seen) > self._ENTITY_CACHE_MAX:
                seen.clear()
            for row in changed:
                seen[row[0]] = (row[1:], now)

    def save(self):
        conn = self._conn
        if conn is None:
            return
        critical = self._critical_dirty
        try:
            if critical:
                with self._critical_section():
                    conn.commit()
                self._critical_dirty = False
            else:
                conn.commit()
        except sqlite3.OperationalError as exc:
            if not _sqlite_is_locked(exc):
                raise
            # Giữ nguyên transaction đang chờ; lần save() sau sẽ commit tiếp.
            self._note_skip("commit")

    def close(self):
        if self.filename == ":memory:" or self._conn is None:
            return
        try:
            with self._critical_section():
                self._conn.commit()
        except sqlite3.OperationalError as exc:
            if not _sqlite_is_locked(exc):
                raise
            logger.warning("⚠️ [Session] không commit được lúc đóng (file bị khoá): %s", exc)
        try:
            self._conn.close()
        except sqlite3.Error:
            pass
        self._conn = None


def _build_telegram_session():
    """Trả về session cho TelegramClient: bản chịu khoá, hoặc tên file (Telethon mặc định)."""
    name = Config.SESSION_NAME
    if not bool(getattr(Config, "TELEGRAM_SESSION_RESILIENT", True)):
        return name
    try:
        return ResilientSQLiteSession(
            name,
            busy_timeout=float(getattr(Config, "TELEGRAM_SESSION_BUSY_TIMEOUT", 1.0)),
            critical_timeout=float(getattr(Config, "TELEGRAM_SESSION_CRITICAL_TIMEOUT", 10.0)),
            wal=bool(getattr(Config, "TELEGRAM_SESSION_WAL", True)),
        )
    except Exception as exc:  # không để lỗi session tuỳ biến làm bot không khởi động được
        logger.warning("⚠️ [Session] dựng ResilientSQLiteSession lỗi (%s) — dùng session mặc định", exc)
        return name


# ── Đánh dấu bot dừng ngoài ý muốn để run.bat tự chạy lại (exit code 4) ────────────
EXIT_UNINTENDED_STOP = 4
_UNINTENDED_STOP = False


def _mark_unintended_stop(reason: str) -> None:
    """Gọi ở các nhánh 'dừng bot' do lỗi. Không đánh dấu nếu người dùng chủ động tắt."""
    global _UNINTENDED_STOP
    try:
        if get_shutdown_handler().shutdown_initiated:
            return
    except Exception:
        pass
    _UNINTENDED_STOP = True
    logger.critical("🔁 [Supervisor] Bot dừng ngoài ý muốn (%s) — thoát mã %s để run.bat tự chạy lại",
                    reason, EXIT_UNINTENDED_STOP)


_RESTARTED_BY_LAUNCHER = os.environ.get("AUTOBOT_RESTARTED") == "1"


client = None

async def send_auth_key_alert(exc: BaseException) -> bool:

    token = str(getattr(Config, "ALERT_BOT_TOKEN", "") or "").strip()
    chat_id = getattr(Config, "ALERT_CHAT_ID", 0)
    timeout = max(
        1.0,
        float(getattr(Config, "ALERT_HTTP_TIMEOUT", 10.0)),
    )

    if not token:
        logger.critical(
            "❌ [Alert] Thiếu ALERT_BOT_TOKEN — không thể gửi cảnh báo "
            "AuthKeyDuplicated"
        )
        return False

    if not chat_id:
        logger.critical(
            "❌ [Alert] Thiếu ALERT_CHAT_ID — không thể gửi cảnh báo "
            "AuthKeyDuplicated"
        )
        return False

    session_name = os.path.basename(str(getattr(Config, "SESSION_NAME", "")))
    error_text = str(exc).replace("\r", " ").replace("\n", " ").strip()
    if len(error_text) > 500:
        error_text = error_text[:497] + "..."

    text = "\n".join(
        (
            "🚨 BOT DỪNG KHẨN CẤP",
            "Lỗi: Telegram AuthKeyDuplicated",
            "",
            f"• Session: {session_name or '(unknown)' }",
            f"• Host: {socket.gethostname()}",
            f"• PID: {os.getpid()}",
            f"• Thời gian: {datetime.now().astimezone().isoformat()}",
            f"• Chi tiết: {error_text}",
            "",
            "Bot đã dừng retry tự động.",
            "Kiểm tra instance/IP đang dùng chung session, sau đó tạo "
            "session mới nếu cần.",
        )
    )

    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    body = urlencode(
        {
            "chat_id": str(chat_id),
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")

    request = Request(
        endpoint,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "giftcode-bot-alert/1.0",
        },
        method="POST",
    )

    def _post_alert() -> tuple[int, bytes]:
        with urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read()

    try:
        status_code, response_body = await asyncio.to_thread(_post_alert)
        if 200 <= status_code < 300:
            logger.critical(
                "🚨 [Alert] Đã gửi cảnh báo AuthKeyDuplicated qua Bot API "
                "độc lập tới chat_id=%s",
                chat_id,
            )
            return True

        logger.critical(
            "❌ [Alert] Bot API trả HTTP %s: %s",
            status_code,
            response_body[:300].decode("utf-8", errors="replace"),
        )
        return False

    except Exception as alert_error:

        logger.critical(
            "❌ [Alert] Gửi cảnh báo AuthKeyDuplicated thất bại: %s",
            alert_error,
        )
        return False


def backup_corrupt_telegram_session() -> dict[str, object]:

    enabled = bool(getattr(Config, "SESSION_AUTO_BACKUP", True))
    delete_after_backup = bool(
        getattr(Config, "SESSION_DELETE_AFTER_BACKUP", False)
    )
    session_value = str(getattr(Config, "SESSION_NAME", "") or "").strip()

    result: dict[str, object] = {
        "enabled": enabled,
        "deleted": False,
        "backup_dir": "",
        "files": [],
        "error": "",
    }

    if not enabled:
        logger.warning("⚠️ [Session] SESSION_AUTO_BACKUP=false — bỏ qua backup")
        return result

    if not session_value:
        result["error"] = "SESSION_NAME is empty"
        logger.error("❌ [Session] Không backup được: SESSION_NAME rỗng")
        return result

    session_path = Path(session_value)
    if session_path.suffix.lower() != ".session":
        session_path = session_path.with_name(session_path.name + ".session")

    source_files = [
        session_path,
        Path(str(session_path) + "-journal"),
        Path(str(session_path) + "-wal"),
        Path(str(session_path) + "-shm"),
    ]
    existing_files = [path for path in source_files if path.is_file()]

    if not existing_files:
        result["error"] = f"session files not found: {session_path}"
        logger.error(
            "❌ [Session] Không tìm thấy file session để backup: %s",
            session_path,
        )
        return result

    backup_root = Path(
        getattr(Config, "SESSION_BACKUP_DIR", "backups/sessions")
    )
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    backup_dir = backup_root / f"{session_path.stem}_{stamp}"
    result["backup_dir"] = str(backup_dir)

    copied_files: list[Path] = []
    try:
        backup_dir.mkdir(parents=True, exist_ok=False)

        for source in existing_files:
            destination = backup_dir / source.name
            shutil.copy2(source, destination)
            copied_files.append(source)

        result["files"] = [str(path) for path in copied_files]
        logger.warning(
            "📦 [Session] Đã backup %s file vào %s",
            len(copied_files),
            backup_dir,
        )

        if delete_after_backup:
            for source in copied_files:
                source.unlink()
            result["deleted"] = True
            logger.warning(
                "🗑️ [Session] Đã xóa %s file session sau khi backup thành công",
                len(copied_files),
            )
        else:
            logger.info(
                "ℹ️ [Session] Giữ nguyên session lỗi; bật "
                "SESSION_DELETE_AFTER_BACKUP=true nếu muốn xóa"
            )

        return result

    except Exception as backup_error:
        result["error"] = str(backup_error)
        logger.exception(
            "❌ [Session] Backup thất bại — không xóa session: %s",
            backup_error,
        )
        return result


_systems = None
message_queue: asyncio.Queue | None = None
message_workers: list = []
_history_queue: asyncio.Queue | None = asyncio.Queue(maxsize=2000)
_history_writer_task = None
_domain_semaphores: dict = {}
_submit_semaphore: asyncio.Semaphore | None = None
_http_submit_semaphore: asyncio.Semaphore | None = None
_active_submit_tasks: set = set()

_domain_queues: dict = {}
_domain_accounts: dict = {}
_domain_workers: dict = {}
_tab_sync_pending: dict = {}


def _dispatch_max_age(item: dict, domain: str) -> float:
    source = str(item.get("source_kind") or item.get("source") or "").lower()
    if source == "ocr":
        return float(getattr(Config, "CODE_MAX_AGE_SECONDS", 120.0))
    return float((getattr(Config, "SITE_CODE_MAX_AGE_SECONDS", None) or {}).get(
        domain, getattr(Config, "CODE_MAX_AGE_SECONDS", 120.0)
    ))
def _media_download_timeout(domain: str, message_date=None) -> float:
    """Timeout tải media: không quá tuổi tối đa của mã theo domain, tối đa MEDIA_DOWNLOAD_TIMEOUT_SECONDS."""
    cap = float(getattr(Config, "MEDIA_DOWNLOAD_TIMEOUT_SECONDS", 180.0))
    if cap <= 0:
        cap = 180.0
    max_age = float((getattr(Config, "SITE_CODE_MAX_AGE_SECONDS", None) or {}).get(
        domain, getattr(Config, "CODE_MAX_AGE_SECONDS", 120.0)
    ))
    if max_age > 0:
        age = age_seconds(message_date)
        remaining = max_age - age if age is not None else max_age
        return max(0.001, min(cap, remaining))
    return cap


_account_rotation_locks: dict[str, asyncio.Lock] = {}
# PATCH_MARKER_2026-11-01-xx8-rotate: con tro xoay vong rieng cho XX8
# (RAM, reset khi restart bot thi vong lai bat dau tu kaoboy012).
_xx8_rotate_cursor: dict[str, int] = {}
_ocr_media_queue: asyncio.Queue | None = None
_ocr_media_workers: list = []

_proc_semaphore: asyncio.Semaphore | None = None
_ocr_semaphore: asyncio.Semaphore | None = None
_domain_rate_limiters: dict = {}


_ocr_result_cache: dict[str, tuple[float, dict]] = {}


def _ocr_file_cache_key(
    path: str,
    target_url: str,
    crop_config,
    is_video: bool,
    single_code: bool,
    telegram_media_id=None,
    policy=None,
) -> str:

    if telegram_media_id is not None:
        media_key = f"telegram-media:{telegram_media_id}"
    else:
        digest = hashlib.sha256()
        with open(path, "rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        media_key = digest.hexdigest()
    params = json.dumps(
        {
            "target_url": target_url,
            "crop": crop_config,
            "video": bool(is_video),
            "single": bool(single_code),
            "policy": policy,
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return f"{media_key}:{hashlib.sha256(params).hexdigest()}"


def _telegram_media_id(event):
    return telegram_media_identity(getattr(event, "message", None))


def _ocr_video_media_info(event):
    message = getattr(event, "message", None)
    media = getattr(message, "media", None)
    document = getattr(media, "document", None) or getattr(message, "document", None)
    attributes = getattr(document, "attributes", ()) or ()
    mime_type = str(getattr(document, "mime_type", "") or "")
    is_video = mime_type.startswith("video/") or any(
        isinstance(attribute, DocumentAttributeVideo) for attribute in attributes
    )
    if not is_video:
        return False, None, None
    file_info = getattr(message, "file", None)
    extension = Path(str(getattr(file_info, "name", "") or "")).suffix.lower()
    if not extension:
        extension = str(getattr(file_info, "ext", "") or "").lower()
    if extension and not extension.startswith("."):
        extension = f".{extension}"
    if not extension:
        extension = {
            "video/mp4": ".mp4",
            "video/quicktime": ".mov",
            "video/x-matroska": ".mkv",
            "video/webm": ".webm",
        }.get(mime_type, ".mp4")
    media_id = _telegram_media_id(event)
    return True, media_id, extension


def _ocr_video_cache_path(media_id):
    if not media_id:
        return None
    key = hashlib.sha256(str(media_id).encode("utf-8")).hexdigest()
    return Path(getattr(Config, "OCR_VIDEO_FILE_CACHE_DIR", "data/ocr_video_cache")) / f"{key}.video"


def _prune_ocr_video_file_cache(cache_dir: Path | None = None) -> None:
    cache_dir = cache_dir or Path(
        getattr(Config, "OCR_VIDEO_FILE_CACHE_DIR", "data/ocr_video_cache")
    )
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        files = []
        for path in cache_dir.glob("*.video"):
            try:
                stat = path.stat()
            except OSError as exc:
                logger.warning("OCR_VIDEO_FILE_CACHE stat_failed path=%s error=%s", path, exc)
                continue
            files.append((path, stat.st_mtime, stat.st_size))
    except OSError as exc:
        logger.warning("OCR_VIDEO_FILE_CACHE scan_failed dir=%s error=%s", cache_dir, exc)
        return

    now = time.time()
    ttl = max(1.0, float(getattr(Config, "OCR_VIDEO_FILE_CACHE_TTL_SECONDS", 7 * 86400)))
    max_entries = max(1, int(getattr(Config, "OCR_VIDEO_FILE_CACHE_MAX_ENTRIES", 128)))
    max_bytes = max(1, int(getattr(Config, "OCR_VIDEO_FILE_CACHE_MAX_BYTES", 1024**3)))
    active = []
    for item in files:
        path, modified, size = item
        if now - modified > ttl:
            try:
                path.unlink()
            except OSError as exc:
                logger.warning("OCR_VIDEO_FILE_CACHE evict_failed path=%s error=%s", path, exc)
                active.append(item)
            continue
        active.append(item)

    total_bytes = sum(size for _, _, size in active)
    active.sort(key=lambda item: item[1])
    failed_evictions = set()
    while active and (len(active) > max_entries or total_bytes > max_bytes):
        candidate = next(
            (index for index, item in enumerate(active) if item[0] not in failed_evictions),
            None,
        )
        if candidate is None:
            break
        path, _, size = active[candidate]
        try:
            path.unlink()
            active.pop(candidate)
            total_bytes -= size
        except OSError as exc:
            logger.warning("OCR_VIDEO_FILE_CACHE evict_failed path=%s error=%s", path, exc)
            failed_evictions.add(path)


def _prune_ocr_video_partial_cache(partial_root: Path | None = None) -> None:
    from media_download_manager import _active_partial_dirs
    root = partial_root or Path(getattr(Config, "OCR_VIDEO_FILE_CACHE_DIR", "data/ocr_video_cache")).parent / "ocr_video_partial"
    if not root.is_dir():
        return
    ttl = float(getattr(Config, "OCR_VIDEO_PARTIAL_TTL_SECONDS", 6 * 3600))
    now = time.time()
    try:
        for sub in root.iterdir():
            try:
                if sub.is_symlink() or not sub.is_dir() or sub.resolve() in _active_partial_dirs:
                    continue
                if now - sub.stat().st_mtime > ttl:
                    for child in sub.iterdir():
                        if child.is_file() and not child.is_symlink():
                            child.unlink(missing_ok=True)
                    sub.rmdir()
            except OSError:
                continue
    except OSError as exc:
        logger.warning("OCR_VIDEO_PARTIAL_CACHE scan_failed dir=%s error=%s", root, exc)


def _ocr_is_image_message(message):
    if getattr(message, "photo", None):
        return True
    document = getattr(getattr(message, "media", None), "document", None) or getattr(message, "document", None)
    mime_type = str(getattr(document, "mime_type", "") or "")
    return mime_type.startswith("image/") and not any(
        isinstance(attribute, DocumentAttributeVideo)
        for attribute in (getattr(document, "attributes", ()) or ())
    )


def _ocr_has_known_code_input_link(event):
    message = getattr(event, "message", None)
    text = " ".join((
        str(getattr(message, "text", "") or ""),
        str(getattr(message, "message", "") or ""),
        *(str(getattr(entity, "url", "") or "")
          for entity in getattr(message, "entities", None) or ()),
    )).lower()
    known_domains = {
        "xx88code.com",
        "getcodexx8bonus.pages.dev",
    }
    for url in (getattr(Config, "DOMAIN_TO_CHANNEL_URL", {}) or {}).values():
        domain = urlparse(str(url)).hostname
        if domain:
            known_domains.add(domain.lower())
    return any(domain in text for domain in known_domains)


def _ocr_channel_policy(cfg):
    domain = normalize_domain(cfg.get("url", ""))
    if domain == "xx88code.com":
        max_samples = getattr(Config, "XX88_VIDEO_MAX_SAMPLES", 20)
        max_duration = getattr(Config, "XX88_VIDEO_MAX_DURATION_SEC", 12.0)
    elif domain == "getcodexx8bonus.pages.dev":
        max_samples = getattr(Config, "XX8_VIDEO_MAX_SAMPLES", 20)
        max_duration = getattr(Config, "XX8_VIDEO_MAX_DURATION_SEC", 12.0)
    elif domain == "llwincode.com":
        max_samples = getattr(Config, "LLWIN_VIDEO_MAX_SAMPLES", 20)
        max_duration = getattr(Config, "LLWIN_VIDEO_MAX_DURATION_SEC", 12.0)
    else:
        max_samples = getattr(Config, "OCR_VIDEO_MAX_SAMPLES", 10)
        max_duration = getattr(Config, "OCR_VIDEO_MAX_DURATION_SEC", 7.0)
    return {
        "reading_policy": OCR_POLICY_VERSION,
        "anchor_sampling": (("midpoint-minus-1", "end-minus-1", .10, .90, 6)
                            if domain in ("llwincode.com", "xx88code.com") else None),
        "consensus": bool(cfg.get("ocr_require_consensus", True)),
        "strict_shape": bool(cfg.get("strict_code_shape", False)),
        "lengths": cfg.get("ocr_code_lengths"),
        "frames": cfg.get("ocr_frame_seconds"),
        "max_codes": cfg.get("max_ocr_codes_per_batch"),
        "filter_group": cfg.get("filter_group"),
        "sampling": (getattr(Config, "OCR_VIDEO_SAMPLE_SECONDS", .40),
                     max_samples, max_duration),
        "confidence": getattr(Config, "OCR_CONFIDENCE_THRESHOLD", .70),
        "streaming": (getattr(Config, "OCR_STREAMING_ENABLED", True),
                      getattr(Config, "OCR_STREAMING_SAMPLE_SECONDS", .30),
                      getattr(Config, "OCR_STREAMING_WINDOW", 12),
                      getattr(Config, "OCR_STREAMING_MARGIN", .90)),
        "xx8_lengths": tuple(getattr(Config, "XX8_OCR_CODE_LENGTHS", (8, 10))),
        "xx88_length": getattr(Config, "XX88_OCR_CODE_LENGTH", 6),
    }


def _cached_photo_ocr(event, cfg):
    """Also reuse video/document OCR before downloading forwarded media again."""
    media_id = _telegram_media_id(event)
    if not media_id:
        return None
    message = getattr(event, "message", None)
    document = getattr(getattr(message, "media", None), "document", None) or getattr(message, "document", None)
    is_video = (str(getattr(document, "mime_type", "")).startswith("video/")
                or media_id.startswith(("video:", "animation:"))
                or any(isinstance(attr, DocumentAttributeVideo) for attr in getattr(document, "attributes", ()) or ()))
    crop = cfg.get("ocr_crop")
    crops = cfg.get("ocr_crops") or ([crop] if crop else [None])
    key = _ocr_file_cache_key("", cfg.get("url", ""), crops, is_video,
                              bool(cfg.get("ocr_single_code_mode", False)), media_id,
                              _ocr_channel_policy(cfg))
    return _ocr_cache_get(key)


def _ocr_cache_get(key: str):
    now = time.monotonic()
    item = _ocr_result_cache.get(key)
    if not item:
        return None
    created, result = item
    ttl = max(1.0, float(getattr(Config, "OCR_CACHE_TTL_SECONDS", 600.0)))
    if now - created > ttl:
        _ocr_result_cache.pop(key, None)
        return None
    return {
        **result,
        "codes": [dict(code) for code in result.get("codes", [])],
        "message": "ocr_cache_hit",
    }


def _ocr_cache_put(key: str, result: dict):
    # A transient download/model failure must be retried against the real
    # pipeline, rather than replayed from cache until the TTL expires.
    if not result.get("success") and not (
        result.get("failure_kind") == "NO_CODE" and result.get("retryable") is False
    ):
        return
    max_entries = max(1, int(getattr(Config, "OCR_CACHE_MAX_ENTRIES", 128)))
    _ocr_result_cache[key] = (time.monotonic(), {
        **{k: v for k, v in result.items() if k != "early_emitted"},
        "codes": [dict(code) for code in result.get("codes", [])],
    })
    if len(_ocr_result_cache) > max_entries:
        oldest = sorted(_ocr_result_cache.items(), key=lambda item: item[1][0])
        for old_key, _ in oldest[: max(1, len(_ocr_result_cache) - max_entries)]:
            _ocr_result_cache.pop(old_key, None)


_KJC_SPECIAL_CHANNEL_IDS = Config.KJC_SPECIAL_CHANNEL_IDS
_KJC_BROADCAST_URLS = Config.KJC_BROADCAST_URLS


@lru_cache(maxsize=128)
def normalize_domain(url: str) -> str:
    p = urlparse(url or "")
    return (p.netloc or p.path).lower().replace("www.", "").strip("/")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def get_site_log_tag(target_url: str) -> str:
    d = normalize_domain(target_url)
    return _SITE_LOG_LABELS.get(d, d or "SYSTEM")


def _refresh_daily_state():
    today = _today_str()
    if bot_state._daily_date != today:
        bot_state._daily_used.clear()
        bot_state._account_exhausted_today.clear()
        bot_state._channel_account_index.clear()
        bot_state._domain_account_cursor.clear()
        bot_state._fanout_account_cursor.clear()
        bot_state._rotation_loaded_domains.clear()
        if browser_engine is not None:
            try:
                browser_engine.reset_exhausted_accounts()
            except Exception:
                pass
        bot_state._daily_date = today
        logger.info(f"🗓️ Ngày mới ({today})")


def _stateless_entry_enabled() -> bool:
    """PATCH_MARKER_2026-10-27-stateless-entry: bot chỉ nhập + submit, không theo dõi tài khoản."""
    return bool(getattr(Config, "STATELESS_ACCOUNT_ENTRY", True))


async def _mark_account_done_today(channel_key: str, username: str):
    if _stateless_entry_enabled() or uses_direct_http(channel_key):
        return
    channel_key = normalize_domain(str(channel_key).strip()).casefold()
    username = str(username or "").strip().casefold()
    if not username:
        return
    _refresh_daily_state()
    today = _today_str()
    bot_state._daily_used.add((today, channel_key, username))
    _notify_browser_exhausted(channel_key, username)
    db = _systems.get("db") if _systems else None
    if db is not None:
        marker = getattr(db, "mark_account_success_done", None)
        if marker is not None:
            await asyncio.get_running_loop().run_in_executor(
                _DB_EXECUTOR, marker, channel_key, username, today
            )


def _is_account_done_today(channel_key: str, username: str) -> bool:
    if _stateless_entry_enabled() or uses_direct_http(channel_key):
        return False
    _refresh_daily_state()
    channel_key = normalize_domain(str(channel_key).strip()).casefold()
    username = str(username or "").strip().casefold()
    return (_today_str(), channel_key, username) in bot_state._daily_used


async def _mark_account_exhausted(domain: str, username: str) -> None:
    domain = normalize_domain(str(domain or "")).casefold()
    username = str(username or "").strip().casefold()
    if not domain or not username:
        return
    _refresh_daily_state()
    today = _today_str()
    key = (today, domain, username)
    bot_state._account_exhausted_today.add(key)
    if not _stateless_entry_enabled() and not uses_direct_http(domain):
        bot_state._daily_used.add(key)
    if browser_engine is not None and not uses_direct_http(domain):
        browser_engine.mark_account_exhausted(domain, username)
    db = _systems.get("db") if _systems else None
    marker = getattr(db, "mark_account_exhausted", None) if db is not None else None
    if marker is not None:
        await asyncio.get_running_loop().run_in_executor(
            _DB_EXECUTOR, marker, domain, username, today,
        )


def _is_account_exhausted_today(domain: str, username: str) -> bool:
    _refresh_daily_state()
    key = (_today_str(), normalize_domain(str(domain or "")).casefold(),
           str(username or "").strip().casefold())
    return key in bot_state._account_exhausted_today


def _should_lock_account(domain: str, item: dict, result: str) -> bool:
    if str(result or "").upper() in {"ACCOUNT_LIMIT", "ACCOUNT_BLOCKED"}:
        return True
    if _stateless_entry_enabled():
        return False
    source = _dispatch_source_kind(item)
    return not (domain == "xx88code.com" and source == "spoiler")


def _ordered_domain_accounts(domain: str) -> list[dict]:
    """Keep configured slots stable even after an account finishes."""
    accounts = _domain_accounts.get(domain)
    if accounts is None:
        accounts = get_effective_domain_accounts(domain)
    return sorted(accounts, key=lambda a: (
        a.get("priority", 999), str(a.get("username") or ""),
    ))


def _available_domain_accounts(domain: str) -> list[dict]:
    if _stateless_entry_enabled() and not uses_direct_http(domain):
        return _ordered_domain_accounts(domain) or [{"username": "", "priority": 0}]
    if uses_direct_http(domain):
        return [account for account in _ordered_domain_accounts(domain)
                if str(account.get("username") or "").strip()
                and not _is_account_exhausted_today(domain, account.get("username"))]
    _refresh_daily_state()
    return [account for account in _ordered_domain_accounts(domain)
            if str(account.get("username") or "").strip()
            and not _is_account_done_today(domain, account["username"])
            and not _is_account_exhausted_today(domain, account["username"])]


async def _load_account_rotation(channel_key: str) -> None:
    """Restore daily successes before listing or distributing any accounts."""
    if _stateless_entry_enabled() or uses_direct_http(channel_key):
        return  # không đọc/ghi DB trạng thái tài khoản
    _refresh_daily_state()
    if channel_key in bot_state._rotation_loaded_domains:
        return
    lock = _account_rotation_locks.setdefault(channel_key, asyncio.Lock())
    async with lock:
        while True:
            _refresh_daily_state()
            if channel_key in bot_state._rotation_loaded_domains:
                return
            today = _today_str()
            db = _systems.get("db") if _systems else None
            cursor, exhausted = 0, set()
            if db is not None and hasattr(db, "get_account_rotation"):
                cursor, exhausted = await asyncio.get_running_loop().run_in_executor(
                    _DB_EXECUTOR, db.get_account_rotation, channel_key, today)
            if today != _today_str():
                continue
            bot_state._domain_account_cursor.setdefault(channel_key, cursor)
            for name in exhausted:
                username = str(name or "").strip().casefold()
                if username:
                    bot_state._daily_used.add((today, channel_key, username))
                    _notify_browser_exhausted(channel_key, username)
            bot_state._rotation_loaded_domains.add(channel_key)
            return


def _result_indicates_account_limit(message: str) -> bool:
    from submission_outcomes import is_account_limit_text
    return is_account_limit_text(message)


def _pick_account_stateless(domain: str, accounts: list, preferred_username: str | None = None):
    """Trả tài khoản để nhập. Không khóa, không đặt chỗ in-flight, không ghi DB, không chờ.

    Item đã ghim (pin_account) -> đúng tài khoản đó. Item chưa ghim (dữ liệu cũ trong inbox)
    -> vòng tròn trong RAM.
    """
    ordered = sorted(
        (a for a in (accounts or _ordered_domain_accounts(domain))
         if str(a.get("username") or "").strip()
         and not _is_account_exhausted_today(domain, a.get("username"))),
        key=lambda a: a.get("priority", 999),
    )
    if not ordered:
        return None
    if preferred_username:
        wanted = str(preferred_username).strip().casefold()
        for acc in ordered:
            if str(acc["username"]).strip().casefold() == wanted:
                return acc
    start = bot_state._fanout_account_cursor.get(domain, 0) % len(ordered)
    bot_state._fanout_account_cursor[domain] = (start + 1) % len(ordered)
    return ordered[start]


async def _get_next_available_account(
    channel_key: str, accounts: list, preferred_username: str | None = None
):
    if _stateless_entry_enabled():
        return _pick_account_stateless(channel_key, accounts, preferred_username)

    await _load_account_rotation(channel_key)
    lock = _account_rotation_locks.setdefault(channel_key, asyncio.Lock())
    async with lock:
        _refresh_daily_state()
        ordered = sorted(accounts, key=lambda a: a.get("priority", 999))
        if not ordered:
            return None

        today = _today_str()
        db = _systems.get("db") if _systems else None
        loop = asyncio.get_running_loop()

        if preferred_username:
            preferred = str(preferred_username).strip().casefold()
            for acc in ordered:
                username = str(acc.get("username") or "")
                if username.strip().casefold() != preferred:
                    continue
                if (_is_account_done_today(channel_key, username)
                        or _is_account_exhausted_today(channel_key, username)):
                    return None
                reservation_key = (channel_key, username.strip().casefold())
                if reservation_key in bot_state._inflight_accounts:
                    return None
                bot_state._inflight_accounts.add(reservation_key)
                return acc
            return None

        prefilled = _prefilled_accounts_for(channel_key)
        if prefilled:
            for pname in prefilled:
                for acc in ordered:
                    username = acc["username"]
                    if username.strip().casefold() != str(pname).strip().casefold():
                        continue
                    if (_is_account_done_today(channel_key, username)
                            or _is_account_exhausted_today(channel_key, username)):
                        continue
                    reservation_key = (channel_key, username.strip().casefold())
                    if reservation_key in bot_state._inflight_accounts:
                        continue
                    bot_state._inflight_accounts.add(reservation_key)
                    return acc

        start = bot_state._domain_account_cursor.get(
            channel_key, 0
        ) % len(ordered)
        for offset in range(len(ordered)):
            acc = ordered[(start + offset) % len(ordered)]
            username = acc["username"]
            if (_is_account_done_today(channel_key, username)
                    or _is_account_exhausted_today(channel_key, username)):
                continue
            reservation_key = (channel_key, username.strip().casefold())
            if reservation_key in bot_state._inflight_accounts:
                continue
            bot_state._inflight_accounts.add(reservation_key)
            cursor = (start + offset + 1) % len(ordered)
            bot_state._domain_account_cursor[channel_key] = cursor
            if db is not None and hasattr(db, "save_account_cursor"):
                try:
                    await loop.run_in_executor(
                        _DB_EXECUTOR, db.save_account_cursor, channel_key, cursor, today
                    )
                except BaseException:

                    bot_state._inflight_accounts.discard(reservation_key)
                    raise
            return acc
        return None


def _release_account_reservation(channel_key: str, username):
    if username:
        bot_state._inflight_accounts.discard((channel_key, str(username).strip().casefold()))


def _domain_has_unused_account(domain: str) -> bool:
    return bool(_available_domain_accounts(domain))


def _spoiler_entity_signature(message) -> str:

    signature = []
    for entity in getattr(message, "entities", None) or []:
        if not isinstance(entity, MessageEntitySpoiler):
            continue
        try:
            signature.append((int(entity.offset), int(entity.length)))
        except (AttributeError, TypeError, ValueError):
            continue
    return json.dumps(signature, separators=(",", ":")) if signature else ""


def _message_content_hash(text: str, spoiler_signature: str = "") -> str:
    payload = text or ""
    if spoiler_signature:
        payload = f"{payload}\x1fspoiler:{spoiler_signature}"
    return hashlib.md5(payload.encode("utf-8", errors="ignore")).hexdigest()


def _on_message_queue_drop(item) -> None:

    if isinstance(item, RawIngressItem):
        _raw_ingest_pending.discard(item.key)
        return
    if isinstance(item, tuple) and len(item) == 2:
        try:
            row_id = int(item[1])
            bot_state._inbox_enqueued_ids.discard(row_id)
            _inbox_message_cache.pop(row_id, None)
        except (TypeError, ValueError):
            pass


async def _retry_dropped_domain_item(
    inbox_id: int, claim_token: str | None, work_item_id: int | None = None
) -> None:

    if _durable_inbox is None:
        return
    try:
        if work_item_id:

            bot_state._work_item_enqueued_ids.discard(int(work_item_id))
            if _inbox_wakeup is not None:
                _inbox_wakeup.set()
        else:
            await asyncio.get_running_loop().run_in_executor(
                _INBOX_STATE_EXECUTOR, _durable_inbox.retry_or_fail,
                inbox_id, "domain queue overflow/drop", 5, claim_token,
            )
    except Exception as exc:
        logger.error("❌ Không thể retry inbox row=%s sau domain queue drop: %s", inbox_id, exc)


def _on_domain_queue_drop(item) -> None:

    inbox_id = item.get("inbox_id") if isinstance(item, dict) else None
    claim_token = item.get("claim_token") if isinstance(item, dict) else None
    work_item_id = item.get("work_item_id") if isinstance(item, dict) else None
    if inbox_id and _durable_inbox is not None:
        try:
            task = asyncio.create_task(
                _retry_dropped_domain_item(int(inbox_id), claim_token, work_item_id),
                name=f"retry-dropped-domain-{inbox_id}",
            )
            bot_state.bg_tasks.add(task)
            task.add_done_callback(bot_state.bg_tasks.discard)
        except (TypeError, ValueError, RuntimeError) as exc:
            logger.error("❌ Không thể schedule retry inbox row=%s sau domain queue drop: %s", inbox_id, exc)


_OCR_ONLY_CHANNEL_IDS = frozenset((-1002817093108, -1004395536862))


def _prune_site_code_seen():
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    # Có thể được gọi từ thread monitor (_emergency_ram_cleanup) trong khi event loop
    # đang ghi vào dict: duyệt trên bản chụp và dùng pop() để không lỗi khi dict đổi.
    for k, ts in list(bot_state._site_code_seen.items()):
        if now - ts > ttl:
            bot_state._site_code_seen.pop(k, None)


def _code_age_for_log(*candidates):
    """Tuổi mã (giây) từ mốc thời gian đầu tiên đọc được; None nếu không có."""
    for value in candidates:
        age = age_seconds(value)
        if age is not None:
            return age
    return None


def _fmt_age(age) -> str:
    return "?" if age is None else f"{age:.1f}"


def _prune_processed_message_hashes():
    ttl, now = 300.0, time.time()
    for k in [k for k, (_, ts) in bot_state._processed_message_hashes.items() if now - ts > ttl]:
        del bot_state._processed_message_hashes[k]


def is_site_code_duplicate(domain: str, user: str, code: str) -> bool:
    if _stateless_entry_enabled() and not uses_direct_http(domain):
        user = ""
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    k = (domain, user, normalize_code(code, domain))
    if bot_state._site_code_seen.get(k) is not None and now - bot_state._site_code_seen[k] < ttl:
        return True
    bot_state._site_code_seen[k] = now
    return False


def _clear_site_code_duplicate(domain: str, user: str, code: str) -> None:
    if _stateless_entry_enabled() and not uses_direct_http(domain):
        user = ""
    bot_state._site_code_seen.pop((domain, user, normalize_code(code, domain)), None)


_defer_budget: dict = {}


def _defer_budget_exhausted(domain: str, code: str) -> bool:
    """Giới hạn số lần/thời gian defer của một mã khi tab chờ xác minh.

    Chỉ áp dụng cho domain trong DEFER_BUDGET_DOMAINS. Trả True khi mã đã vượt
    DEFER_BUDGET_MAX_CYCLES lần hoặc DEFER_BUDGET_MAX_SECONDS giây kể từ lần defer đầu.
    """
    if domain not in getattr(Config, "DEFER_BUDGET_DOMAINS", ()):
        return False
    now = time.monotonic()
    if len(_defer_budget) > 256:
        for k in [k for k, v in _defer_budget.items() if now - v[0] > 300.0]:
            _defer_budget.pop(k, None)
    key = (domain, normalize_code(code, domain))
    entry = _defer_budget.get(key)
    if entry is None or now - entry[0] > 300.0:
        entry = [now, 0]
        _defer_budget[key] = entry
    entry[1] += 1
    max_cycles = int(getattr(Config, "DEFER_BUDGET_MAX_CYCLES", 4))
    max_seconds = float(getattr(Config, "DEFER_BUDGET_MAX_SECONDS", 30.0))
    return entry[1] > max_cycles or (now - entry[0]) > max_seconds


def _get_proc_semaphore() -> asyncio.Semaphore:
    global _proc_semaphore
    if _proc_semaphore is None:
        _proc_semaphore = asyncio.Semaphore(int(getattr(Config, "MAX_CONCURRENT_PROCESSING", 24)))
    return _proc_semaphore


def _get_submit_semaphore(domain: str = "") -> asyncio.Semaphore:
    global _submit_semaphore, _http_submit_semaphore
    if uses_direct_http(domain):
        # A browser waiting for user verification must not occupy an HTTP slot.
        # Site-specific concurrency, request rates and pacing still apply.
        if _http_submit_semaphore is None:
            _http_submit_semaphore = asyncio.Semaphore(Config.DIRECT_HTTP_MAX_CONNECTIONS)
        return _http_submit_semaphore
    if _submit_semaphore is None:
        _submit_semaphore = asyncio.Semaphore(
            max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS", 4)))
        )
    return _submit_semaphore


def _get_ocr_semaphore() -> asyncio.Semaphore:
    global _ocr_semaphore
    if _ocr_semaphore is None:
        _ocr_semaphore = OCR_SEMAPHORE
    return _ocr_semaphore


def _domain_slot_capacity(domain: str) -> int:
    if _stateless_entry_enabled():
        return 2 if domain in {"xx88code.com", "v79code.com"} else 1
    site_cap = max(1, int(getattr(Config, "DOMAIN_TAB_LIMITS", {
        "xx88code.com": 3, "v79code.com": 2,  # PATCH_MARKER_2026-10-25-v5-xx88-tabs-queue
    }).get(domain, 1)))
    site_cap = max(1, int(getattr(Config, "PARALLEL_DOMAIN_TAB_LIMITS", {}).get(domain, site_cap)))
    site_cap = min(site_cap, 2 if domain in {"xx88code.com", "v79code.com"} else 1)
    if uses_direct_http(domain):
        return min(site_cap, Config.DIRECT_HTTP_MAX_CONNECTIONS,
                   max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS_PER_DOMAIN", 3))))
    limit = max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS_PER_DOMAIN", 3)))
    profile = get_site_profile(domain)
    if profile is not None:
        limit = min(limit, max(1, int(profile.tab_slots)))
    return min(limit, site_cap)


def _domain_slot_limit(domain: str) -> int:
    if not is_site_business_hours_open(domain):
        return 0
    capacity = _domain_slot_capacity(domain)
    if _stateless_entry_enabled():
        # Số luồng = số tab đang mở (tính cả tab đang bận), tối đa bằng số tab cố định.
        # Không dùng available_user_tab_count: nó chỉ đếm tab RẢNH nên worker #2 bị
        # đẩy ngủ ngay khi worker #1 đang giữ tab -> mất song song.
        if not uses_direct_http(domain) and _BROWSER_ENGINE_OK:
            return min(capacity, browser_engine.live_user_tab_count(domain))
        return capacity
    available_accounts = len(_available_domain_accounts(domain))
    active = min(capacity, available_accounts)
    if not uses_direct_http(domain) and _BROWSER_ENGINE_OK:
        active = min(active, browser_engine.available_user_tab_count(domain))
    return active


class _TokenBucket:
    def __init__(self, rpm: float, burst: float):
        self.rate_per_sec = max(float(rpm), 1.0) / 60.0
        self.capacity = max(float(burst), 1.0)
        self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.last_refill) * self.rate_per_sec)
                self.last_refill = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait = (1.0 - self.tokens) / self.rate_per_sec
            await asyncio.sleep(wait)


def get_domain_rate_limiter(domain: str) -> _TokenBucket:
    if domain not in _domain_rate_limiters:
        limits = getattr(Config, "DOMAIN_RATE_LIMITS", {}).get(domain)
        if limits is None:
            limits = (
                getattr(Config, "REQUESTS_PER_MINUTE", 30),
                getattr(Config, "MAX_BURST", 5),
            )
        rpm, burst = limits
        _domain_rate_limiters[domain] = _TokenBucket(float(rpm), float(burst))
    return _domain_rate_limiters[domain]


def get_domain_semaphore(domain: str) -> asyncio.Semaphore:
    if domain not in _domain_semaphores:
        limit = _domain_slot_capacity(domain)
        _domain_semaphores[domain] = asyncio.Semaphore(limit)
    return _domain_semaphores[domain]


CODE_HISTORY_DIR = Path("logs/code_history")
CODE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
_HISTORY_FIELDS = [
    "time", "event_type", "channel", "site", "account", "code", "source",
    "status", "telegram_delay", "submit_elapsed", "message", "screenshot",
]


def _write_history_rows(rows: list[dict]):
    if not rows:
        return
    try:
        csv_p = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        jsonl_p = CODE_HISTORY_DIR / f"code_history_{_today_str()}.jsonl"
        header = not csv_p.exists()
        with csv_p.open("a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=_HISTORY_FIELDS)
            if header:
                w.writeheader()
            w.writerows(rows)
        with jsonl_p.open("a", encoding="utf-8") as f:
            f.writelines(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    except Exception as e:
        logger.exception("History batch write failed")


async def _history_writer_loop():
    while True:
        rows = []
        try:
            row = await _history_queue.get()
            if row is None:
                _history_queue.task_done()
                break
            rows.append(row)

            for _ in range(31):
                try:
                    extra = _history_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if extra is None:
                    await _history_queue.put(None)
                    break
                rows.append(extra)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(_DB_EXECUTOR, _write_history_rows, rows)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("History writer failed")
        finally:
            for _ in rows:
                try:
                    _history_queue.task_done()
                except Exception:
                    break


def start_history_writer():
    global _history_queue, _history_writer_task
    if _history_queue is None:
        _history_queue = asyncio.Queue(maxsize=2000)
    if _history_writer_task is not None and not _history_writer_task.done():
        return _history_writer_task
    _history_writer_task = asyncio.create_task(_history_writer_loop())
    return _history_writer_task


def append_code_history(event_type, code="", target_url="", account="", channel="", source="", status="", telegram_delay=None, submit_elapsed=None, message="", screenshot=""):
    try:
        row = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": event_type,
            "channel": channel or "",
            "site": normalize_domain(target_url),
            "account": account or "",
            "code": str(code or ""),
            "source": source or "",
            "status": status or "",
            "telegram_delay": "" if telegram_delay is None else f"{float(telegram_delay):.2f}",
            "submit_elapsed": "" if submit_elapsed is None else f"{float(submit_elapsed):.2f}",
            "message": str(message or "").replace("\n", " ")[:300],
            "screenshot": str(screenshot or ""),
        }
        try:
            _history_queue.put_nowait(row)
        except asyncio.QueueFull:
            logger.error("History queue full; dropping record type=%s", event_type)
        return row
    except Exception as e:
        logger.exception("Could not enqueue history record")
        return None


def build_daily_summary():
    try:
        csv_p = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        if not csv_p.exists():
            return None
        summary = {}
        with csv_p.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("event_type") != "FINAL_RESULT":
                    continue
                k = (row.get("site", ""), row.get("account", ""))
                summary.setdefault(k, {"SUCCESS": 0, "FAILED": 0, "UNKNOWN": 0})
                status = str(row.get("status") or "").upper()
                bucket = (
                    "SUCCESS" if status.startswith("SUCCESS")
                    else "FAILED" if status == "FAILED"
                    else "UNKNOWN"
                )
                summary[k][bucket] += 1
        out = CODE_HISTORY_DIR / f"daily_summary_{_today_str()}.csv"
        with out.open("w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=["date", "site", "account", "success", "failed", "unknown", "total"])
            w.writeheader()
            for (site, acc), c in sorted(summary.items()):
                s, fa, u = c.get("SUCCESS", 0), c.get("FAILED", 0), c.get("UNKNOWN", 0)
                w.writerow({"date": _today_str(), "site": site, "account": acc, "success": s, "failed": fa, "unknown": u, "total": s + fa + u})
        return str(out)
    except Exception as e:
        logger.warning(f"⚠️ daily_summary: {e}")
        return None


def measure_telegram_delay_fast(msg_ts):
    try:
        if msg_ts.tzinfo is None:
            msg_ts = msg_ts.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - msg_ts).total_seconds()
    except Exception:
        return None


def build_domain_accounts_map() -> dict:
    domains = {
        normalize_domain(cfg.get("url", ""))
        for cfg in Config.CHANNEL_CONFIG.values()
        if cfg.get("url")
    }
    domains.update(
        normalize_domain(domain)
        for domain in getattr(Config, "DOMAIN_ACCOUNT_OVERRIDES", {})
    )
    result = {}
    for domain in domains:
        if not domain or not is_domain_enabled(domain):
            continue
        accounts = get_effective_domain_accounts(domain)
        if accounts:
            result[domain] = accounts
    return result


def get_domain_queue(domain: str) -> asyncio.Queue:
    if domain not in _domain_queues:
        # Preserve each site's overflow/recovery policy; always draw randomly.
        if (scheduling.freshest_first_enabled(domain)
                or domain in ("xx88code.com", "getcodexx8bonus.pages.dev", "llwincode.com")):
            queue_type = RandomBoundedCodeQueue
        else:
            queue_type = RandomCodeQueue
        q = queue_type(maxsize=int(getattr(Config, "DOMAIN_QUEUE_MAXSIZE", 500)))
        logger.info("[Queue] %s selection=random queue=%s", domain, queue_type.__name__)
        _domain_queues[domain] = q
        qm = get_queue_manager()
        if qm is not None:
            qm.register(f"domain:{domain}", q, on_drop=_on_domain_queue_drop)
    return _domain_queues[domain]


async def _enqueue_domain_item(domain, item: dict | None = None) -> bool:
    if item is None:
        item = domain
        domain = str(item.get("domain") or normalize_domain(item.get("target_url", "")))
    source = _dispatch_source_kind(item)
    item["source"] = source
    item["source_kind"] = source
    item["domain"] = str(domain or item.get("domain") or normalize_domain(item.get("target_url", "")))
    domain = item["domain"]
    if not domain:
        return False
    q = get_domain_queue(domain)
    work_item_id = item.get("work_item_id")
    if work_item_id:

        item["_queued_perf"] = time.perf_counter()
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            bot_state._work_item_enqueued_ids.discard(int(work_item_id))
            if _inbox_wakeup is not None:
                _inbox_wakeup.set()
            logger.warning(
                "⚠️ [%s] domain queue đầy (%s/%s) — giữ work_item=%s trong DurableInbox",
                domain, q.qsize(), q.maxsize, work_item_id,
            )
            return False
        bot_state._work_item_enqueued_ids.add(int(work_item_id))
        return True

    item["_queued_perf"] = time.perf_counter()
    await q.put(item)
    return True


async def _next_domain_dispatch_item(domain: str, queue):
    """Draw XX88 independently; retain pairing of already-queued V79 codes."""
    item = await queue.get()
    group = None
    if (_stateless_entry_enabled() and not uses_direct_http(domain)
            and domain == "v79code.com"):
        previous = _tab_sync_pending.pop(domain, None)
        code = normalize_code(item["code"], domain)
        if previous is not None:
            if not previous["event"].is_set() and previous["first_code"] != code:
                group = previous
            else:
                previous["event"].set()
        if (group is None and queue.qsize() > 0 and _BROWSER_ENGINE_OK
                and browser_engine.available_user_tab_count(domain) >= 2):
            group = {"expected": 2, "ready": set(), "event": asyncio.Event(),
                     "timeout": max(5.0, float(getattr(Config, "SUBMIT_TIMEOUT_SECONDS", 45.0))) - 1.0,
                     "first_code": code}
            _tab_sync_pending[domain] = group
    return item, group


def _fanout_code_item(
    item: dict, count: int, *, start_index: int = 0, account_count: int | None = None
) -> list[dict]:

    count = max(1, int(count))
    total = max(count, int(account_count or count))
    return [
        {
            **item,
            "fanout": count > 1,
            "fanout_index": (int(start_index) + slot) % total,
        }
        for slot in range(count)
    ]


def _build_stateless_entry_items(items: list[dict]) -> list[dict]:
    """One work item per code; HTTP account fallback happens sequentially."""
    grouped: dict[str, list[dict]] = {}
    for item in items:
        grouped.setdefault(str(item.get("domain") or ""), []).append(item)
    result: list[dict] = []
    for domain, batch in grouped.items():
        seen = set()
        for item in batch:
            code = normalize_code(item.get("code", ""), domain)
            if code and code not in seen:
                seen.add(code)
                source_kind = _dispatch_source_kind(item)
                result.append({**item, "source": source_kind, "source_kind": source_kind,
                               "fanout": False, "pin_account": False})
    return result


def _build_distinct_code_items(
    items: list[dict], domain: str | None = None, *, single_spoiler_shared: bool = False,
    source: str | None = None,
) -> list[dict]:

    if not items:
        return []
    if source:
        items = [{**item, "source": source, "source_kind": source} for item in items]
    if domain:
        items = [{**item, "domain": domain} for item in items]
    if _stateless_entry_enabled():
        return _build_stateless_entry_items(items)
    result = []
    grouped = {}
    for item in items:
        grouped.setdefault(str(item.get("domain") or ""), []).append(item)
    for domain, batch in grouped.items():
        if uses_direct_http(domain):
            seen = set()
            for item in batch:
                code = normalize_code(item.get("code", ""), domain)
                if code and code not in seen:
                    seen.add(code)
                    result.append({**item, "fanout": False, "pin_account": False})
            continue
        accounts = _ordered_domain_accounts(domain)
        account_count = len(accounts)
        eligible_slots = [index for index, account in enumerate(accounts)
                          if str(account.get("username") or "").strip()
                          and not _is_account_done_today(domain, account["username"])
                          and not _is_account_exhausted_today(domain, account["username"])]
        if not eligible_slots:
            continue
        if single_spoiler_shared and len({normalize_code(x.get("code", ""), domain) for x in batch}) == 1:
            limit = max(1, _domain_slot_limit(domain))
            count = 1 if domain == "livemm88.net" else min(len(eligible_slots), limit)
            rotating_two_tab_site = domain in {"xx88code.com", "livemm88.net"}
            start = (bot_state._fanout_account_cursor.get(domain, 0) % account_count) if rotating_two_tab_site else 0
            slots = sorted(eligible_slots, key=lambda index: (index - start) % account_count)[:count]
            for x in batch[:1]:
                copies = _fanout_code_item(x, count, account_count=account_count)
                for copy, slot in zip(copies, slots):
                    copy.update(fanout_index=slot, pin_account=True)
                result.extend(copies)
            if rotating_two_tab_site:
                bot_state._fanout_account_cursor[domain] = (slots[-1] + 1) % account_count
        else:
            start = bot_state._fanout_account_cursor.get(domain, 0) % account_count
            slots = sorted(eligible_slots, key=lambda index: (index - start) % account_count)
            for ix, x in enumerate(batch):
                copy = dict(x)
                slot = slots[ix % len(slots)]
                copy.update(fanout=False, fanout_index=slot, pin_account=True)
                result.append(copy)
            bot_state._fanout_account_cursor[domain] = (slot + 1) % account_count
    return result


def _build_spoiler_dispatch_items(
    items: list[dict], domain: str | None = None, spoiler_codes=None,
) -> list[dict]:
    if spoiler_codes is not None:
        template = items[0] if items else {"domain": domain or ""}
        items = [{**template, "code": code} for code in spoiler_codes]
    return _build_distinct_code_items(
        [{**item, "source": "spoiler", "source_kind": "spoiler"} for item in items],
        domain, single_spoiler_shared=True, source="spoiler")


def _dispatch_source_kind(item, durable_item=None):
    source = str(item.get("source_kind") or (durable_item or {}).get("source_kind")
                 or item.get("source") or "text").lower()
    if source in {"spoiler", "kjc_spoiler"}:
        return "spoiler"
    if source in {"ocr", "image_ocr", "image-ocr"}:
        return "ocr"
    return "text"


def _record_spoiler_no_lock(user, *, late=False):
    _async_ingest_counts["SPOILER_NO_LOCK"] += 1
    logger.info("SPOILER_NO_LOCK domain=xx88 user=%s late=%s total=%s", user, late,
                _async_ingest_counts["SPOILER_NO_LOCK"])


async def _submit_code_with_account_retries(
    account,
    code,
    target_url,
    domain,
    source_kind: str | None = None,
    defer_failure_mark: bool = False,
) -> str:
    user = account["username"]
    tab_only = bool(account.get("_tab_only"))
    if source_kind == "kjc_spoiler":
        source_kind = "spoiler"

    single_attempt = tab_only or bool(account.get("_http_sequential"))
    max_r = 1 if single_attempt else max(1, int(getattr(Config, "MAX_RETRIES_PER_ACCOUNT", 1)))
    retry = False if single_attempt else bool(getattr(Config, "RETRY_ON_TIMEOUT", True))

    captcha_retries_left = (
        max(0, int(getattr(Config, "XX88_CAPTCHA_RETRIES", 1)))
        if domain == "xx88code.com" and not single_attempt
        else 0
    )
    normal_attempts = 0
    attempt = 0
    while True:
        attempt += 1
        submit_options = (
            {"tab_only": True, "sync_group": account.get("_sync_group")}
            if tab_only else {}
        )
        result = await submit_code_with_delay(user, code, target_url, _systems, **submit_options) or {}

        if tab_only and result.get("account"):
            user = account["username"] = str(result["account"])

        success = bool(result.get("success", False))
        infra_failure = bool(result.get("_infra_failure", False))
        has_pts = bool(result.get("has_points", False))
        wrong = bool(result.get("is_wrong_code", False)) or str(result.get("status") or "").upper() == "FAILED"
        blocked = bool(result.get("is_account_blocked", False))
        rate_limited = bool(result.get("rate_limited", False))
        retry_after = max(0.1, float(result.get("retry_after_seconds", getattr(Config, "RATE_LIMIT_BACKOFF_SECONDS", 5.0)) or 5.0))

        msg = str(
            result.get("message", "")
            or ""
        )[:120]

        result_code = str(
            result.get("code", "")
            or ""
        ).upper().strip()

        msg_upper = msg.upper()
        failure_kind = str(result.get("failure_kind", "") or "").upper()

        if failure_kind == "TAB_UNAVAILABLE":
            append_code_history(
                event_type="SUBMIT_ATTEMPT", code=code, target_url=target_url,
                account=user, status="TAB_UNAVAILABLE", message=msg,
            )
            return "TAB_UNAVAILABLE"

        if failure_kind == "UNKNOWN_AFTER_SUBMIT" or str(result.get("status") or "").upper() in {"NO_RESULT", "AMBIGUOUS"}:

            append_code_history(
                event_type="FINAL_RESULT", code=code, target_url=target_url,
                account=user, status="REVIEW_REQUIRED", message=msg or "No decisive response",
            )
            return "REVIEW_REQUIRED"

        if failure_kind == "BROWSER_VERIFICATION_WAIT":
            return "BROWSER_VERIFICATION_WAIT"

        if failure_kind == "TOKEN_NOT_READY" or result.get("status") == "TOKEN_NOT_READY":
            append_code_history(
                event_type="SUBMIT_ATTEMPT", code=code, target_url=target_url,
                account=user, status="TOKEN_NOT_READY", message=msg,
            )
            return "TOKEN_NOT_READY"

        if failure_kind == "PENDING_VERIFICATION" or result.get("status") == "PENDING_VERIFICATION":
            logger.warning(
                "⏸️ [%s] %s — PENDING_VERIFICATION, giữ nguyên page và kết thúc item để nhả worker",
                domain, msg or "Turnstile verification pending",
            )
            append_code_history(
                event_type="SUBMIT_ATTEMPT", code=code, target_url=target_url,
                account=user, status="PENDING_VERIFICATION", message=msg,
            )
            return "PENDING_VERIFICATION"

        if failure_kind == "CAPTCHA_EXPIRED" or result_code == "CAPTCHA_EXPIRED":

            append_code_history(
                event_type="SUBMIT_ATTEMPT", code=code, target_url=target_url,
                account=user, status="CAPTCHA_EXPIRED", message=msg,
            )
            if retry and captcha_retries_left > 0:
                captcha_retries_left -= 1
                logger.warning(
                    "⌛ [%s] %s — captcha/token không hợp lệ, reload đã xong; retry xác minh còn=%s",
                    domain, code, captcha_retries_left,
                )
                await asyncio.sleep(0.25)
                continue
            logger.warning("⌛ [%s] %s — captcha vẫn lỗi sau retry riêng → chờ xác minh lại", domain, code)
            return "PENDING_VERIFICATION"

        if failure_kind == "SITE_UI_CHANGED":
            logger.error("🧩 [%s] %s — SITE_UI_CHANGED, không retry mù", domain, msg)
            append_code_history(
                event_type="SUBMIT_ATTEMPT", code=code, target_url=target_url,
                account=user, status="SITE_UI_CHANGED", message=msg,
            )
            return "SITE_UI_CHANGED"

        if rate_limited or result.get("browser_kind") == "RATE_LIMITED" or failure_kind == "RATE_LIMITED":
            # Một phản hồi throttle ở BẤT KỲ site nào đều đóng cổng cho mọi
            # worker của site đó. Không đánh đồng business throttle với UNKNOWN.
            configured_base, configured_max = getattr(
                Config, "SITE_RATE_LIMIT_COOLDOWNS", {}
            ).get(domain, (getattr(Config, "RATE_LIMIT_BACKOFF_SECONDS", 5.0), 60.0))
            base_delay = max(1.0, float(configured_base), retry_after)
            max_delay = max(base_delay, float(configured_max))
            delay = await submit_pacer.throttle(
                domain, user, initial=base_delay, maximum=max_delay,
            )
            append_code_history(
                event_type="SUBMIT_ATTEMPT", code=code, target_url=target_url,
                account=user, status="RATE_LIMITED",
                message=f"cooldown={delay:.1f}s {msg}".strip(),
            )
            _sent_n, _gap_s = submit_pacer.last_throttle_stats(domain)
            logger.warning(
                "🚫 [Submit-Pacer] domain=%s account=%s code=%s cooldown=%.1fs; "
                "website yêu cầu giảm nhịp gửi | sent_since_last_throttle=%s "
                "since_last_throttle=%s",
                domain, user, code, delay, _sent_n,
                "n/a" if _gap_s is None else f"{_gap_s:.1f}s",
            )
            return "RATE_LIMITED"

        if infra_failure and not result.get("keep_page") and "CLOUDFLARE" not in msg_upper and "TURNSTILE" not in msg_upper:
            normal_attempts += 1
            if retry and normal_attempts < max_r:
                logger.warning(
                    "🔁 [%s] %s — browser infrastructure retry %s/%s — %s",
                    domain, code, normal_attempts, max_r, msg or "tab unavailable",
                )
                await asyncio.sleep(min(0.25 * normal_attempts, 1.0))
                continue
            append_code_history(
                event_type="SUBMIT_ATTEMPT",
                code=code,
                target_url=target_url,
                account=user,
                status="INFRA_FAILURE",
                message=msg or "browser infrastructure unavailable",
            )
            return "INFRA_FAILURE"

        if result.get("keep_page") or "CLOUDFLARE" in msg_upper or "TURNSTILE" in msg_upper:
            logger.warning(
                "⏸️ [%s] %s — giữ nguyên page, không retry captcha",
                domain,
                msg or "Cloudflare verification pending",
            )
            return "FAILED"

        if (not success) and (
            blocked or bool(result.get("account_limit")) or _result_indicates_account_limit(msg_upper)
        ):
            append_code_history(
                event_type="SUBMIT_ATTEMPT",
                code=code,
                target_url=target_url,
                account=user,
                status="ACCOUNT_LIMIT",
                message=msg or result_code or "Account limit reached",
            )
            logger.warning("↩️ [%s] %s báo đạt giới hạn từ site", domain, user)
            return "ACCOUNT_LIMIT"

        terminal_failure = (
            result_code in {
                "CODE_NOT_USED",
                "CODE_USED",
                "INVALID_CODE",
            }
            or "CODE ĐÃ ĐƯỢC SỬ DỤNG" in msg_upper
            or "MÃ ĐÃ ĐƯỢC SỬ DỤNG" in msg_upper
            or "CODE NOT USED" in msg_upper
        )

        if terminal_failure:
            await submit_pacer.accepted_response(domain)
            logger.warning(
                "⏭️ [%s] %s — %s — không retry",
                domain,
                code,
                msg or result_code or "terminal failure",
            )

            return "FAILED"

        # Phản hồi nghiệp vụ rõ ràng giúp reset hệ số backoff domain.
        if success or wrong:
            await submit_pacer.accepted_response(domain)

        if success and has_pts:

            append_code_history(
                event_type="FINAL_RESULT",
                code=code,
                target_url=target_url,
                account=user,
                status="SUCCESS_POINTS",
                message="OK có điểm",
            )

            return "SUCCESS_POINTS"

        if success and not has_pts:

            append_code_history(
                event_type="FINAL_RESULT",
                code=code,
                target_url=target_url,
                account=user,
                status="SUCCESS_NO_POINTS",
                message="OK không điểm",
            )

            return "SUCCESS_NO_POINTS"

        if wrong:

            append_code_history(
                event_type="SUBMIT_ATTEMPT",
                code=code,
                target_url=target_url,
                account=user,
                status="FAILED",
                message=msg,
            )

            return "FAILED"

        normal_attempts += 1
        if retry and normal_attempts < max_r:
            logger.warning(
                "🔁 [%s] %s — retry %s/%s — %s",
                domain,
                code,
                normal_attempts,
                max_r,
                msg or "temporary error",
            )

            await asyncio.sleep(
                min(
                    0.3 * normal_attempts,
                    1.0,
                )
            )

            continue

        append_code_history(
            event_type="FINAL_RESULT",
            code=code,
            target_url=target_url,
            account=user,
            status="NO_RESULT",
            message=(
                f"NO_RESULT sau {attempt} lần"
                f"{': ' + msg if msg else ''}"
            ),
        )

        return "NO_RESULT"


async def _submit_http_accounts_sequential(
    code, target_url, domain, *, source_kind=None, preferred_username=None,
):
    """Try configured HTTP accounts, advancing only after a decisive code failure."""
    accounts = [dict(account, _http_sequential=True) for account in _ordered_domain_accounts(domain)
                if str(account.get("username") or "").strip()
                and not _is_account_exhausted_today(domain, account.get("username"))]
    if preferred_username:
        preferred = str(preferred_username).strip().casefold()
        accounts.sort(key=lambda account: str(account["username"]).strip().casefold() != preferred)
    if not accounts:
        return "NO_ACCOUNT", {"username": "", "_http_sequential": True}
    # PATCH_MARKER_2026-11-01-xx8-rotate: XX8 1 ma 1 tai khoan (user chot
    # 01/11). Moi ma bat dau tu tai khoan ke tiep trong vong
    # kaoboy012 -> conve99sau -> minichan011 -> kaoboy012... tren danh
    # sach tai khoan con kha dung (da loai tai khoan het luot). Chi ap
    # cho getcode; domain HTTP khac (neu co) giu nguyen thu tu uu tien.
    if domain == "getcodexx8bonus.pages.dev" and len(accounts) > 1:
        start = _xx8_rotate_cursor.get(domain, 0) % len(accounts)
        _xx8_rotate_cursor[domain] = start + 1
        accounts = accounts[start:] + accounts[:start]
        logger.info("[XX8-ROTATE] code=%s bat dau tu %s", code, accounts[0]["username"])
    attempted = False
    all_limited = True
    last_account = None
    for account in accounts:
        username = str(account["username"]).strip().casefold()
        reservation_key = (domain, username)
        if reservation_key in bot_state._inflight_accounts:
            continue
        bot_state._inflight_accounts.add(reservation_key)
        attempted = True
        last_account = account
        try:
            status = await _submit_code_with_account_retries(
                account, code, target_url, domain, source_kind=source_kind,
                defer_failure_mark=True,
            )
        finally:
            bot_state._inflight_accounts.discard(reservation_key)
        if status == "ACCOUNT_LIMIT":
            await _mark_account_exhausted(domain, account["username"])
            continue
        all_limited = False
        # PATCH_MARKER_2026-11-01-xx8-rotate: FAILED cua tai khoan duoc
        # ghim la quyet dinh cuoi cho ma nay (ma loi thi loi voi moi
        # tai khoan), khong roi sang tai khoan ke tiep nhu truoc.
        # ACCOUNT_LIMIT o tren van roi sang tai khoan ke tiep.
        if domain == "getcodexx8bonus.pages.dev" and status == "FAILED":
            return status, account
        if status != "FAILED":
            return status, account
    if not attempted:
        return "INFRA_FAILURE", {"username": "", "_http_sequential": True}
    if all_limited:
        return "ACCOUNT_LIMIT", last_account
    return "FAILED", last_account


def _fanout_preferred_username(domain: str, item: dict) -> str | None:
    if not item.get("fanout") and not item.get("pin_account"):
        return None
    accounts = _ordered_domain_accounts(domain)
    if not accounts:
        return None

    try:
        index = int(item.get("fanout_index", 0))
    except (TypeError, ValueError):
        index = 0
    if index < 0 or index >= len(accounts):
        return None
    username = str(accounts[index].get("username") or "").strip()
    return username or None


async def domain_code_worker(domain: str, target_url: str, worker_id: int = 1):
    queue = get_domain_queue(domain)
    site_tag = get_site_log_tag(target_url)
    log_tok = set_log_context(site_tag)
    try:
        logger.info(f"👷 [Domain-Worker#{worker_id}] '{domain}' ready")
        while bot_state.is_running:
            active_slots = _domain_slot_limit(domain)
            if worker_id > active_slots:
                await asyncio.sleep(0.5 if is_site_business_hours_open(domain) else 10.0)
                continue
            if _BROWSER_ENGINE_OK:
                pause_remaining = browser_engine.domain_dispatch_pause_remaining(domain)
                if pause_remaining > 0:
                    await asyncio.sleep(min(0.5, pause_remaining))
                    continue
            # Pull-when-ready: mọi tab đang chờ xác minh thì không lấy mã (lấy ra rồi hoãn chỉ
            # đốt lượt worker và làm mã già thêm); chờ sự kiện "tab sẵn sàng" rồi kiểm tra lại.
            if _BROWSER_ENGINE_OK and await scheduling.hold_until_verification_ready(
                domain, browser_engine, float(getattr(Config, "VERIFICATION_GATED_DEFER_SECONDS", 0.3))
            ):
                continue
            try:
                item, sync_group = await asyncio.wait_for(_next_domain_dispatch_item(domain, queue), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            code = item["code"]
            queue_started = item.get("_queued_perf")
            if queue_started is not None:
                queue_wait_ms = max(0.0, (time.perf_counter() - float(queue_started)) * 1000.0)
                if queue_wait_ms >= float(getattr(Config, "DOMAIN_QUEUE_SLOW_WARN_MS", 500.0)):
                    logger.warning(
                        "🐌 [Domain-Queue] site=%s code=%s wait=%.0fms pending=%s workers=%s",
                        domain, code, queue_wait_ms, queue.qsize(), _domain_slot_limit(domain),
                    )
            inbox_id = item.get("inbox_id")
            claim_token = item.get("claim_token")
            work_item_id = item.get("work_item_id")
            durable_item = None
            work_claim_token = None
            item_target_url = item.get("target_url") or target_url
            item_domain = str(item.get("domain") or normalize_domain(item_target_url))

            item_tok = set_log_context(site_tag)
            account = None
            current_task = asyncio.current_task()
            try:
                if current_task is not None:
                    _active_submit_tasks.add(current_task)

                if work_item_id:
                    bot_state._work_item_enqueued_ids.discard(int(work_item_id))
                if not is_domain_enabled(item_domain):
                    logger.info("⏸️ [%s] Site đã tắt — bỏ item RAM, giữ dữ liệu bền", item_domain)
                    continue
                if work_item_id and _durable_inbox is not None:
                    durable_item = await asyncio.get_running_loop().run_in_executor(
                        _INBOX_STATE_EXECUTOR,
                        _durable_inbox.claim_work_item,
                        int(work_item_id),
                    )
                    if not durable_item:
                        continue
                    work_claim_token = durable_item["claim_token"]
                    code = durable_item["code"]
                    item["code"] = code
                    item["target_url"] = durable_item["target_url"]
                    item["domain"] = durable_item["domain"]
                    item_target_url = item["target_url"] or target_url
                    item_domain = str(durable_item.get("domain") or item.get("domain") or normalize_domain(item_target_url))

                # Tuổi tối đa theo domain (XX8_CODE_MAX_AGE_SECONDS, ...); không khai báo
                # thì dùng CODE_MAX_AGE_SECONDS. Mã chờ hàng đợi quá lâu thường đã hết hạn.
                _max_age = _dispatch_max_age(item, item_domain)
                db = _systems.get("db") if _systems else None
                if db is not None:
                    try:
                        loop = asyncio.get_running_loop()

                        if not item.get("fanout") and await loop.run_in_executor(_DB_EXECUTOR, db.is_code_used, item_domain, code):
                            if work_item_id and _durable_inbox is not None:
                                await loop.run_in_executor(
                                    _INBOX_STATE_EXECUTOR,
                                    _durable_inbox.complete_work_item,
                                    int(work_item_id), work_claim_token,
                                )
                            elif inbox_id and _durable_inbox is not None:
                                await loop.run_in_executor(
                                    _INBOX_STATE_EXECUTOR,
                                    _durable_inbox.complete_item,
                                    int(inbox_id),
                                    claim_token,
                                )
                            continue
                    except Exception:
                        logger.warning(
                            "⚠️ [%s] lỗi khi đánh dấu item trùng hoàn tất (work_item=%s)",
                            item_domain, work_item_id, exc_info=True,
                        )

                _message_timestamp = (
                    item.get("msg_ts") or item.get("message_date")
                    or (durable_item or {}).get("message_date")
                )
                _queue_age = age_seconds(_message_timestamp)
                if _max_age > 0 and is_stale(_message_timestamp, _max_age):
                    if work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.complete_work_item,
                            int(work_item_id), work_claim_token,
                        )
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR, _durable_inbox.complete_item,
                            int(inbox_id), claim_token,
                        )
                    _async_ingest_counts["queue_drop_stale"] += 1
                    append_code_history(
                        event_type="FINAL_RESULT", code=code, target_url=item_target_url,
                        account="", status="EXPIRED_IN_QUEUE",
                        message=f"message age={_queue_age:.1f}s exceeds max_age={_max_age:.1f}s",
                    )
                    logger.warning(
                        "QUEUE_DROP_STALE code=%s age=%.1f max_age=%.1f",
                        code, _queue_age, _max_age,
                    )
                    continue
                tab_only = _stateless_entry_enabled() and not uses_direct_http(item_domain)
                http_sequential = uses_direct_http(item_domain)
                preferred_username = None if (tab_only or http_sequential) else _fanout_preferred_username(item_domain, item)
                if tab_only:
                    account = {"username": "", "_tab_only": True, "_sync_group": sync_group}
                elif http_sequential:
                    configured = [a for a in _ordered_domain_accounts(item_domain)
                                  if str(a.get("username") or "").strip()]
                    picked = (_pick_account_stateless(item_domain, configured)
                              if _stateless_entry_enabled() else (configured[0] if configured else None))
                    account = dict(picked, _http_sequential=True) if picked else None
                else:
                    account = await _get_next_available_account(
                        item_domain, _domain_accounts.get(item_domain, []),
                        preferred_username=preferred_username)
                if (
                    account is None
                    and preferred_username
                    and item.get("pin_account")
                    and (_is_account_done_today(item_domain, preferred_username)
                         or _is_account_exhausted_today(item_domain, preferred_username))
                ):

                    account = await _get_next_available_account(
                        item_domain,
                        _domain_accounts.get(item_domain, []),
                    )
                if account is None:
                    append_code_history(event_type="FINAL_RESULT", code=code, target_url=item_target_url, account="", status="NO_ACCOUNT", message="Hết tài khoản")
                    if preferred_username and _is_account_done_today(item_domain, preferred_username):
                        account_temporarily_busy = False
                    else:
                        account_temporarily_busy = _domain_has_unused_account(item_domain)
                    if account_temporarily_busy:
                        if work_item_id and _durable_inbox is not None:
                            await asyncio.get_running_loop().run_in_executor(
                                _INBOX_STATE_EXECUTOR,
                                _durable_inbox.defer_work_item,
                                int(work_item_id),
                                "all accounts temporarily busy",
                                max(0.1, float(getattr(Config, "DISPATCH_RETRY_DELAY_SECONDS", 1.0))), work_claim_token,
                            )
                        elif inbox_id and _durable_inbox is not None:
                            await asyncio.get_running_loop().run_in_executor(
                                _INBOX_EXECUTOR,
                                _durable_inbox.retry_or_fail,
                                int(inbox_id),
                                "all accounts temporarily busy",
                                max(0.1, float(getattr(Config, "DISPATCH_RETRY_DELAY_SECONDS", 1.0))),
                                claim_token,
                            )
                    elif work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.complete_work_item,
                            int(work_item_id), work_claim_token,
                        )
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR,
                            _durable_inbox.complete_item,
                            int(inbox_id),
                            claim_token,
                        )
                    warn = logger.warning
                    if account_temporarily_busy:
                        def warn(message, *args):
                            return warning_once(
                                ("account-defer", code, str(preferred_username or "").casefold()),
                                message, *args,
                            )
                    warn(
                        "⏭️ [%s#%s] %s %s: %s",
                        item_domain,
                        worker_id,
                        "tạm hoãn" if account_temporarily_busy else "bỏ",
                        code,
                        (f"account {preferred_username} đang bận" if preferred_username else "tài khoản đang bận")
                        if account_temporarily_busy
                        else (f"account {preferred_username} đã SUCCESS hôm nay" if preferred_username else "tất cả tài khoản đã SUCCESS hôm nay"),
                    )
                    continue

                if is_site_code_duplicate(item_domain, account["username"], code):
                    logger.info(
                        "⏭️ [%s#%s] duplicate domain-user-code: %s/%s/%s",
                        item_domain,
                        worker_id,
                        item_domain,
                        account["username"],
                        code,
                    )
                    if work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.complete_work_item,
                            int(work_item_id), work_claim_token,
                        )
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.complete_item,
                            int(inbox_id),
                            claim_token,
                        )
                    continue

                logger.info(f"⌨️ Đang nhập | {code} → {account['username']}")
                # Số liệu để chỉnh *_CODE_MAX_AGE_SECONDS: tuổi mã lúc bắt đầu nhập,
                # thời gian nhập, và kết quả cuối. Lọc log theo "[CODE-AGE]".
                _age_at_dispatch = _code_age_for_log(
                    item.get("msg_ts"), (durable_item or {}).get("created_at"),
                )
                _submit_t0 = time.monotonic()
                if http_sequential:
                    status, account = await _submit_http_accounts_sequential(
                        code, item_target_url, item_domain,
                        source_kind=_dispatch_source_kind(item, durable_item),
                        preferred_username=account.get("username") if account else None)
                else:
                    status = await _submit_code_with_account_retries(
                        account, code, item_target_url, item_domain,
                        source_kind=_dispatch_source_kind(item, durable_item))
                logger.info(
                    "📊 [CODE-AGE] domain=%s account=%s code=%s age_at_dispatch=%ss "
                    "submit=%.1fs outcome=%s max_age=%.0fs",
                    item_domain, account["username"], code, _fmt_age(_age_at_dispatch),
                    time.monotonic() - _submit_t0, status, _max_age,
                )

                if status in {"SUCCESS_POINTS", "SUCCESS_NO_POINTS"}:
                    if _should_lock_account(item_domain, item, status):
                        await _mark_account_done_today(item_domain, account["username"])
                    elif item_domain == "xx88code.com" and _dispatch_source_kind(item, durable_item) == "spoiler":
                        _record_spoiler_no_lock(account["username"])
                    await _mark_code_used_if_final(item_domain, code, status)
                elif status == "FAILED":
                    await _mark_code_used_if_final(item_domain, code, status)

                if status == "TOKEN_NOT_READY":

                    _clear_site_code_duplicate(item_domain, account["username"], code)

                    token_delay = 2.0
                    if work_item_id and _durable_inbox is not None:
                        token_retry_state = await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.defer_work_item,
                            int(work_item_id),
                            "challenge token not ready",
                            token_delay, work_claim_token,
                        )
                        if token_retry_state != "deferred":
                            logger.debug(
                                "[%s#%s] token retry %s: %s",
                                item_domain, worker_id, code, token_retry_state,
                            )
                            continue
                        if _inbox_wakeup is not None:
                            _inbox_wakeup.set()
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR,
                            _durable_inbox.retry,
                            int(inbox_id),
                            "challenge token not ready",
                            token_delay,
                            claim_token,
                        )
                    else:
                        retry_item = dict(item)
                        await asyncio.sleep(token_delay)
                        try:
                            await queue.put(retry_item)
                        except asyncio.CancelledError:
                            raise
                    logger.warning(
                        "⏳ [%s#%s] %s — token Turnstile/sync chưa sẵn, defer ngắn",
                        item_domain, worker_id, code,
                    )
                    continue

                if status == "BROWSER_VERIFICATION_WAIT" and browser_engine is not None:
                    browser_engine.pause_domain_dispatch(item_domain, 5.0)

                if status == "PENDING_VERIFICATION":
                    if browser_engine is not None:
                        browser_engine.pause_domain_dispatch(item_domain, 5.0)

                    logger.warning(
                        "🖐️ [%s#%s] %s đang chờ xác minh thủ công — tạm dừng worker domain, "
                        "không lấy mã kế tiếp",
                        item_domain, worker_id, code,
                    )
                    verification_done = True
                    if browser_engine is not None and item_domain == "llwincode.com":
                        # PATCH_MARKER_2026-11-01-lag-fix: cap 30s. Worker LLwin
                        # la duy nhat cua domain; cho toi 210s (default cu) thi
                        # moi ma sau no gia them toi 210s trong khi ma LLwin
                        # chi song 120s (max_age). Watcher van chay rieng;
                        # het 30s worker di tiep theo dung nhanh timeout cu.
                        verification_done = await browser_engine.wait_for_pending_verification(
                            item_domain,
                            account["username"],
                            timeout_seconds=min(
                                30.0,
                                float(
                                    getattr(Config, "PENDING_VERIFY_WORKER_WAIT_SECONDS", 210.0)
                                ),
                            ),
                        )
                    if not verification_done:
                        logger.warning(
                            "⌛ [%s#%s] worker hết thời gian chờ xác minh %s; "
                            "watcher vẫn không bị cancel",
                            item_domain, worker_id, code,
                        )
                    verification_outcome = None
                    verify_status = "REVIEW_REQUIRED"
                    if verification_done and browser_engine is not None:
                        try:
                            verification_outcome = browser_engine.consume_pending_verification_result(
                                item_domain, account["username"], code
                            )
                        except Exception as exc:
                            logger.debug("⚠️ [%s] consume verify outcome lỗi: %s", item_domain, exc)

                    if verification_outcome:
                        verify_status = str(verification_outcome.get("status") or "NO_RESULT").upper()
                        if verify_status in {"SUCCESS_POINTS", "SUCCESS_NO_POINTS"}:
                            if _should_lock_account(item_domain, item, verify_status):
                                await _mark_account_done_today(item_domain, account["username"])
                            elif item_domain == "xx88code.com" and _dispatch_source_kind(item, durable_item) == "spoiler":
                                _record_spoiler_no_lock(account["username"], late=True)
                            await _mark_code_used_if_final(item_domain, code, verify_status)
                        elif verify_status in {"ACCOUNT_LIMIT", "ACCOUNT_BLOCKED"}:
                            await _mark_account_exhausted(item_domain, account["username"])
                        elif verify_status == "FAILED":
                            await _mark_code_used_if_final(item_domain, code, "FAILED")
                        logger.info(
                            "🎯 [%s#%s] %s outcome sau xác minh → %s",
                            item_domain, worker_id, code, verify_status,
                        )
                    else:
                        if verification_done:
                            logger.warning(
                                "⚠️ [%s#%s] %s watcher kết thúc nhưng chưa có outcome business rõ ràng — không khóa account",
                                item_domain, worker_id, code,
                            )
                        else:
                            logger.warning(
                                "⚠️ [%s#%s] %s vẫn đang chờ xác minh; kết quả chưa rõ — không khóa account",
                                item_domain, worker_id, code,
                            )
                    _clear_site_code_duplicate(item_domain, account["username"], code)
                    confirmed = bool(verification_outcome) and str(verification_outcome.get("status", "")).upper() in {
                        "SUCCESS_POINTS", "SUCCESS_NO_POINTS", "FAILED", "ACCOUNT_LIMIT"
                    }
                    if not confirmed and work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR, _durable_inbox.mark_work_item_uncertain,
                            int(work_item_id), "Verification outcome not confirmed", work_claim_token,
                        )
                        logger.warning("❓ [%s] %s verify chưa rõ kết quả; cần đối soát", item_domain, code)
                        continue
                    if work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.complete_work_item,
                            int(work_item_id), work_claim_token,
                        )
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR,
                            _durable_inbox.complete_item,
                            int(inbox_id),
                            claim_token,
                        )
                    logger.info(
                        "▶️ [%s#%s] xác minh/cleanup %s đã kết thúc — tiếp tục mã kế tiếp",
                        item_domain, worker_id, code,
                    )
                    continue

                if status == "SITE_UI_CHANGED":
                    if work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.complete_work_item,
                            int(work_item_id), work_claim_token,
                        )
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.complete_item,
                            int(inbox_id),
                            claim_token,
                        )
                    continue

                if status in {"ACCOUNT_LIMIT", "ACCOUNT_BLOCKED"}:
                    await _mark_account_exhausted(item_domain, account["username"])

                    if work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.defer_work_item,
                            int(work_item_id),
                            "account blocked; try next account",
                            0.2, work_claim_token,
                        )
                        if _inbox_wakeup is not None:
                            _inbox_wakeup.set()
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR,
                            _durable_inbox.retry_or_fail,
                            int(inbox_id),
                            "account blocked; try next account",
                            1,
                            claim_token,
                        )
                    else:
                        try:
                            queue.put_nowait({**item, "fanout": True})
                        except asyncio.QueueFull:
                            logger.warning("⚠️ [%s#%s] queue full while switching account for %s", item_domain, worker_id, code)
                    continue

                if status == "RATE_LIMITED":
                    if account is not None:
                        _clear_site_code_duplicate(item_domain, account["username"], code)
                    # Backoff ở cả domain; không cho các mã tiếp theo gửi
                    # lập tức trong khi site đang yêu cầu dừng thao tác.
                    delay = max(
                        1.0,
                        await submit_pacer.remaining(item_domain, account["username"]),
                    )
                    if work_item_id and _durable_inbox is not None:
                        # retry_work_item có giới hạn attempts; không defer
                        # vô hạn như defer_work_item (giảm attempts mỗi lần).
                        state = await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.retry_work_item,
                            int(work_item_id),
                            "business rate limited: observe site cooldown",
                            delay, work_claim_token,
                        )
                        logger.warning(
                            "⏳ [%s#%s] %s → RATE_LIMITED, durable=%s, delay>=%.1fs",
                            item_domain, worker_id, code, state, delay,
                        )
                        if _inbox_wakeup is not None:
                            _inbox_wakeup.set()
                    elif inbox_id and _durable_inbox is not None:
                        # Legacy inbox không có work_item_id: không replay cả
                        # batch đa mã do retry() reset toàn bộ parent row.
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR, _durable_inbox.complete_item,
                            int(inbox_id), claim_token,
                        )
                        logger.warning(
                            "⚠️ [%s#%s] Không có work_item_id cho %s; "
                            "đã ghi RATE_LIMITED, không phát lại cả batch",
                            item_domain, worker_id, code,
                        )
                    else:
                        attempts = int(item.get("_rate_limit_attempts", 0)) + 1
                        if attempts <= 2:
                            try:
                                queue.put_nowait({**item, "_rate_limit_attempts": attempts})
                            except asyncio.QueueFull:
                                logger.warning("⚠️ [%s#%s] Queue đầy khi defer %s", item_domain, worker_id, code)
                        else:
                            logger.warning("🛑 [%s] Dừng retry %s sau %s lần site throttle", item_domain, code, attempts)
                    continue

                if status in {"TAB_UNAVAILABLE", "BROWSER_VERIFICATION_WAIT"}:
                    _clear_site_code_duplicate(item_domain, account["username"], code)
                    if _defer_budget_exhausted(item_domain, code):
                        append_code_history(
                            event_type="FINAL_RESULT", code=code, target_url=item_target_url,
                            account=account["username"], status="VERIFICATION_BUDGET_EXHAUSTED",
                            message="Tab chưa xác minh quá ngân sách defer; bỏ mã để nhường mã mới",
                        )
                        if work_item_id and _durable_inbox is not None:
                            await asyncio.get_running_loop().run_in_executor(
                                _INBOX_STATE_EXECUTOR,
                                _durable_inbox.complete_work_item,
                                int(work_item_id), work_claim_token,
                            )
                        elif inbox_id and _durable_inbox is not None:
                            await asyncio.get_running_loop().run_in_executor(
                                _INBOX_EXECUTOR, _durable_inbox.complete_item,
                                int(inbox_id), claim_token,
                            )
                        logger.warning(
                            "🛑 [%s#%s] %s → hết ngân sách defer (%s), bỏ mã; hãy xác minh tab",
                            item_domain, worker_id, code, status,
                        )
                        continue
                    waiting_verification = status == "BROWSER_VERIFICATION_WAIT"
                    delay = 5.0 if waiting_verification else max(2.0, float(getattr(Config, "TAB_MISSING_RETRY_SECONDS", 8.0)))
                    if waiting_verification and _BROWSER_ENGINE_OK:
                        # Browser/global submit semaphores have already been
                        # released. Resume as soon as the shared tab probe wakes,
                        # without another fixed five-second retry delay.
                        if await browser_engine.wait_for_verification_ready(item_domain, timeout=5.0):
                            delay = 0.1
                    reason = "browser verification pending before submit" if waiting_verification else "no usable Edge tab"
                    if work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR, _durable_inbox.defer_work_item,
                            int(work_item_id), reason, delay, work_claim_token,
                        )
                        if _inbox_wakeup is not None:
                            _inbox_wakeup.set()
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR, _durable_inbox.retry_or_fail,
                            int(inbox_id), reason, delay, claim_token,
                        )
                    else:
                        if int(item.get("_volatile_attempts", 0)) < 2:
                            retry_item = dict(item)
                            retry_item["_volatile_attempts"] = int(item.get("_volatile_attempts", 0)) + 1
                            await asyncio.sleep(min(delay, 3.0))
                            try:
                                queue.put_nowait(retry_item)
                            except asyncio.QueueFull:
                                logger.warning("⚠️ [%s] queue full while waiting for Edge tab", item_domain)
                    continue

                if status == "REVIEW_REQUIRED":
                    _clear_site_code_duplicate(item_domain, account["username"], code)
                    append_code_history(
                        event_type="FINAL_RESULT", code=code, target_url=item_target_url,
                        account=account["username"], status="REVIEW_REQUIRED",
                        message="Response unclear: check dashboard/history; never auto-resubmit",
                    )
                    if work_item_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR, _durable_inbox.mark_work_item_uncertain,
                            int(work_item_id), "No decisive response after submit", work_claim_token,
                        )
                    elif inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR, _durable_inbox.complete_item,
                            int(inbox_id), claim_token,
                        )
                    logger.warning("❓ [%s#%s] %s → REVIEW_REQUIRED (không submit lại)", item_domain, worker_id, code)
                    continue

                if status == "INFRA_FAILURE":
                    if account is not None:
                        _clear_site_code_duplicate(item_domain, account["username"], code)
                    retry_state = "retried"
                    if work_item_id and _durable_inbox is not None:
                        retry_state = await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.retry_work_item,
                            int(work_item_id),
                            f"browser infrastructure unavailable for {code}",
                            10, work_claim_token,
                        )
                    elif inbox_id and _durable_inbox is not None:
                        retry_state = await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR,
                            _durable_inbox.retry_or_fail,
                            int(inbox_id),
                            f"browser infrastructure unavailable for {code}",
                            10,
                            claim_token,
                        )

                    if retry_state == "retried" and not inbox_id and not work_item_id:
                        try:
                            queue.put_nowait(dict(item))
                        except asyncio.QueueFull:
                            logger.warning(
                                "⚠️ [%s#%s] queue full while requeueing infrastructure failure for %s",
                                item_domain,
                                worker_id,
                                code,
                            )
                    continue

                if work_item_id and _durable_inbox is not None:
                    await asyncio.get_running_loop().run_in_executor(
                        _INBOX_STATE_EXECUTOR,
                        _durable_inbox.complete_work_item,
                        int(work_item_id), work_claim_token,
                    )
                elif inbox_id and _durable_inbox is not None:
                    await asyncio.get_running_loop().run_in_executor(
                        _INBOX_EXECUTOR, _durable_inbox.complete_item, int(inbox_id), claim_token
                    )

                icon = "✅" if status.startswith("SUCCESS") else ("❌" if status == "FAILED" else "⏰")
                logger.info(f"{icon} [{item_domain}#{worker_id}] {code} → {status}")
            except Exception as e:
                logger.error(f"❌ [{item_domain}#{worker_id}] {code}: {e}")
                if account is not None:
                    _clear_site_code_duplicate(item_domain, account["username"], code)
                if work_item_id and _durable_inbox is not None:
                    try:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR,
                            _durable_inbox.retry_work_item,
                            int(work_item_id),
                            f"domain worker: {e}",
                            5, work_claim_token,
                        )
                    except Exception:
                        logger.exception("❌ Không thể retry work item=%s", work_item_id)
                elif inbox_id and _durable_inbox is not None:
                    try:

                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR,
                            _durable_inbox.retry_or_fail,
                            int(inbox_id),
                            f"domain worker: {e}",
                            5,
                            claim_token,
                        )
                    except Exception:
                        logger.exception("❌ Không thể retry inbox row=%s", inbox_id)
                elif not work_item_id and not inbox_id:

                    volatile_attempts = int(item.get("_volatile_attempts", 0)) + 1
                    if volatile_attempts <= 3:
                        retry_item = dict(item)
                        retry_item["_volatile_attempts"] = volatile_attempts
                        try:
                            queue.put_nowait(retry_item)
                        except asyncio.QueueFull:
                            logger.error(
                                "❌ [%s#%s] mất item %s vì domain queue đầy và không có DurableInbox",
                                item_domain,
                                worker_id,
                                code,
                            )
                    else:
                        logger.error(
                            "🛑 [%s#%s] bỏ item %s sau %s lỗi khi không có DurableInbox",
                            item_domain,
                            worker_id,
                            code,
                            volatile_attempts - 1,
                        )
            finally:
                if sync_group is not None:
                    sync_group["event"].set()
                    if _tab_sync_pending.get(domain) is sync_group:
                        _tab_sync_pending.pop(domain, None)
                if account is not None:
                    _clear_site_code_duplicate(item_domain, account.get("username", ""), code)
                if account is not None and not (account.get("_tab_only") or account.get("_http_sequential")):
                    _release_account_reservation(item_domain, account.get("username"))
                reset_log_context(item_tok)
                if current_task is not None:
                    _active_submit_tasks.discard(current_task)
                queue.task_done()
    finally:
        reset_log_context(log_tok)


def start_domain_workers():
    global _domain_accounts
    _domain_accounts = build_domain_accounts_map()
    dmap = {}
    for cfg in Config.CHANNEL_CONFIG.values():
        if not cfg.get("enabled", True):
            continue
        d = normalize_domain(cfg.get("url", ""))
        if d and d not in dmap:
            dmap[d] = cfg["url"]

    total = 0
    for d, url in dmap.items():
        if d in _domain_workers:
            continue
        n_acc = len(_ordered_domain_accounts(d))
        wc = 2 if d in {"xx88code.com", "v79code.com"} else 1
        ws = []
        for i in range(1, wc + 1):
            t = asyncio.create_task(domain_code_worker(d, url, worker_id=i), name=f"domain-{d}-{i}")
            ws.append(t)
            bot_state.bg_tasks.add(t)
        _domain_workers[d] = ws
        total += wc
        _rpm, _burst = getattr(Config, "DOMAIN_RATE_LIMITS", {}).get(d, (0, 0))
        _acc_gap, _dom_gap = getattr(Config, "SITE_SUBMIT_GAPS", {}).get(d, (0.0, 0.0))
        logger.info(
            "⚙️ [Submit-Policy] site=%s accounts=%s workers=%s rpm=%s burst=%s "
            "account_gap=%.2fs domain_gap=%.2fs submit_pacer=enabled route=%s",
            d, n_acc, wc, _rpm, _burst, _acc_gap, _dom_gap,
            "http" if uses_direct_http(d) else "browser",
        )
    logger.info(f"🚀 {total} domain workers (HTTP/browser) cho {len(dmap)} domain")


async def _mark_code_used_if_final(domain: str, code: str, status: str):
    if status not in ("SUCCESS_POINTS", "SUCCESS_NO_POINTS", "FAILED"):
        return
    db = _systems.get("db") if _systems else None
    if db is None:
        return
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(_DB_EXECUTOR, db.mark_code_used, domain, code)
    except Exception as e:
        logger.debug(f"⚠️ mark_code_used: {e}")


_VIETNAMESE_DIACRITIC_RE = re.compile(r"[àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ]", re.IGNORECASE)


_PLAIN_TEXT_ORDER_PHONE_CONTEXT_RE = re.compile(
    r"(?:mã\s*(?:đơn|don|dh)|đơn\s*hàng|don\s*hang|order|invoice|"
    r"transaction|tracking|mã\s*giao\s*dịch|ma\s*giao\s*dich|"
    r"sđt|sdt|điện\s*thoại|dien\s*thoai|phone|hotline|liên\s*hệ|lien\s*he)",
    re.IGNORECASE,
)


async def submit_code_safe(
    user: str, code: str, target_url: str, systems: dict, *,
    tab_only: bool = False, sync_group=None,
):
    domain = normalize_domain(target_url)
    if not is_domain_enabled(domain):
        return {"success": False, "message": "Site disabled", "status": "SITE_DISABLED"}
    route = "http" if uses_direct_http(domain) else "browser"
    logger.info(f"🚀 [{route.upper()}] SUBMIT | {user} | {code} | {domain}")
    started = time.monotonic()
    try:
        if route == "http":
            raw = await _get_http_submitter().submit_for_target(
                user, code, target_url, systems,
                callbacks={"append_history": append_code_history},
            )
        elif tab_only:
            raw = await browser_engine.submit_code_browser_tab(
                code, target_url, systems, sync_group=sync_group)
            user = str(raw.get("account") or "")
        else:
            adapter = _get_browser_adapter()
            result = await adapter.submit_for_target(
                user, code, target_url, systems
            )
            raw = dict(result.raw)
            raw.setdefault("success", result.success)
            raw.setdefault("message", result.message)
            if result.kind.value == "TECHNICAL_FAILURE":
                raw["_infra_failure"] = True
            raw["browser_kind"] = result.kind.value
            raw["elapsed_seconds"] = result.elapsed_seconds or (time.monotonic() - started)
        raw["route"] = route
        if raw.get("latency_ms"):
            logger.info(
                "[METRIC] submit domain=%s account=%s code=%s latency=%s",
                domain,
                user,
                code,
                raw["latency_ms"],
            )
        if raw.get("_infra_failure") or raw.get("failure_kind") or raw.get("status") == "PENDING_VERIFICATION":
            elapsed_ms = float(raw.get("elapsed_seconds") or (time.monotonic() - started)) * 1000.0
            dashboard_status = str(raw.get("status") or raw.get("failure_kind") or "INFRA_FAILURE")
            update_dashboard(
                domain=domain,
                account=user,
                code=code,
                status=dashboard_status,
                rtt_ms=elapsed_ms,
                raw_response=str(raw.get("message") or "submission infrastructure failure"),
                latency_ms=raw.get("latency_ms"),
            )
        return raw
    except Exception as exc:
        logger.error(f"❌ [{route}|{domain}] submit lỗi ({type(exc).__name__})")
        if route == "http":
            return {
                "success": False, "message": f"HTTP result unconfirmed: {type(exc).__name__}",
                "route": route, "status": "AMBIGUOUS", "failure_kind": "UNKNOWN_AFTER_SUBMIT",
                "elapsed_seconds": time.monotonic() - started,
            }
        return {
            "success": False,
            "message": str(exc),
            "route": "browser",
            "_infra_failure": True,
            "elapsed_seconds": time.monotonic() - started,
        }

async def submit_code_with_delay(
    user: str, code: str, target_url: str, systems: dict, *,
    tab_only: bool = False, sync_group=None,
):
    domain = normalize_domain(target_url)
    timer = RequestTimer(request_id=f"submit:{domain}:{user}:{code}", kind="submit_request")

    ik = (domain, user, normalize_code(code, domain))
    if ik in bot_state._inflight_codes:
        timer.finish("duplicate")
        return {"success": False, "message": "In-flight duplicate"}
    bot_state._inflight_codes.add(ik)

    try:
        overall_started = time.perf_counter()
        async with timer.stage_async("rate_limit_wait"):
            # Chờ cooldown TRƯỚC khi chiếm slot browser/global. Không đặt
            # lịch submit ở đây vì còn phải chờ semaphore/tab tài khoản.
            await submit_pacer.wait_ready(domain, user)
            await get_domain_rate_limiter(domain).acquire()
            # Cooldown có thể vừa thay đổi trong lúc chờ token bucket.
            await submit_pacer.wait_ready(domain, user)
        rate_ms = (time.perf_counter() - overall_started) * 1000.0

        result = {"success": False, "message": "Not started"}
        submit_started = time.monotonic()

        domain_started = time.perf_counter()
        async with get_domain_semaphore(domain):
            domain_ms = (time.perf_counter() - domain_started) * 1000.0
            global_started = time.perf_counter()
            async with _get_submit_semaphore(domain):
                global_ms = (time.perf_counter() - global_started) * 1000.0
                # Recheck cooldown after semaphores. The selected transport
                # reserves its precise pacing slot immediately before click/POST.
                pacer_start = time.perf_counter()
                await submit_pacer.wait_ready(domain, user)
                pacer_ms = (time.perf_counter() - pacer_start) * 1000.0
                transport_started = time.perf_counter()
                try:
                    async with timer.stage_async("http_submit" if uses_direct_http(domain) else "browser_submit"):
                        timeout_setting = (
                            getattr(Config, "XX88_SUBMIT_TIMEOUT_SECONDS", 15.0)
                            if domain == "xx88code.com"
                            else getattr(Config, "SUBMIT_TIMEOUT_SECONDS", 45.0)
                        )
                        submit_timeout = max(5.0, float(timeout_setting))
                        result = await asyncio.wait_for(
                            submit_code_safe(
                                user, code, target_url, systems,
                                tab_only=tab_only, sync_group=sync_group,
                            ),
                            timeout=submit_timeout,
                        )
                except asyncio.TimeoutError:
                    result = {
                        "success": False,
                        "message": f"Timeout {submit_timeout:.0f}s",
                        "_infra_failure": True,
                    }
                    if uses_direct_http(domain):
                        result.pop("_infra_failure")
                        result.update(status="AMBIGUOUS", failure_kind="UNKNOWN_AFTER_SUBMIT", route="http")
                    update_dashboard(
                        domain=domain,
                        account=user,
                        code=code,
                        status=result.get("status", "INFRA_FAILURE"),
                        rtt_ms=submit_timeout * 1000.0,
                        raw_response=result["message"],
                    )
                except Exception as e:
                    result = {"success": False, "message": str(e), "_infra_failure": True}
                    if uses_direct_http(domain):
                        result.pop("_infra_failure")
                        result.update(status="AMBIGUOUS", failure_kind="UNKNOWN_AFTER_SUBMIT", route="http")
                    update_dashboard(
                        domain=domain,
                        account=user,
                        code=code,
                        status=result.get("status", "INFRA_FAILURE"),
                        rtt_ms=(time.monotonic() - submit_started) * 1000.0,
                        raw_response=str(e),
                    )
                transport_ms = (time.perf_counter() - transport_started) * 1000.0

        warn_ms = max(50.0, float(getattr(Config, "FLOW_SLOW_WAIT_WARN_MS", 300.0)))
        if bool(getattr(Config, "FLOW_LATENCY_LOG_ENABLED", True)) and (
            max(rate_ms, domain_ms, global_ms, pacer_ms) >= warn_ms
            or transport_ms >= float(getattr(Config, "FLOW_SLOW_BROWSER_WARN_MS", 2500.0))
        ):
            logger.warning(
                "⏱️ [FLOW] site=%s account=%s code=%s rate_wait=%.0fms "
                "domain_wait=%.0fms global_wait=%.0fms pacer_wait=%.0fms transport=%s submit=%.0fms",
                domain, user, code, rate_ms, domain_ms, global_ms, pacer_ms,
                "http" if uses_direct_http(domain) else "browser", transport_ms,
            )

        return result
    finally:
        timer.finish("done")
        bot_state._inflight_codes.discard(ik)


def build_unique_account_targets():
    items, seen = [], set()
    for cid, cfg in sorted(Config.CHANNEL_CONFIG.items(), key=lambda x: x[1].get("priority", 999)):
        if not cfg.get("enabled", True):
            continue
        url = cfg["url"]
        d = normalize_domain(url)
        accs = _available_domain_accounts(d)
        if not accs:
            continue
        for a in sorted(accs, key=lambda x: x.get("priority", 999)):
            k = (d, str(a["username"]).strip().casefold())
            if k in seen:
                continue
            seen.add(k)
            if _is_account_done_today(d, a["username"]):
                continue
            items.append({"chat_id": cid, "channel_name": cfg.get("name", ""), "target_url": url, "domain": d, "key": f"{d}|{a['username']}", "accounts": [a]})
    return items


async def init_channels_and_accounts():
    bot_state._site_code_seen.clear()
    domains = {normalize_domain(cfg["url"]) for cfg in Config.CHANNEL_CONFIG.values()
               if cfg.get("url") and cfg.get("enabled", True)}
    for domain in sorted(domains):
        await _load_account_rotation(domain)
    targets = build_unique_account_targets()
    if not targets:
        logger.info("ℹ️ Không còn target cần nhập: tài khoản đã SUCCESS hôm nay hoặc kênh đã tắt")
        return
    logger.info(
        "✅ %s target domain+tài khoản duy nhất đăng ký từ %s channel cấu hình",
        len(targets),
        len(Config.CHANNEL_CONFIG),
    )
    logger.info("🤖 BOT RUNNING (HTTP/browser theo từng site)")


async def init_systems():
    print_version_info()
    db = init_database(Config.DATABASE_PATH)
    hm, _ = init_monitoring(initialize_performance=False)
    try:
        hm.set_high_memory_callback(_emergency_ram_cleanup)
    except Exception:
        pass
    global _durable_inbox
    _durable_inbox = DurableInbox(
        getattr(Config, "TELEGRAM_INBOX_DB_PATH", "data/telegram_inbox.db"),
        lease_seconds=getattr(Config, "TELEGRAM_INBOX_LEASE_SECONDS", 300),

        max_attempts=getattr(Config, "MAX_INBOX_ATTEMPTS", 3),
        retry_base_delay=getattr(Config, "INBOX_RETRY_BASE_DELAY", 2.0),
        retry_max_delay=getattr(Config, "INBOX_RETRY_MAX_DELAY", 120.0),
    )
    recovery_mode = getattr(
        Config,
        "TELEGRAM_INBOX_RECOVERY_MODE",
        "at_least_once",
    )
    loop = asyncio.get_running_loop()
    if recovery_mode == "live_only":
        saved_checkpoints = await loop.run_in_executor(
            _INBOX_STATE_EXECUTOR,
            _durable_inbox.load_poll_checkpoints,
        )
        if saved_checkpoints:
            logger.warning(
                "⚠️ [Inbox] live_only không tương thích với checkpoint bền; "
                "chuyển sang at_least_once để không bỏ row đã được checkpoint"
            )
            recovery_mode = "at_least_once"
    if recovery_mode == "live_only":
        discarded = await loop.run_in_executor(
            _INBOX_STATE_EXECUTOR,
            _durable_inbox.discard_unfinished,
            "startup_live_only_discard_unfinished",
        )
        logger.warning(
            "🧹 [Inbox] live-only: bỏ qua %s row pending/processing từ phiên trước",
            discarded,
        )
    else:
        restored = await loop.run_in_executor(
            _INBOX_STATE_EXECUTOR,
            _durable_inbox.reset_retryable_failed,
            3600,
        )
        if restored:
            logger.info("[Inbox] Startup: recovered %s old retryable OCR failures", restored)
        reclaimed = await loop.run_in_executor(
            _INBOX_STATE_EXECUTOR,
            _durable_inbox.reclaim_stale,
        )
        if reclaimed:
            logger.warning("♻️ [Inbox] at-least-once: thu hồi %s row bị kẹt", reclaimed)
    logger.info("📥 [Inbox] recovery_mode=%s", recovery_mode)
    start_history_writer()
    get_shutdown_handler().setup(bot_state)
    pm = init_performance_monitor()
    return {
        "db": db,
        "db_executor": _DB_EXECUTOR,
        "inbox": _durable_inbox,
        "performance_monitor": pm,
        "health_monitor": hm,
    }


def _emergency_ram_cleanup():
    try:
        n_before = len(bot_state._site_code_seen)
        # Chỉ cắt mục đã quá SITE_CODE_DEDUP_TTL. Không clear() toàn bộ: tập này
        # chỉ chứa khóa chuỗi nhỏ nên giải phóng RAM gần như không đáng kể, nhưng
        # xóa hết sẽ làm mất chống nhập trùng đúng lúc máy đang quá tải.
        _prune_site_code_seen()
        n2 = f"{n_before}->{len(bot_state._site_code_seen)}"
        gc.collect()
        if browser_engine is not None:
            try:

                browser_engine.request_browser_memory_cleanup()
            except Exception:
                pass
        logger.warning(f"🧹 [RAM] seen={n2} | đã yêu cầu Edge dọn tab rảnh")
    except Exception:
        pass


async def verify_telegram_session():
    try:
        me = await client.get_me()
        dc = client.session.dc_id
        logger.info(f"✅ Session: @{me.username or me.id} | DC{dc}")
        return True
    except AuthKeyDuplicatedError:
        raise
    except Exception as e:
        logger.error(f"❌ session: {e}")
        return False


async def sync_telegram_dialogs() -> bool:

    if not getattr(Config, "TELEGRAM_SYNC_DIALOGS", True):
        logger.info("⏭️ Skip Telegram dialog sync (TELEGRAM_SYNC_DIALOGS=false)")
        return True

    timeout = max(
        10.0,
        float(getattr(Config, "TELEGRAM_DIALOG_SYNC_TIMEOUT", 120.0)),
    )
    try:
        # PATCH_MARKER_2026-10-25-v4-poll-ocr
        # Startup từng lấy limit=None = phân trang qua TOÀN BỘ dialog của tài
        # khoản trước khi handler chạy; cộng với verify 30 kênh đồng thời và
        # poll/backfill đầu tiên, đó là chùm RPC dồn trong ~1 phút đầu và là
        # ứng viên số 1 cho FloodWait ~30s sau READY trong log 25/10 (poll
        # chỉ 7 RPC/phút thì không tự gây flood). Giới hạn ở 500 dialog gần
        # nhất (kênh cấu hình đều đang hoạt động nên nằm trong đó); kênh nào
        # thiếu vẫn được direct-resolve ở dưới, không mất độ phủ.
        dialog_limit = max(100, int(getattr(Config, "TELEGRAM_DIALOG_SYNC_LIMIT", 500)))
        logger.info("📥 Syncing dialogs để kích hoạt update stream (limit=%s)...", dialog_limit)
        dialogs = await asyncio.wait_for(
            client.get_dialogs(limit=dialog_limit),
            timeout=timeout,
        )
        dialog_ids = {
            int(get_peer_id(dialog.entity))
            for dialog in dialogs
            if getattr(dialog, "entity", None) is not None
            and getattr(dialog.entity, "id", None) is not None
        }
        configured = {int(chat_id) for chat_id in Config.CHANNEL_CONFIG}
        cached = len(configured & dialog_ids)
        logger.info(
            "✅ Telegram dialogs synced | total=%s configured_cached=%s/%s",
            len(dialogs),
            cached,
            len(configured),
        )
        missing = sorted(configured - dialog_ids)
        if missing:
            logger.warning(
                "⚠️ Dialog cache còn thiếu %s channel; thử resolve trực tiếp "
                "để làm nóng entity cache",
                len(missing),
            )
            sem = asyncio.Semaphore(3)  # v4-poll-ocr: giãn RPC startup, tránh chùm FloodWait

            async def resolve_missing(chat_id: int) -> bool:
                async with sem:
                    try:
                        entity = await asyncio.wait_for(
                            client.get_entity((Config.CHANNEL_CONFIG.get(chat_id) or {}).get("username") or chat_id),
                            timeout=float(
                                getattr(Config, "TELEGRAM_CHANNEL_TIMEOUT", 10.0)
                            ),
                        )
                        return getattr(entity, "id", None) is not None
                    except AuthKeyDuplicatedError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "⚠️ Không resolve được channel %s trong sync: %s",
                            chat_id,
                            exc,
                        )
                        return False

            resolved = await asyncio.gather(
                *(resolve_missing(chat_id) for chat_id in missing)
            )
            resolved_count = sum(bool(item) for item in resolved)
            logger.info(
                "✅ Direct entity resolve trong sync: %s/%s",
                resolved_count,
                len(missing),
            )

            if resolved_count < len(missing):
                logger.warning(
                    "⚠️ Dialog cache chưa đủ sau direct resolve; tiếp tục khởi động "
                    "để bước verify_channels kiểm tra quyền truy cập thực tế"
                )
        return True
    except Exception as exc:
        logger.error("❌ Telegram dialog sync lỗi: %s", exc)
        return False


async def verify_channels_and_get_ids():
    sem = asyncio.Semaphore(3)  # v4-poll-ocr: giãn RPC startup, tránh chùm FloodWait
    valid = {}

    async def chk(cid, cfg):
        async with sem:
            try:
                entity = await asyncio.wait_for(
                    client.get_entity(cid),
                    timeout=float(
                        getattr(Config, "TELEGRAM_CHANNEL_TIMEOUT", 10.0)
                    ),
                )
                if getattr(entity, "left", False):
                    logger.warning(
                        "[PushDiag] channel=%s: tài khoản chưa tham gia kênh; "
                        "có thể chỉ nhận được qua poll. Hãy kiểm tra membership thủ công.", cid,
                    )
                logger.info(
                    "✅ [Telegram channel] id=%s name=%s entity=%s",
                    cid,
                    cfg.get("name", ""),
                    getattr(entity, "title", None) or getattr(entity, "username", None) or type(entity).__name__,
                )
                return cid, cfg
            except AuthKeyDuplicatedError:
                raise
            except Exception as e:
                logger.error(
                    "❌ [Telegram channel INVALID/NO ACCESS] id=%s name=%s error=%s",
                    cid,
                    cfg.get("name", ""),
                    e,
                )
                return cid, None

    res = await asyncio.gather(*[chk(c, cfg) for c, cfg in Config.CHANNEL_CONFIG.items()])
    for cid, cfg in res:
        if cfg is not None:
            valid[cid] = cfg
    logger.info(
        "📋 %s/%s channels valid | handler will subscribe by numeric chat_id (not name)",
        len(valid),
        len(Config.CHANNEL_CONFIG),
    )
    return valid


def _channel_code_limit(cfg) -> int:
    return max(1, int((cfg or {}).get("max_codes_per_message", 1)))


def _exclude_spoiler_codes_from_ocr(codes, cfg=None, domain=None):
    cfg = cfg or {}
    domain = domain or normalize_domain(cfg.get("url", ""))
    excluded = {normalize_code(value, domain) for value in cfg.get("_ocr_exclude_codes", ())}
    return [value for value in codes if normalize_code(value, domain) not in excluded]


def _filter_channel_codes(codes, cfg, *, source="normal"):
    target_url = (cfg or {}).get("url", "")
    unique = unique_keep_order(codes, target_url)
    if source == "image_ocr":
        # Revalidate cache/dispatch input before applying the per-message limit.
        domain = normalize_domain(target_url)
        unique = [code for code in unique
                  if CodeValidator.validate_code(code, target_url=target_url, source=source).get("valid")
                  and (domain != "getcodexx8bonus.pages.dev" or
                       (code.startswith("XX") and code.isascii() and code.isalnum() and
                        len(code) in tuple(getattr(Config, "XX8_OCR_CODE_LENGTHS", (8, 10)))))]
    if not (cfg or {}).get("strict_code_shape"):
        return scheduling.random_codes(unique)
    lengths = (cfg or {}).get("ocr_code_lengths", (10, 12))
    kept = [
        code for code in unique
        if len(code) in {int(length) for length in lengths}
        and code.isascii()
        and code.isalnum()
        and any(char.isalpha() for char in code)
        and (
            any(char.isdigit() for char in code)
            or (any(char.islower() for char in code) and any(char.isupper() for char in code))
        )
    ]
    return scheduling.random_codes(kept, _channel_code_limit(cfg))


def _ocr_failure_result(message: str, *, retryable: bool = True, text: str = "") -> dict:
    """Keep infrastructure errors distinct from a completed OCR scan with no code."""
    return {
        "success": False, "codes": [], "message": message, "text": text,
        "retryable": retryable,
        "failure_kind": "INFRASTRUCTURE" if retryable else "NO_CODE",
    }


async def _wait_for_ocr_admission():
    started = time.perf_counter()
    try:
        while getattr(Config, "PAUSE_OCR_ON_HIGH_CPU", True) and ocr_admission_paused():
            await asyncio.sleep(0.5)
    finally:
        timer = get_current_timer()
        if timer is not None:
            timer.stages["ocr_cpu_wait"] = timer.stages.get("ocr_cpu_wait", 0.0) + time.perf_counter() - started


async def process_image_from_telegram(event, channel_config, systems, on_code=None):
    domain = normalize_domain(channel_config.get("url", ""))
    dual_parse_domain = domain in (
        "xx88code.com", "getcodexx8bonus.pages.dev", "llwincode.com",
    )
    if not dual_parse_domain and not _is_ocr_allowed_channel(getattr(event, "chat_id", None), event):
        logger.info("⏭️ [OCR] Bỏ qua: channel không nằm trong OCR_ALLOWED_CHANNEL_IDS")
        return {"success": False, "codes": [], "message": "OCR channel not allowed", "text": "", "failure_kind": "SKIPPED", "retryable": False}
    cached = _cached_photo_ocr(event, channel_config)
    if cached is not None:
        logger.info("[OCR-CACHE] media hit before download chat=%s message=%s", event.chat_id, event.message.id)
        return cached
    # Downloads/cache lookup/manual input can progress while OCR is occupied
    # or admission is paused. Only native inference acquires a compute slot.
    return await _process_image_inner(event, channel_config, systems, on_code)


def _extract_site_codes_local(image_path, target_url, channel_config, on_code=None):
    """Model initialization and inference share one protected native future."""
    extractor = get_image_extractor()
    if extractor is None:
        raise RuntimeError("OCR not init")
    return extract_site_codes(image_path, extractor, target_url, channel_config, on_code=on_code)


async def _process_image_inner(event, channel_config, systems, on_code=None):
    target_url = channel_config.get("url", "")
    req_t = get_current_timer()
    image_started = time.perf_counter()
    ocr_started = None
    size_bytes = None
    dl = None
    img_path = None
    ocr_cache_status = "not_checked"
    telegram_media_id = None

    try:
        logger.info("📸 [OCR] processing")
        tmp = tempfile.mkdtemp(prefix=f"ocr_pid{os.getpid()}_")
        register_ocr_tmp(tmp)
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            is_video_media, video_media_id, video_extension = _ocr_video_media_info(event)
            telegram_media_id = video_media_id or _telegram_media_id(event)
            video_cache_path = _ocr_video_cache_path(telegram_media_id) if is_video_media else None
            cached_video_path = None
            if video_cache_path is not None:
                _prune_ocr_video_file_cache(video_cache_path.parent)
                _prune_ocr_video_partial_cache()
            if video_cache_path is not None and video_cache_path.is_file():
                cached_video_path = Path(tmp) / f"cached_video{video_extension}"
                try:
                    shutil.copy2(video_cache_path, cached_video_path)
                    size_bytes = cached_video_path.stat().st_size
                    dl = SimpleNamespace(
                        path=str(cached_video_path), size_bytes=size_bytes, elapsed_ms=0.0,
                        bytes_per_sec=None, attempts=0, dedup_hit=False,
                    )
                    logger.info("OCR_VIDEO_FILE_CACHE hit media_id=%s", telegram_media_id)
                except OSError as error:
                    logger.warning("OCR_VIDEO_FILE_CACHE read_failed media_id=%s error=%s",
                                   telegram_media_id, error)
                    cached_video_path = None
            if cached_video_path is None:
                report_download_started()
                try:
                    _download_domain = normalize_domain(target_url)
                    _dl_manager = (
                        _xx8_media_download_manager
                        if _download_domain == "getcodexx8bonus.pages.dev"
                        else _media_download_manager
                    )
                    if _download_domain == "getcodexx8bonus.pages.dev":
                        logger.info(
                            "⚡ [XX8-FAST] Telegram multipart=%s workers=%s",
                            getattr(Config, "XX8_FAST_TELEGRAM_DOWNLOAD", True),
                            getattr(Config, "XX8_FAST_DOWNLOAD_WORKERS", 8),
                        )
                    if req_t:
                        async with req_t.stage_async("telegram_download"):
                            dl = await _dl_manager.download(event, tmp, timeout=_media_download_timeout(
                                _download_domain, getattr(getattr(event, 'message', None), 'date', None)))
                    else:
                        dl = await _dl_manager.download(event, tmp, timeout=_media_download_timeout(
                            _download_domain, getattr(getattr(event, 'message', None), 'date', None)))
                finally:
                    report_download_finished()

            img_path = dl.path
            size_bytes = dl.size_bytes

            if dl.dedup_hit:
                return _ocr_failure_result("dedup")
            if not img_path:
                return _ocr_failure_result("download fail")

            orig_ext = Path(img_path).suffix.lower() or ".jpg"
            if is_video_media and orig_ext not in VIDEO_FORMATS:
                orig_ext = video_extension
            if video_cache_path is not None and Path(img_path) != video_cache_path:
                cache_tmp = None
                try:
                    cache_max_bytes = max(1, int(getattr(
                        Config, "OCR_VIDEO_FILE_CACHE_MAX_BYTES", 1024**3
                    )))
                    if Path(img_path).stat().st_size <= cache_max_bytes:
                        video_cache_path.parent.mkdir(parents=True, exist_ok=True)
                        fd, cache_tmp = tempfile.mkstemp(
                            prefix=f".{video_cache_path.stem}.", suffix=".tmp",
                            dir=str(video_cache_path.parent),
                        )
                        os.close(fd)
                        shutil.copy2(img_path, cache_tmp)
                        os.replace(cache_tmp, video_cache_path)
                        _prune_ocr_video_file_cache(video_cache_path.parent)
                        logger.info("OCR_VIDEO_FILE_CACHE stored media_id=%s", telegram_media_id)
                    else:
                        logger.info(
                            "OCR_VIDEO_FILE_CACHE skip_oversize media_id=%s max_bytes=%s",
                            telegram_media_id, cache_max_bytes,
                        )
                except OSError as error:
                    logger.warning("OCR_VIDEO_FILE_CACHE write_failed media_id=%s error=%s",
                                   telegram_media_id, error)
                finally:
                    if cache_tmp and os.path.exists(cache_tmp):
                        os.unlink(cache_tmp)
            uniq = Path(tmp) / f"ocr_{ts}{orig_ext}"
            Path(img_path).rename(uniq)
            img_path = str(uniq)

            loop = asyncio.get_running_loop()
            crop = channel_config.get("ocr_crop")
            ocr_crops = channel_config.get("ocr_crops") or ([crop] if crop else [None])
            is_video = orig_ext.lower() in VIDEO_FORMATS
            cache_key = await loop.run_in_executor(
                _MEDIA_EXECUTOR,
                _ocr_file_cache_key,
                img_path,
                target_url,
                ocr_crops,
                is_video,
                bool((channel_config or {}).get("ocr_single_code_mode", False)),
                telegram_media_id,
                _ocr_channel_policy(channel_config),
            )
            cached_result = _ocr_cache_get(cache_key)
            if cached_result is not None:
                ocr_cache_status = "hit"
                logger.info(
                    "⚡ [OCR] cache HIT media_id=%s file=%s",
                    telegram_media_id or "file-hash-fallback",
                    Path(img_path).name,
                )
                return cached_result
            ocr_cache_status = "miss"
            logger.debug(
                "🧊 [OCR] cache MISS media_id=%s file=%s",
                telegram_media_id or "file-hash-fallback",
                Path(img_path).name,
            )

            slot_started = time.perf_counter()
            async with _get_ocr_semaphore():
                if req_t:
                    req_t.stages["ocr_slot_wait"] = req_t.stages.get("ocr_slot_wait", 0.0) + time.perf_counter() - slot_started
                # Recheck resources after waiting for a slot. The downloaded
                # file is already available and downloads hold no OCR slot.
                if getattr(Config, "PAUSE_OCR_ON_HIGH_CPU", True):
                    await _wait_for_ocr_admission()
                ocr_started = time.perf_counter()
                # on_code (phát mã sớm) chỉ có ý nghĩa với video; ảnh tĩnh quét một lần như cũ.
                future = loop.run_in_executor(
                    _OCR_EXECUTOR, _extract_site_codes_local, img_path, target_url, channel_config,
                    on_code if is_video else None)
                try:
                    if req_t:
                        async with req_t.stage_async("ocr"):
                            result = await asyncio.shield(future)
                    else:
                        result = await asyncio.shield(future)
                except asyncio.CancelledError:
                    # Keep the compute slot and temp input until native
                    # initialization/inference actually finishes, including
                    # repeated cancellation requests during shutdown.
                    while not future.done():
                        try:
                            await asyncio.shield(future)
                        except asyncio.CancelledError:
                            continue
                        except Exception:
                            break
                    if not future.cancelled():
                        error = future.exception()
                        if error is not None:
                            logger.debug("[OCR] Native worker failed while cancellation was pending: %s", error)
                    raise
                metrics = result.get("ocr_metrics", {})
                ocr_frame_count = metrics.get("sampled_frames", 0)
                logger.info("[OCR-DYNAMIC] domain=%s codes=%s metrics=%s",
                            normalize_domain(target_url), len(result.get("codes", [])), metrics)
            _ocr_cache_put(cache_key, result)
            return result
        finally:
            total_elapsed_ms = (time.perf_counter() - image_started) * 1000.0
            ocr_elapsed_ms = (
                (time.perf_counter() - ocr_started) * 1000.0
                if ocr_started is not None
                else None
            )
            logger.info(
                "⏱️ [OCR-TIMING] image=%s domain=%s total_ms=%.1f ocr_ms=%s "
                "download_ms=%s frames=%s size_bytes=%s cache_status=%s media_id=%s "
                "queue_ms=%.1f cpu_wait_ms=%.1f slot_wait_ms=%.1f",
                Path(img_path).name if img_path else "(download-failed)",
                normalize_domain(target_url),
                total_elapsed_ms,
                f"{ocr_elapsed_ms:.1f}" if ocr_elapsed_ms is not None else "n/a",
                f"{getattr(dl, 'elapsed_ms', 0.0):.1f}" if dl is not None else "n/a",
                locals().get("ocr_frame_count", len(locals().get("targets", []) or [])),
                size_bytes if size_bytes is not None else "n/a",
                ocr_cache_status,
                telegram_media_id or "none",
                (req_t.stages.get("ocr_queue_wait", 0.0) * 1000.0) if req_t else 0.0,
                (req_t.stages.get("ocr_cpu_wait", 0.0) * 1000.0) if req_t else 0.0,
                (req_t.stages.get("ocr_slot_wait", 0.0) * 1000.0) if req_t else 0.0,
            )
            if req_t:
                req_t.emit("media_complete", media=bool(getattr(dl, "path", None) or getattr(dl, "dedup_hit", False)), file_size_bytes=size_bytes, media_extension=Path(img_path).suffix.lower() if img_path else None, download_elapsed_ms=getattr(dl, "elapsed_ms", None), download_bytes_per_sec=getattr(dl, "bytes_per_sec", None), download_attempts=getattr(dl, "attempts", None), download_dedup_hit=getattr(dl, "dedup_hit", None))
            try:
                shutil.rmtree(tmp)
            except Exception:
                pass
            finally:
                release_ocr_tmp(tmp)
    except Exception as e:
        logger.error(f"❌ [OCR] {e}\n{traceback.format_exc()}")
        return _ocr_failure_result(f"OCR error: {e}")


async def submit_codes_from_image(
    user, codes_data, target_url, channel_config, systems, inbox_id=None, claim_token=None,
    msg_ts=None,
):
    domain = normalize_domain(target_url)
    if not is_domain_enabled(domain):
        return
    await _load_account_rotation(domain)
    if not _available_domain_accounts(domain):
        if inbox_id and _durable_inbox is not None:
            await asyncio.get_running_loop().run_in_executor(
                _INBOX_STATE_EXECUTOR, _durable_inbox.mark_ignored,
                int(inbox_id), "no_eligible_accounts", claim_token)
        return

    codes = _filter_channel_codes(
        [item.get("code", "") for item in codes_data or []], channel_config, source="image_ocr"
    )
    codes = _exclude_spoiler_codes_from_ocr(codes, channel_config, domain)
    db = systems.get("db") if systems else None
    if db is not None and codes:
        unused = await asyncio.get_running_loop().run_in_executor(_DB_EXECUTOR, db.unused_codes, domain, codes)
        codes = [code for code in codes if normalize_code(code, domain) in unused]

    if domain == "xx88code.com":
        logger.info("🔎 [XX88-CODE-AUDIT] ocr=%d unique=%d after_dedup=%d codes=%s",
                    len(codes_data or []), len(unique_keep_order([x.get("code", "") for x in codes_data or []], domain)),
                    len(codes), codes)
    elif domain == "llwincode.com":
        logger.info(
            "🔎 [LLWIN-CODE-AUDIT] ocr=%d after_filter_dedup=%d codes=%s",
            len(codes_data or []), len(codes), codes,
        )

    if domain not in ("xx88code.com", "getcodexx8bonus.pages.dev") and bool(getattr(Config, "SOURCE_AWARE_DISPATCH", True)):
        codes = codes[:_channel_code_limit(channel_config)]
    row = None
    if inbox_id and _durable_inbox is not None:
        row = await asyncio.get_running_loop().run_in_executor(_INBOX_STATE_EXECUTOR, _durable_inbox.get, int(inbox_id))
    items = [{"code": code, "domain": domain, "target_url": target_url,
              "source": "image_ocr",
              "source_kind": "ocr",
              "channel_name": channel_config.get("name", ""),
              "inbox_id": inbox_id, "claim_token": claim_token,
              "msg_ts": (row or {}).get("message_date") or msg_ts, "fanout_index": 0,
              "fanout": False}
             for code in codes]
    if items:
        items = (_build_stateless_entry_items(items) if _stateless_entry_enabled()
                 else _build_distinct_code_items(items, domain, source="ocr"))
    if domain in ("xx88code.com", "getcodexx8bonus.pages.dev"):
        label = "XX8" if domain == "getcodexx8bonus.pages.dev" else "XX88"
        logger.info("🔎 [%s-DISPATCH-AUDIT] code_count=%d account_submissions=%d pairs=%s",
                    label, len(codes), len(items),
                    [(x.get("code"), x.get("fanout_index")) for x in items])
    if inbox_id and _durable_inbox is not None:
        if not items:
            await asyncio.get_running_loop().run_in_executor(
                _INBOX_STATE_EXECUTOR, _durable_inbox.mark_ignored, int(inbox_id), "ocr_no_code", claim_token)
            return
        ids = await asyncio.get_running_loop().run_in_executor(
            _INBOX_STATE_EXECUTOR, _durable_inbox.create_work_items, int(inbox_id), items, claim_token)
        items = [{**item, "work_item_id": item_id} for item, item_id in zip(items, ids) if item_id]
    queued = 0
    deferred = 0
    for item in items:
        if await _enqueue_domain_item(item):
            queued += 1
        else:
            deferred += 1
    logger.info(
        "📥 [IMG] %s code | %s lượt account → %s queued, %s deferred | %s",
        len(codes), len(items), queued, deferred, domain,
    )


async def _enqueue_ocr_media_job(event, cfg, systems, account: str | None, *, track_inbox: bool = True) -> bool:

    global _ocr_media_queue
    message = getattr(event, "message", None)
    caption = getattr(message, "message", None)
    if caption is None:
        caption = getattr(message, "text", "")
    dual_parse_domain = normalize_domain(cfg.get("url", "")) in (
        "xx88code.com", "getcodexx8bonus.pages.dev", "llwincode.com",
    )
    if (not dual_parse_domain and _ocr_is_image_message(message)
            and len(str(caption or "")) == 0
            and not _ocr_has_known_code_input_link(event)):
        logger.info("OCR_PREFILTER_SKIP reason=no_text_or_link")
        await _ignore_inbox_event(event, "ocr_prefilter_no_text_or_link")
        return False
    # Enqueue without waiting for CPU recovery: the bounded media queue separates
    # OCR load from text/spoiler handlers. The OCR worker checks CPU before work.
    enqueue_started = time.perf_counter()
    if _ocr_media_queue is None:
        start_ocr_media_workers()
    if not track_inbox:

        event = copy.copy(event)
        event.inbox_id = None
        event.claim_token = None
    inbox_id = getattr(event, "inbox_id", None)
    claim_token = getattr(event, "claim_token", None)
    if inbox_id and _durable_inbox is not None:
        await asyncio.get_running_loop().run_in_executor(
            _INBOX_STATE_EXECUTOR,
            _durable_inbox.set_remaining,
            int(inbox_id),
            1,
            claim_token,
        )
    try:
        _ocr_media_queue.put_nowait((event, cfg, systems, account, time.perf_counter()))
        logger.info(
            "📥 [OCR-QUEUE] enqueue chat=%s message=%s q=%s/%s enqueue_ms=%.1f",
            getattr(event, "chat_id", "?"),
            getattr(getattr(event, "message", None), "id", "?"),
            _ocr_media_queue.qsize(),
            _ocr_media_queue.maxsize,
            (time.perf_counter() - enqueue_started) * 1000.0,
        )
        return True
    except asyncio.QueueFull:
        dropped = _ocr_media_queue.get_nowait()
        _ocr_media_queue.task_done()
        _ocr_media_queue.put_nowait((event, cfg, systems, account, time.perf_counter()))
        _async_ingest_counts["OCR_DROP_OLD"] += 1
        logger.warning("OCR_DROP_OLD total=%s q=%s/%s",
                       _async_ingest_counts["OCR_DROP_OLD"], _ocr_media_queue.qsize(), _ocr_media_queue.maxsize)
        old_event = dropped[0]
        old_id = getattr(old_event, "inbox_id", None)
        if old_id and _durable_inbox is not None:
            loop = asyncio.get_running_loop()
            active = await loop.run_in_executor(_INBOX_STATE_EXECUTOR, _durable_inbox.has_active_work_items, int(old_id))
            if not active:
                await loop.run_in_executor(
                    _INBOX_STATE_EXECUTOR, _durable_inbox.mark_ignored,
                    int(old_id), "ocr_drop_old", getattr(old_event, "claim_token", None))
            else:
                logger.warning("⚠️ [OCR-QUEUE] đầy — giữ row=%s trong durable inbox để retry", old_id)
        return True


async def _retry_ocr_inbox_event(event, reason: str) -> bool:
    """Schedule bounded durable replay; return False for untracked media jobs."""
    inbox_id = getattr(event, "inbox_id", None)
    if not inbox_id or _durable_inbox is None:
        return False
    loop = asyncio.get_running_loop()
    if await loop.run_in_executor(
        _INBOX_STATE_EXECUTOR, _durable_inbox.has_active_work_items, int(inbox_id),
    ):
        # Routing may have committed all work items before a later enqueue
        # failed. Retrying/failing their parent can strand these durable codes;
        # let inbox drain resume them under their existing item identities.
        if _inbox_wakeup is not None:
            _inbox_wakeup.set()
        logger.warning("[OCR-RETRY] row=%s retains active durable work: %s", inbox_id, reason)
        return True
    state = await loop.run_in_executor(
        _INBOX_STATE_EXECUTOR,
        _durable_inbox.retry_or_fail,
        int(inbox_id),
        f"ocr infrastructure: {reason}",
        max(0.1, float(getattr(Config, "INBOX_RETRY_BASE_DELAY", 2.0))),
        getattr(event, "claim_token", None),
    )
    if state == "retried" and _inbox_wakeup is not None:
        _inbox_wakeup.set()
    logger.warning("[OCR-RETRY] row=%s state=%s reason=%s", inbox_id, state, reason)
    return True


async def _run_ocr_media_job(event, cfg, systems, account, *, queued_at=None):
    # Queue workers do not inherit the message handler's ContextVar timer.
    timer = RequestTimer.from_event(event, kind="ocr_media")
    if queued_at is not None:
        timer.stages["ocr_queue_wait"] = max(0.0, time.perf_counter() - queued_at)
        timer.started_perf = queued_at
    token = set_current_timer(timer)
    log_token = set_log_context(get_site_log_tag(cfg.get("url", "")))
    try:
        await _run_ocr_media_job_inner(event, cfg, systems, account)
    finally:
        try:
            timer.finish()
        finally:
            reset_current_timer(token)
            reset_log_context(log_token)


async def _run_ocr_media_job_inner(event, cfg, systems, account):
    # Untracked supplementary OCR has no durable row; retain its job locally
    # through bounded retries rather than losing it or replaying unrelated work.
    max_attempts = max(1, int(getattr(Config, "MAX_INBOX_ATTEMPTS", 3)))
    for attempt in range(max_attempts):
        domain = normalize_domain(cfg.get("url", ""))
        max_age = float((getattr(Config, "SITE_CODE_MAX_AGE_SECONDS", None) or {}).get(
            domain, getattr(Config, "CODE_MAX_AGE_SECONDS", 120.0)))
        stale, age = first_known_age(
            (getattr(getattr(event, "message", None), "date", None),), max_age)
        if stale:
            logger.info("[OCR-QUEUE] skip expired media age=%.1fs max_age=%.1fs domain=%s",
                        age, max_age, domain)
            await _ignore_inbox_event(event, "ocr_stale_message")
            return
        early = None
        if getattr(Config, "OCR_VIDEO_EARLY_EMIT_ENABLED", True):
            message_date = getattr(getattr(event, "message", None), "date", None)

            async def _early_submit(item, _date=message_date):
                # Mã gửi sớm đi như mã tạm thời (không work_item/inbox): nếu không, mã đầu tiên xong
                # sớm có thể đóng hàng gốc trong lúc OCR còn quét, làm rơi các mã còn lại.
                await submit_codes_from_image(
                    account, [item], cfg["url"], cfg, systems, None, None, msg_ts=_date)

            early = scheduling.EarlyDispatch(asyncio.get_running_loop(), _early_submit)
        try:
            result = await process_image_from_telegram(
                event, cfg, systems, early.on_code if early is not None else None)
            if result.get("success"):
                codes_for_batch = result["codes"]
                skipped = set()
                if early is not None:
                    codes_for_batch, skipped = await early.settle(
                        result["codes"], result.get("early_emitted"))
                    if early.errors:
                        logger.warning("⚠️ [OCR-EARLY] %s lần gửi sớm lỗi; mã đó vẫn nằm trong lô cuối: %s",
                                       len(early.errors), early.errors[0])
                    if skipped:
                        logger.info("⚡ [OCR-EARLY] đã gửi sớm %s/%s mã: %s",
                                    len(skipped), len(result["codes"]), sorted(skipped))
                inbox_id = getattr(event, "inbox_id", None)
                if skipped and not codes_for_batch:
                    # Mọi mã đã đi đường gửi sớm: đóng hàng gốc thay vì ghi "không có mã".
                    if inbox_id and _durable_inbox is not None:
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_STATE_EXECUTOR, _durable_inbox.set_remaining,
                            int(inbox_id), 0, getattr(event, "claim_token", None))
                else:
                    # Persist/route extracted codes through the same bounded
                    # retry path when a temporary database error occurs.
                    await submit_codes_from_image(
                        account, codes_for_batch, cfg["url"], cfg, systems,
                        inbox_id, getattr(event, "claim_token", None),
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result = _ocr_failure_result(f"{type(exc).__name__}: {exc}")
        report_ocr_result(result)
        if result.get("success"):
            return
        if result.get("failure_kind") == "NO_CODE" and not result.get("retryable"):
            logger.info(
                "[OCR-NO-CODE] chat=%s message=%s reason=%s chars=%s; no submit",
                getattr(event, "chat_id", None), getattr(getattr(event, "message", None), "id", None),
                result.get("message", "no valid code"), len(result.get("text") or ""),
            )
            await _ignore_inbox_event(event, "ocr_no_result")
            return
        if result.get("failure_kind") == "SKIPPED" and not result.get("retryable"):
            await _ignore_inbox_event(event, "ocr_channel_not_allowed")
            return
        reason = str(result.get("message") or "OCR outcome not confirmed")
        if await _retry_ocr_inbox_event(event, reason):
            return
        if attempt + 1 >= max_attempts:
            logger.error("[OCR-RETRY] Untracked job exhausted %s attempts: %s", max_attempts, reason)
            return
        delay = min(
            max(0.1, float(getattr(Config, "INBOX_RETRY_MAX_DELAY", 120.0))),
            max(0.1, float(getattr(Config, "INBOX_RETRY_BASE_DELAY", 2.0))) * (2 ** attempt),
        )
        await asyncio.sleep(delay)


async def ocr_media_worker(worker_id: int):
    logger.info("👷 OCR/media worker #%s ready", worker_id)
    while bot_state.is_running or (_ocr_media_queue is not None and not _ocr_media_queue.empty()):
        if _ocr_media_queue is None:
            await asyncio.sleep(0.2)
            continue
        job = None
        try:
            job = await asyncio.wait_for(_ocr_media_queue.get(), timeout=1.0)
            event, cfg, systems, account, queued_at = job
            try:
                await _run_ocr_media_job(event, cfg, systems, account, queued_at=queued_at)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("❌ [OCR-QUEUE#%s] %s: %s", worker_id, type(exc).__name__, exc)
                await _retry_ocr_inbox_event(event, f"ocr_media_worker: {exc}")
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
        except Exception:
            # Even retry persistence can fail during a database outage. Keep
            # the worker alive; the durable lease can recover its tracked job.
            logger.exception("❌ [OCR-QUEUE#%s] retry persistence failed; worker continues", worker_id)
        finally:
            if job is not None and _ocr_media_queue is not None:
                _ocr_media_queue.task_done()


def start_ocr_media_workers():
    global _ocr_media_queue
    if _ocr_media_queue is None:
        _ocr_media_queue = asyncio.Queue(
            maxsize=max(1, int(getattr(Config, "OCR_MEDIA_QUEUE_MAXSIZE", 32)))
        )
    if _ocr_media_workers:
        return
    count = max(1, int(getattr(Config, "OCR_MEDIA_WORKERS", 4)))
    for i in range(1, count + 1):
        _ocr_media_workers.append(
            asyncio.create_task(ocr_media_worker(i), name=f"ocr-media-{i}")
        )
    logger.info("🚀 %s OCR/media workers started (queue=%s, native OCR slots=%s, download-ahead=true)",
                count, _ocr_media_queue.maxsize, getattr(Config, "MAX_CONCURRENT_OCR", 2))


async def _ignore_inbox_event(event, reason: str) -> None:
    inbox_id = getattr(event, "inbox_id", None)
    claim_token = getattr(event, "claim_token", None)
    if inbox_id and _durable_inbox is not None:
        await asyncio.get_running_loop().run_in_executor(
            _INBOX_EXECUTOR,
            _durable_inbox.mark_ignored,
            int(inbox_id),
            reason,
            claim_token,
        )


# PATCH_MARKER_2026-11-01-llwin-kt-slim: parser KT LLwin gop thang vao
# PATCH_MARKER_2026-11-02-llwin-kt-v2: sua 2 loi ngay 02/11 (xem _segment_tokens
# PATCH_MARKER_2026-11-02-llwin-kt-v3: sua loi 13:21 (xem _strip_diacritics
# va parse_llwin_spoiler) — van chi file nay, tren nen KT v2.
# va _try_llwin_kt_spoiler) — van chi file nay, khong them file moi.
# file nay (logic goc parser KT v2.1, khong doi thuat toan) — khong
# can them file .py rieng nao nua.


import re
import unicodedata

LLWIN_SPOILER_CHANNEL_IDS = frozenset({
    -1003873843218,  # LLwin PHAT CODE MIEN PHI (t.me/llwincodefree)
    -1003506071961,  # LLwin DICH VU GIAI NHAN (@LLWINDICHVUGIAINHAN)
    -1003903971775,  # LLwin GIAI TRI 24H (@LLwingiaitri24h)
    # V2.1: them 2 kenh KJC cung format (user bao 28/10; doi chieu tu
    # CHANNEL_CONFIG trong 0a.zip — ca 2 deu ve xx88code.com, khong
    # phai llwincode.com; chi nhan bang chat_id vi config khong co
    # username cho 2 kenh nay).
    -1002528908352,  # KJC GAI XINH -> xx88code.com
    -1003503954906,  # KJC - DONG HANH THE THAO -> xx88code.com
})

# Ma trong format KT-spoiler nay dung 6 ky tu (user chot 28/10/2026).
KT_CODE_LENGTH = 6

LLWIN_SPOILER_CHANNEL_KEYS = frozenset({
    "llwincodefree",
    "llwindichvugiainhan",
    "llwingiaitri24h",
    "llwin phat code mien phi",
    "llwin dich vu giai nhan",
    "llwin giai tri 24h",
})

# Tu so tieng Viet -> chu so (khoa da bo dau, viet hoa).
VIETNAMESE_DIGITS = {
    "KHONG": "0",
    "MOT": "1",
    "HAI": "2",
    "BA": "3",
    "BON": "4",
    "NAM": "5",
    "SAU": "6",
    "BAY": "7",
    "TAM": "8",
    "CHIN": "9",
}

# Ten chu cai tieng Viet -> chu cai (khoa da qua _norm: bo dau + viet
# hoa; dau moc (horn) la combining mark nen bi boc theo, vi du GO=G,
# LO=L, MO=M, RO=R, HAT=H; rieng DE (D) va ĐE (Đ) van phan biet duoc
# vi Đ khong phan rach duoc).
VIETNAMESE_LETTER_NAMES = {
    "A": "A",
    "BE": "B",
    "XE": "C",   # XE la C, khong phai X — de nham nhat
    "DE": "D",
    "ĐE": "Đ",
    "E": "E",
    "GO": "G",
    "HAT": "H",
    "I": "I",
    "GI": "J",
    "KA": "K",
    "LO": "L",
    "MO": "M",
    "NO": "N",
    "O": "O",
    "PE": "P",
    "QUY": "Q",
    "RO": "R",
    "ET": "S",
    "SO": "S",   # cach goi khac cua S ("so")
    "TE": "T",
    "U": "U",
    "VE": "V",
    "ICH": "X",   # ICH moi la X
    "Y": "Y",
    "ZET": "Z",
}

def _strip_diacritics(text: str) -> str:
    # PATCH_MARKER_2026-11-02-llwin-kt-v3: NFD -> NFKD. NFKD them kha nang
    # go chu tuong thich ve ASCII: chu dam toan hoc (𝐍->N), chu fullwidth
    # (Ｎ->N, ％->%, ＆->&, ：->:). NFD khong go duoc nhung dang nay nen
    # header "NHAP CODE NGAY" dang fancy/fullwidth va ma dang W％D1WV
    # deu khong khop regex -> parse tra None (dung loi 13:21). Đ van
    # khong phan ra (kiem chung: NFKD('Đ') == 'Đ') nen DE/ĐE van phan biet.
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(c for c in decomposed if unicodedata.category(c) != "Mn")


def _norm(text: str) -> str:
    """Bo dau + viet hoa, de so khop khong phu thuoc dau/hoa-thuong."""
    return _strip_diacritics(text).upper()


def _channel_allowed(chat_id=None, channel_key=None) -> bool:
    if chat_id is not None:
        return chat_id in LLWIN_SPOILER_CHANNEL_IDS
    if channel_key:
        key = _norm(channel_key).strip().lstrip("@").lower()
        return key in LLWIN_SPOILER_CHANNEL_KEYS
    return False


def _resolve_fill(suffix: str):
    """Hau to sau '_' -> 1 ky tu (so hoac chu cai), hoac None neu
    khong hieu. Thu tu: so truc tiep -> 1 chu cai don -> tu so ->
    ten chu cai. Khong hieu thi tra None (khong doan bua)."""
    s = (suffix or "").strip()
    if not s:
        return None
    if s[0].isdigit():
        return s[0]
    if len(s) == 1 and s.isalpha() and s.isascii():
        return s.upper()
    chunk = re.split(r"[\s\-–—_,.;:]+", _norm(s))[0]
    chunk = re.sub(r"[^A-ZĐ]", "", chunk)
    if not chunk:
        return None
    if chunk in VIETNAMESE_DIGITS:
        return VIETNAMESE_DIGITS[chunk]
    if chunk in VIETNAMESE_LETTER_NAMES:
        return VIETNAMESE_LETTER_NAMES[chunk]
    return None


def _clean_token(raw: str) -> str:
    """Boc mep token de tach ma khoi phan trang tri, NHUNG giu nguyen
    dau ASCII vi dau ASCII co the la placeholder:
    - Boc ky tu khong phai ASCII o 2 mep (emoji, ky tu trang tri,
      vd '✨OD%KLK' -> 'OD%KLK').
    - Neu token con >1 dau dac biet thi boc dau cau o mep trong tap
      .,;:!? (vd 'AVA*MX,' -> 'AVA*MX', 'AVA*MX!' -> 'AVA*MX'): mot ma
      that chi co dung 1 dau dac biet, dau thu 2 o mep chi co the la
      dau cau chen vao. Dau dac biet don doc o mep (placeholder o vi
      tri 1/6, vd '*VA8MX') KHONG bi boc vi luc do chi co 1 dau."""
    tok = re.sub(r"^[^\x00-\x7F]+|[^\x00-\x7F]+$", "", raw)
    while len([c for c in tok if not c.isalnum()]) > 1 and tok and tok[-1] in ".,;:!?":
        tok = tok[:-1]
    while len([c for c in tok if not c.isalnum()]) > 1 and tok and tok[0] in ".,;:!?":
        tok = tok[1:]
    return tok


def _segment_tokens(segment: str):
    """Tim cac ma ung vien trong doan giua header va dong CODE 1, theo
    thu tu xuat hien. Chi nhan token DUNG 6 ky tu (KT_CODE_LENGTH),
    co DUNG 1 ky tu dac biet o BAT KY vi tri nao (1-6, ke ca dau/cuoi).
    PATCH_MARKER_2026-11-02-llwin-kt-v2 — 2 sua ngay 02/11:
    (a) BO dieu kien letters >= 3: ma 02/11 '70%G7K' chi co 2 chu cai
    (G, K) nen bi loai, dan den CODE 1 ghep nham sang 'BDFC*Q'
    (KT3 lech vi tri 5) va CODE 2 mat cap. Doan giua header va CODE 1
    da dinh vi chat che, token 6 ky tu + 1 dau dac biet o day gan nhu
    chac chan la ma, khong can them dieu kien chu cai (case 'GIAM 50%'
    van bi loai vi khong du 6 ky tu / khong co 1 dau).
    (b) Emoji/ky tu khong ASCII thanh khoang trang TRUOC khi tach, va
    tach them tren / | , ; : neu khong, '70%G7K<heart>BDFC*Q' dinh
    nhau thanh 1 token 13 ky tu, code_tokens rong -> parse tra None
    -> hook KT im lang roi ve spoiler chung (dung loi 02/11)."""
    special = []
    cleaned = re.sub(r"[^\x00-\x7F]+", " ", segment)
    for raw in re.split(r"[\s/\\|,;]+", cleaned):
        tok = _clean_token(raw)
        if not tok:
            continue
        specials = [c for c in tok if not c.isalnum()]
        if len(specials) == 1 and len(tok) == KT_CODE_LENGTH:
            special.append(tok)
    return special


def parse_llwin_spoiler(text, chat_id=None, channel_key=None):
    """Phan tich tin spoiler LLwin (v2).

    Tra None neu: kenh khong thuoc 5 kenh cho phep (3 LLwin + 2 KJC),
    hoac tin khong phai format spoiler (thieu header NHAP CODE NGAY,
    thieu ma ung vien dung 6 ky tu co 1 dau dac biet, hoac thieu dong
    CODE).
    Tra dict neu dung format:
      codes: list ma da giai (chi nhung cap giai duoc)
      pairs: chi tiet tung cap (ma goc, placeholder, KT, hau to,
        ky tu dien, ma da giai)
      warnings: list canh bao (KT lech, hau to khong hieu, ma
        khong co dau dac biet nen thay theo KT)
      safe_to_submit: True chi khi moi cap deu giai duoc, khong lech
        KT, kenh da xac minh bang chat_id. False -> chi log, KHONG
        tu submit, va cung KHONG roi ve trich ma chung.
    """
    if not text:
        return None
    if chat_id is not None or channel_key:
        if not _channel_allowed(chat_id, channel_key):
            return None
    elif chat_id is None and channel_key is None:
        # Khong co dinh danh kenh: van phan tich de test, nhung danh
        # dau de bot khong dung ket qua nay de submit.
        pass

    norm = _norm(text)
    norm = norm.translate(str.maketrans({"\uff1a": ":", "\uff0f": "/", "\uff5c": "\\", "\uff01": "!", "\uff1f": "?"}))
    # PATCH_MARKER_2026-11-02-llwin-kt-v3: header KHONG con la cong cung.
    # Tin 13:21 co dong CODE+KT nhung parse tra None o 1 trong cac dang
    # sau (da chung minh offline): header fancy/fullwidth, header mat
    # (chi la anh/sticker), ma fullwidth, CODE nam giua dong. Ca 4 dang
    # deu co CODE n: KT... nen ban than dong CODE da du dinh danh tin KT;
    # chi can ghep dung ma truoc dong CODE dau tien va KT khop vi tri
    # dau thi moi safe, khong thi van chan nhu cu.
    header = re.search(r"NHAP\s+CODE\s+NGAY", norm)
    # PATCH_MARKER_2026-11-02-llwin-kt-v3 (bo sung): 1 regex duy nhat cho
    # dong CODE, dung o dau dong hay giua dong cung duoc, va nhom (2)
    # DUNG truoc dong CODE ke tiep (lookahead) chu khong nuot den het
    # dong — neu khong, "CODE 1: ... CODE 2: ..." chung 1 dong chi
    # tim duoc CODE 1, CODE 2 mat ma khong co canh bao nao.
    code_lines = list(re.finditer(
        r"CODE\s*(\d+)\s*:\s*([^\r\n]*?)(?=\s*CODE\s*\d+\s*:|\r?\n|$)", norm))
    if not code_lines:
        return None
    if header and header.end() <= code_lines[0].start():
        segment = norm[header.end():code_lines[0].start()]
    else:
        segment = norm[:code_lines[0].start()]

    code_tokens = _segment_tokens(segment)
    if not code_tokens:
        return None

    pairs, warnings = [], []
    for m in code_lines:
        number = int(m.group(1))
        line = m.group(2).strip()
        if number < 1 or number > len(code_tokens):
            warnings.append(f"CODE {number}: khong co ma tuong ung "
                            f"(chi co {len(code_tokens)} ma)")
            continue
        raw_code = code_tokens[number - 1]
        kt = None
        kt_match = re.search(r"KT\s*(\d+)", line)
        if kt_match:
            kt = int(kt_match.group(1))
        if "_" not in line:
            warnings.append(f"CODE {number}: thieu dau _ (dong: {line!r})")
            pairs.append({"raw": raw_code, "placeholder": None, "kt": kt,
                          "suffix": None, "fill": None, "resolved": None})
            continue
        suffix = line.rsplit("_", 1)[1].strip()
        fill = _resolve_fill(suffix)
        if fill is None:
            warnings.append(f"CODE {number}: khong hieu hau to sau _ ({suffix!r})")
            pairs.append({"raw": raw_code, "placeholder": None, "kt": kt,
                          "suffix": suffix, "fill": None, "resolved": None})
            continue
        # Moi ma ung vien deu co dung 1 dau dac biet (loc o _segment_tokens).
        specials = [i for i, c in enumerate(raw_code) if not c.isalnum()]
        pos = specials[0] + 1  # 1-indexed
        placeholder = raw_code[specials[0]]
        if kt is not None and kt != pos:
            warnings.append(
                f"CODE {number}: KT{kt} lech vi tri dau dac biet ({pos}) "
                f"o ma {raw_code} — giai theo dau, khong tu submit"
            )
        resolved = raw_code[:specials[0]] + fill + raw_code[specials[0] + 1:]
        pairs.append({"raw": raw_code, "placeholder": placeholder, "kt": kt,
                      "suffix": suffix, "fill": fill, "resolved": resolved})

    codes = [p["resolved"] for p in pairs if p.get("resolved")]
    position_mismatch = any("lech vi tri" in w for w in warnings)
    unresolved = any(p.get("resolved") is None for p in pairs)
    unverified = chat_id is None and channel_key is None
    safe = bool(codes) and not unresolved and not position_mismatch and not unverified
    return {
        "codes": codes,
        "pairs": pairs,
        "warnings": warnings,
        "safe_to_submit": safe,
        "channel_unverified": unverified,
    }


def _llwin_kt_caption(event) -> str:
    msg = getattr(event, "message", None)
    for attr in ("message", "text", "raw_text"):
        v = getattr(msg, attr, None) if msg is not None else None
        if v:
            return str(v)
    return ""


def _try_llwin_kt_spoiler(event, domain: str):
    """Giai spoiler LLwin dang KT truoc khi duong spoiler chung chay.

    PATCH_MARKER_2026-11-01-llwin-kt-slim

    Tra ve:
      None               — khong phai tin KT LLwin: de duong cu xu
                           ly nhu cu.
      (codes, True)      — giai safe, duoc submit.
      (None, False)      — DUNG format KT nhung khong safe (KT lech vi
                           tri dau, hau to khong hieu, thieu cap):
                           caller phai CHAN tin nay, khong duoc roi ve
                           trich chung (trich chung se lay KTn lam ma
                           va de nguyen dau placeholder nhu vu
                           KT4HAI / MXXZ%G ngay 01/11).
    Chi goi khi domain == "llwincode.com"; parser tu kiem tra chat_id
    thuoc 3 kenh LLwin, kenh khac tra None.
    """
    if domain != "llwincode.com":
        return None
    try:
        chat_id = int(getattr(event, "chat_id", 0) or 0)
    except Exception:
        chat_id = None
    text = _llwin_kt_caption(event)
    if not text:
        return None
    try:
        result = parse_llwin_spoiler(text, chat_id=chat_id)
    except Exception as exc:
        logger.warning("[LLWIN-KT] parser loi: %s", exc)
        return None
    if result is None:
        # PATCH_MARKER_2026-11-02-llwin-kt-v2 — luoi an toan 02/11: tin
        # co dong "CODE n: KT..." la tin KT, du parser co tra None vi
        # ly do gi (token dinh nhau, header lech, ...) thi CUNG KHONG
        # duoc roi ve spoiler chung: spoiler chung se submit ma tho
        # con dau placeholder (70%G7K ngay 02/11 -> FAILED "khong ton
        # tai") va bo mat ma thu 2 (BDFC*Q khong phai spoiler entity
        # nen duong chung khong bao gio lay duoc no). Chan + log ro
        # ly do de user nhap tay, thay vi mat ca 2 ma trong im lang.
        try:
            norm_text = _norm(text)
        except Exception:
            norm_text = ""
        if re.search(r"CODE\s*\d+\s*:\s*KT", norm_text):
            _kt_m = re.search(r"CODE\s*\d+", norm_text)
            _seg_toks = []
            try:
                _seg_toks = _segment_tokens(norm_text[:_kt_m.start()]) if _kt_m else []
            except Exception:
                _seg_toks = []
            logger.warning(
                "[LLWIN-KT] dang KT nhung parser khong nhan — chan, "
                "khong roi ve spoiler chung (text_len=%s header=%s "
                "seg_tokens=%s)",
                len(text),
                bool(re.search(r"NHAP\s+CODE\s+NGAY", norm_text)),
                _seg_toks,
            )
            return (None, False)
        return None
    if result.get("safe_to_submit") and result.get("codes"):
        logger.info("[LLWIN-KT] giai KT: %s -> %s", result.get("pairs"), result["codes"])
        return (list(result["codes"]), True)
    logger.warning(
        "[LLWIN-KT] khong safe, khong submit (warnings=%s, pairs=%s)",
        result.get("warnings"), result.get("pairs"),
    )
    return (None, False)


async def _process_telegram_message_impl(event):
    if not _systems:
        await _ignore_inbox_event(event, "systems_not_ready")
        return
    if event.chat_id not in Config.CHANNEL_CONFIG:
        await _ignore_inbox_event(event, "channel_not_configured")
        return
    cfg = Config.CHANNEL_CONFIG.get(event.chat_id)
    if not cfg:
        await _ignore_inbox_event(event, "channel_config_missing")
        return
    if not cfg.get("enabled", True):
        await _ignore_inbox_event(event, "channel_disabled")
        return

    msg_date = getattr(event.message, "date", None)
    if msg_date is None:
        await _ignore_inbox_event(event, "message_date_missing")
        return

    if msg_date.tzinfo is None:
        msg_date = msg_date.replace(tzinfo=timezone.utc)

    target_url = cfg["url"]
    domain = normalize_domain(target_url)
    await _load_account_rotation(domain)
    accounts = _available_domain_accounts(domain)
    raw_text = "\n".join(
        dict.fromkeys(
            str(value).strip()
            for value in (
                getattr(event.message, "text", None),
                getattr(event.message, "message", None),
            )
            if str(value or "").strip()
        )
    )
    logger.info(f"📨 Nhận tin | {cfg['name']} | {len(raw_text)} ký tự")

    if (
        getattr(Config, "KJC_SPECIAL_MODE", True)
        and is_kjc_special_channel(event.chat_id)
    ):
        # PATCH_MARKER_2026-10-26-parallel-llwin-local-labels: per-spoiler association.
        kjc_items = extract_kjc_routed_items(event, cfg.get("name", ""))
        if not kjc_items:
            logger.info("⏭️ [KJC] Không có spoiler kèm nhãn site rõ ràng; không phát sang site khác")
            await _ignore_inbox_event(event, "kjc_no_local_label")
            return
        else:
            channel_name = cfg.get("name", "")
            labeled_domains = tuple(dict.fromkeys(item["domain"] for item in kjc_items))
            logger.info(
                "📡 [KJC] %s code-site pairs, route=local-label:%s",
                len(kjc_items), ",".join(labeled_domains),
            )

            eligible_kjc = []
            for item in kjc_items:
                item["inbox_id"] = getattr(event, "inbox_id", None)
                item["claim_token"] = getattr(event, "claim_token", None)
                item["msg_ts"] = msg_date
                eligible_kjc.append(item)

            inbox_id = getattr(event, "inbox_id", None)
            _kjc_db = _systems.get("db") if _systems else None
            if _kjc_db is not None and eligible_kjc:
                _kjc_loop = asyncio.get_running_loop()
                _kjc_kept = []
                for _kjc_domain in dict.fromkeys(i["domain"] for i in eligible_kjc):
                    _kjc_unused = await _kjc_loop.run_in_executor(
                        _DB_EXECUTOR, _kjc_db.unused_codes, _kjc_domain,
                        [i["code"] for i in eligible_kjc if i["domain"] == _kjc_domain],
                    )
                    _kjc_kept.extend(
                        i for i in eligible_kjc
                        if i["domain"] == _kjc_domain and normalize_code(i["code"], _kjc_domain) in _kjc_unused
                    )
                eligible_kjc = _kjc_kept
            for kjc_domain in dict.fromkeys(item["domain"] for item in eligible_kjc):
                await _load_account_rotation(kjc_domain)
            fanout_kjc = _build_spoiler_dispatch_items(eligible_kjc)

            if inbox_id and fanout_kjc and _durable_inbox is not None:
                item_ids = await asyncio.get_running_loop().run_in_executor(
                    _INBOX_EXECUTOR,
                    _durable_inbox.create_work_items,
                    int(inbox_id),
                    fanout_kjc,
                    getattr(event, "claim_token", None),
                )
                fanout_kjc = [
                    {**item, "work_item_id": item_id}
                    for item, item_id in zip(fanout_kjc, item_ids) if item_id
                ]

            routed_kjc = len(fanout_kjc)
            queued_kjc = 0
            deferred_kjc = 0
            for item in fanout_kjc:
                item_domain = item["domain"]
                if await _enqueue_domain_item(item):
                    queued_kjc += 1
                    logger.info("📥 [KJC] %s -> %s", item["code"], item_domain)
                else:
                    deferred_kjc += 1

            if deferred_kjc:
                logger.warning(
                    "⚠️ [KJC] %s item giữ trong DurableInbox vì domain queue đang đầy",
                    deferred_kjc,
                )
            if inbox_id and not routed_kjc:
                await _ignore_inbox_event(event, "kjc_queue_empty")
            return

    cached_codes = None
    source_kind = "unknown"
    dual_parse_media = False
    dual_spoiler_found = False

    if not accounts:
        await _ignore_inbox_event(event, "no_eligible_accounts")
        return

    event_media = getattr(event, "media", None) or getattr(
        getattr(event, "message", None), "media", None
    )
    if event_media:
        raw_text = ""
        is_video_ch = bool(cfg.get("has_video", False))
        _m = getattr(event.message, "media", None)
        is_vid = (
            isinstance(_m, MessageMediaDocument)
            and any(isinstance(a, DocumentAttributeVideo) for a in getattr(getattr(_m, "document", None), "attributes", []))
        )

        media_spoiler_started = time.perf_counter()
        # PATCH_MARKER_2026-11-01-llwin-kt-slim: giai KT TRUOC khi lay
        # token spoiler chung. Duong chung lay KT4_HAI lam ma va de
        # nguyen MXXZ%G (xem anh 01/11) vi no khong hieu format
        # "NHAP CODE NGAY : <ma co dau> / CODE n: KTp_<gia tri>".
        kt = _try_llwin_kt_spoiler(event, domain) if domain == "llwincode.com" else None
        if kt is not None and kt[1]:
            media_spoiler_codes = list(kt[0])
        elif kt is not None and not kt[1]:
            await _ignore_inbox_event(event, "llwin_kt_unsafe")
            return
        else:
            raw_media_spoilers = extract_spoiler_codes(event, target_url)
            media_spoiler_codes = (
                None if raw_media_spoilers is None
                else _filter_channel_codes(raw_media_spoilers, cfg, source="spoiler")
            )
        dual_parse_media = domain in (
            "xx88code.com", "getcodexx8bonus.pages.dev", "llwincode.com",
        )
        dual_spoiler_found = bool(media_spoiler_codes)
        if media_spoiler_codes is not None and media_spoiler_codes:
            cached_codes = unique_keep_order(media_spoiler_codes, domain)
            source_kind = "spoiler"
            media_spoiler_elapsed_ms = (time.perf_counter() - media_spoiler_started) * 1000.0
            logger.warning(
                "🎯 [MEDIA-SPOILER FAST] %.1f ms | %s: %s codes %s — spoiler xếp trước, kênh video vẫn OCR",
                media_spoiler_elapsed_ms,
                cfg.get("name", ""),
                len(cached_codes),
                cached_codes,
            )

            event_media = None

        if event_media is None and cached_codes is not None:

            pass
        elif event_media:

            caption = "\n".join(
                dict.fromkeys(
                    str(value).strip()
                    for value in (
                        getattr(event.message, "message", None),
                        getattr(event.message, "text", None),
                    )
                    if str(value or "").strip()
                )
            )
            found_caption = False

            _caption_skips_ocr = int(event.chat_id) not in _OCR_ONLY_CHANNEL_IDS
            if caption and _caption_skips_ocr:
                ex = extract_codes_from_message(
                    event, caption, target_url, channel_name=cfg.get("name", ""),
                    spoiler_codes=[],
                )
                ex = _filter_channel_codes(ex, cfg)
                if ex:
                    raw_text = caption
                    found_caption = True
                    cached_codes = ex
                    source_kind = "text"

        if cached_codes is None:

            ocr_allowed = _is_ocr_allowed_channel(event.chat_id, event)
            if found_caption:
                pass
            elif not ocr_allowed and not dual_parse_media:
                logger.info(f"⏭️ [{cfg.get('name')}] media/OCR không được phép — chỉ xử lý spoiler/text")
                await _ignore_inbox_event(event, "media_not_ocr_allowed")
                return
            else:

                if is_vid and cfg.get("ocr_video_enabled") is False and not dual_parse_media:
                    await _ignore_inbox_event(event, "ocr_video_disabled")
                    return
                acc = accounts[0]["username"] if accounts else None
                if not acc:
                    await _ignore_inbox_event(event, "ocr_no_account")
                    return
                if dual_parse_media:
                    ocr_queued = await _enqueue_ocr_media_job(event, cfg, _systems, acc)
                    logger.info(
                        "DUAL-PARSE domain=%s spoiler_found=%s ocr_queued=%s",
                        domain, dual_spoiler_found, ocr_queued,
                    )
                    return
                if is_video_ch or is_vid:
                    await _enqueue_ocr_media_job(event, cfg, _systems, acc)
                    return
                if not raw_text:
                    c2 = "\n".join(
                        dict.fromkeys(
                            str(value).strip()
                            for value in (
                                getattr(event.message, "message", None),
                                getattr(event.message, "text", None),
                            )
                            if str(value or "").strip()
                        )
                    )
                    if c2:
                        ex = extract_codes_from_message(
                            event, c2, target_url, channel_name=cfg.get("name", ""),
                            spoiler_codes=[],
                        )
                        ex = _filter_channel_codes(ex, cfg)
                        if ex:
                            raw_text = c2
                            cached_codes = ex
                            source_kind = "text"
                        else:
                            await _enqueue_ocr_media_job(event, cfg, _systems, acc)
                            return
                    else:
                        await _enqueue_ocr_media_job(event, cfg, _systems, acc)
                        return
    if dual_parse_media and cached_codes is not None:
        # PATCH_MARKER_2026-11-01-lag-fix: spoiler da co ma + media khong
        # phai video thi bo OCR kep. Anh khong sinh them ma ngoai caption;
        # OCR chi ton download + CPU, tranh ket noi Telethon voi tin moi.
        # Video giu nguyen OCR vi ma trong khung hinh khac ma caption.
        # PATCH_MARKER_2026-11-01-lag-fix-v2: NGOAI LE — kenh LLwin PHAT
        # CODE MIEN PHI (-1003873843218) KHONG bi skip: kenh nay giu
        # nguyen OCR song song voi spoiler (user chot 01/11). Spoiler van
        # di truoc vi no vao hang doi ngay luc nhan tin (MEDIA-SPOILER
        # FAST), OCR chay sau bang worker rieng, va OCR cua dung kenh
        # nay da co _ocr_exclude_codes = ma spoiler nen khong nhap lai
        # ma spoiler.
        if not is_vid and source_kind == "spoiler" and int(event.chat_id) != -1003873843218:
            logger.info(
                "DUAL-PARSE domain=%s spoiler_found=%s ocr_queued=%s (skip: spoiler + non-video)",
                domain, dual_spoiler_found, False,
            )
        else:
            acc = sorted(accounts, key=lambda a: a.get("priority", 999))[0]["username"]
            ocr_cfg = cfg
            if domain == "llwincode.com" and int(event.chat_id) == -1003873843218 and source_kind == "spoiler":
                ocr_cfg = {**cfg, "_ocr_exclude_codes": tuple(cached_codes)}
            ocr_queued = await _enqueue_ocr_media_job(
                event, ocr_cfg, _systems, acc, track_inbox=False,
            )
            logger.info(
                "DUAL-PARSE domain=%s spoiler_found=%s ocr_queued=%s",
                domain, dual_spoiler_found, ocr_queued,
            )
    msg_ts = event.message.date
    delay = measure_telegram_delay_fast(msg_ts)
    mk = (event.chat_id, event.message.id)

    ch = _message_content_hash(
        f"{raw_text}|{'|'.join(cached_codes or [])}",
        _spoiler_entity_signature(event.message),
    )
    prev = bot_state._processed_message_hashes.get(mk)
    if prev and prev[0] == ch and not getattr(event, "inbox_id", None):
        return
    bot_state._processed_message_hashes[mk] = (ch, time.time())

    if cached_codes is not None:
        final_codes = list(cached_codes)
    else:

        # PATCH_MARKER_2026-11-01-llwin-kt-slim: cung thu tu KT-truoc
        # nhu hook media o tren, cho tin LLwin khong phai media.
        kt = _try_llwin_kt_spoiler(event, domain) if domain == "llwincode.com" else None
        if kt is not None and kt[1]:
            final_codes = unique_keep_order(kt[0], domain)
            source_kind = "spoiler"
        elif kt is not None and not kt[1]:
            await _ignore_inbox_event(event, "llwin_kt_unsafe")
            return
        elif (direct_spoilers := extract_spoiler_codes(event, target_url)):
            final_codes = unique_keep_order(direct_spoilers, domain)
            source_kind = "spoiler"
        else:
            final_codes = extract_codes_from_message(
                event, raw_text, target_url, channel_name=cfg.get("name", ""),
                spoiler_codes=[],
            )
            source_kind = "text"

    final_codes = _filter_channel_codes(final_codes, cfg)
    if not final_codes:
        await _ignore_inbox_event(event, "no_code")
        return

    final_codes = unique_keep_order(final_codes, domain)
    _random_cap = None
    _llwin_spoiler_cap = False
    if (domain == "llwincode.com"
            and int(event.chat_id) == -1003873843218
            and source_kind == "spoiler"):
        # PATCH_MARKER_2026-11-01-spoiler-cap-2: kenh LLwin PHAT CODE MIEN
        # PHI: spoiler chi nhap ngau nhien 2 ma (user chot 01/11). Cap dat
        # SAU khi loc ma da dung nen 2 suat khong bi phi vao ma cu.
        # Ma OCR di duong submit_codes_from_image rieng, khong qua cap
        # nay nen khong bi gioi han 2.
        _random_cap = 2
        _llwin_spoiler_cap = True
    elif (bool(getattr(Config, "SOURCE_AWARE_DISPATCH", True))
            and source_kind != "spoiler" and domain not in ("xx88code.com", "getcodexx8bonus.pages.dev")):
        _random_cap = 1

    logger.info(
        "✅ Đã nhận code | %s | %s | source=%s",
        cfg['name'], ', '.join(final_codes), source_kind,
    )
    for c in final_codes:
        append_code_history(
            event_type="DETECTED", code=c, target_url=target_url,
            channel=cfg.get("name", ""), source=source_kind,
            status="PENDING", telegram_delay=delay,
        )

    dedup = []
    db = _systems["db"] if _systems else None
    if db is not None:
        try:
            loop = asyncio.get_running_loop()
            unused = await loop.run_in_executor(_DB_EXECUTOR, db.unused_codes, domain, final_codes)
            dedup = [c for c in final_codes if normalize_code(c, domain) in unused]
        except Exception as e:
            logger.error(
                "❌ Không kiểm tra được batch dedup, giữ tin để retry: %s",
                e,
            )
            raise RuntimeError("Code dedup temporarily unavailable") from e
    else:
        dedup = list(final_codes)

    if _random_cap is not None:
        dedup = scheduling.random_codes(dedup, _random_cap)
        if _llwin_spoiler_cap:
            logger.info(
                "[SPOILER-CAP] LLwin PHAT CODE MIEN PHI: spoiler %s ma -> nhap ngau nhien 2: %s",
                len(final_codes), dedup,
            )
    if not dedup:
        logger.info(
            f"⏭️ [{domain}] bỏ {', '.join(final_codes)}: code đã dùng/đã gửi trước đó (dedup vĩnh viễn)"
        )

        await _ignore_inbox_event(event, "duplicate_or_used_code")
        return

    avail = sorted(accounts, key=lambda a: a.get("priority", 999))
    if not avail:
        logger.warning(f"⏭️ [{domain}] bỏ {', '.join(dedup)}: không có tài khoản khả dụng")
        await _ignore_inbox_event(event, "no_account")
        return

    cn = cfg.get("name", "")
    inbox_id = getattr(event, "inbox_id", None)
    base_dispatch_items = [
        {
            "code": c,
            "channel_name": cn,
            "chat_id": int(event.chat_id),
            "domain": domain,
            "target_url": target_url,
            "inbox_id": inbox_id,
            "claim_token": getattr(event, "claim_token", None),
            "msg_ts": msg_date,
            "source_kind": source_kind,
            "source": source_kind,
            "fanout": False,
            "fanout_index": 0,
        }
        for c in dedup
    ]
    if _stateless_entry_enabled():
        dispatch_items = _build_stateless_entry_items(base_dispatch_items)
    elif source_kind == "spoiler":
        dispatch_items = _build_spoiler_dispatch_items(
            base_dispatch_items, domain, spoiler_codes=dedup,
        )
    else:
        text_items = base_dispatch_items[:1]
        dispatch_items = _build_distinct_code_items(text_items, domain, source="text")
    n = len(dispatch_items)
    if not n:
        await _ignore_inbox_event(event, "no_eligible_accounts")
        return
    if inbox_id and n and _durable_inbox is not None:
        item_ids = await asyncio.get_running_loop().run_in_executor(
            _INBOX_EXECUTOR,
            _durable_inbox.create_work_items,
            int(inbox_id),
            dispatch_items,
            getattr(event, "claim_token", None),
        )
        dispatch_items = [
            {**item, "work_item_id": item_id}
            for item, item_id in zip(dispatch_items, item_ids) if item_id
        ]
    queued = 0
    deferred = 0
    for item in dispatch_items:

        if await _enqueue_domain_item(item):
            queued += 1
        else:
            deferred += 1
    logger.info(
        "📥 Đã phân luồng | %s code | %s lượt | queued=%s deferred=%s | %s",
        len(dedup), n, queued, deferred, domain,
    )

_raw_ingest_pending = set()
_async_ingest_counts = {
    "INGEST_BACKPRESSURE": 0, "OCR_DROP_OLD": 0, "SPOILER_NO_LOCK": 0,
    "queue_drop_stale": 0,
}


async def _offer_raw_ingest(item):
    """Apply backpressure to the bounded worker queue without evicting accepted work."""
    # PATCH_MARKER_2026-10-26-v6-async-ingest
    if RAW_INGEST_QUEUE.full():
        _async_ingest_counts["INGEST_BACKPRESSURE"] += 1
    await RAW_INGEST_QUEUE.put(item)


async def process_telegram_message(event):
    """Durably enqueue a push before returning to Telethon."""
    chat_id = getattr(event, "chat_id", None)
    message = getattr(event, "message", None)
    if chat_id not in Config.CHANNEL_CONFIG or message is None:
        return
    edited = bool(getattr(event, "_raw_ingest_edited", False))
    receiver = _receiver
    if receiver is None:
        raise RuntimeError("Telegram push received before receiver initialization")
    await receiver.on_push(event, edited=edited)


async def _process_telegram_message_traced(event):
    timer = RequestTimer.from_event(event, kind="telegram_request")
    queue_wait_ms = getattr(event, "queue_wait_ms", None)
    if queue_wait_ms is not None:
        queue_wait_seconds = max(0.0, float(queue_wait_ms)) / 1000.0
        timer.started_perf -= queue_wait_seconds
        timer.stages["ingress_queue_wait"] = queue_wait_seconds
    tok = set_current_timer(timer)
    cfg = Config.CHANNEL_CONFIG.get(getattr(event, "chat_id", None))
    tag = get_site_log_tag(cfg.get("url", "")) if cfg else f"chat_{getattr(event,'chat_id','?')}"
    lt = set_log_context(tag)
    try:
        with timer.stage("process_message"):
            r = await _process_telegram_message_impl(event)
        timer.finish("ok")
        if any(isinstance(entity, MessageEntitySpoiler)
               for entity in (getattr(event.message, "entities", None) or [])):
            logger.info(
                "[SPOILER-FLOW] chat=%s message=%s age_s=%s queue_ms=%.1f process_ms=%.1f",
                event.chat_id, event.message.id, measure_telegram_delay_fast(event.message.date),
                float(queue_wait_ms or 0), timer.stages.get("process_message", 0) * 1000,
            )
        return r
    except asyncio.CancelledError:
        timer.finish("cancelled")
        raise
    except Exception as e:
        timer.finish("error", error_type=type(e).__name__)
        raise
    finally:
        reset_current_timer(tok)
        reset_log_context(lt)


async def message_worker(wid: int):
    logger.info(f"👷 Worker #{wid}")

    while bot_state.is_running or (message_queue is not None and not message_queue.empty()):
        row_id = None
        claimed_row_id = None
        row = None
        claimed_token = None
        queue_age_ms = None
        queued_at_perf = None
        try:
            item = await asyncio.wait_for(message_queue.get(), timeout=1.0)
            if isinstance(item, RawIngressItem):
                try:
                    if _receiver is not None:
                        _receiver._raw_offer_seen_perf = item.seen_perf
                        await _receiver.on_push(item.event, edited=item.edited)
                finally:
                    if _receiver is not None:
                        _receiver._raw_offer_seen_perf = None
                    _raw_ingest_pending.discard(item.key)
                    message_queue.task_done()
                continue
            if isinstance(item, tuple) and len(item) == 2:
                queued_at_perf, row_id = item
                queue_age_ms = (time.perf_counter() - queued_at_perf) * 1000
                if queue_age_ms > 500:
                    logger.warning(
                        "🐌 Telegram message queue delay %.0fms | qsize=%s/%s",
                        queue_age_ms,
                        message_queue.qsize(),
                        message_queue.maxsize,
                    )
            else:
                row_id = item
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ worker#{wid} lấy message lỗi: {e}")
            await asyncio.sleep(0.2)
            continue

        try:
            async with _get_proc_semaphore():
                inbox = _durable_inbox
                if inbox is None:
                    bot_state._inbox_enqueued_ids.discard(int(row_id))
                    logger.warning(
                        "⚠️ [Worker #%s] DurableInbox chưa sẵn sàng — "
                        "giữ row=%s để drain lại sau khi inbox khởi tạo",
                        wid,
                        row_id,
                    )
                    continue
                bot_state._inbox_enqueued_ids.discard(int(row_id))
                row = await asyncio.get_running_loop().run_in_executor(_INBOX_EXECUTOR, inbox.claim, int(row_id))
                if not row:
                    continue
                claimed_row_id = int(row_id)
                claimed_token = row.get("claim_token")

                row_cfg = (getattr(Config, "CHANNEL_CONFIG", None) or {}).get(int(row["chat_id"]), {})
                row_domain = normalize_domain(row_cfg.get("url", "")) if row_cfg else ""
                row_max_age = float((getattr(Config, "SITE_CODE_MAX_AGE_SECONDS", None) or {}).get(
                    row_domain, getattr(Config, "CODE_MAX_AGE_SECONDS", 120.0)))
                _stale, _age = first_known_age(
                    (row.get("message_date"), row.get("created_at")),
                    row_max_age,
                )
                if _stale:
                    _inbox_message_cache.pop(int(row_id), None)
                    await asyncio.get_running_loop().run_in_executor(
                        _INBOX_EXECUTOR,
                        inbox.mark_ignored,
                        int(row_id),
                        "stale_message",
                        claimed_token,
                    )
                    logger.info(
                        "⏭️ [Worker #%s] WORKER_DROP_OLD bỏ tin cũ %.0fs (> %.0fs) row=%s",
                        wid, _age, row_max_age, row_id,
                    )
                    continue

                msg = _inbox_message_cache.pop(int(row_id), None)
                if msg is None:
                    if await asyncio.get_running_loop().run_in_executor(
                        _INBOX_STATE_EXECUTOR,
                        inbox.has_active_work_items,
                        int(row_id),
                    ):

                        continue
                    # Telethon may sleep/retry during disconnects or flood
                    # waits. Never let replay occupy every message worker
                    # indefinitely while fresh spoiler messages are queued.
                    msg = await asyncio.wait_for(
                        client.get_messages(int(row["chat_id"]), ids=int(row["message_id"])),
                        timeout=max(0.1, float(getattr(Config, "CHANNEL_POLL_FETCH_TIMEOUT", 8.0))),
                    )
                if not msg:

                    await asyncio.get_running_loop().run_in_executor(
                        _INBOX_EXECUTOR,
                        inbox.retry_or_fail,
                        int(row_id),
                        "Telegram message unavailable",
                        None,
                        claimed_token,
                    )
                    continue
                ev = SimpleNamespace(
                    chat_id=int(row["chat_id"]),
                    message=msg,
                    media=getattr(msg, "media", None),
                    inbox_id=int(row_id),
                    claim_token=claimed_token,
                    queue_wait_ms=(
                        (time.perf_counter() - queued_at_perf) * 1000
                        if queued_at_perf is not None else None
                    ),
                )
                await _process_telegram_message_traced(ev)
                await asyncio.get_running_loop().run_in_executor(
                    _INBOX_EXECUTOR,
                    inbox.mark_ignored_if_empty,
                    int(row_id),
                    "no_code_or_not_routed",
                    claimed_token,
                )
        except asyncio.CancelledError:

            if claimed_row_id is not None and _durable_inbox is not None:
                try:
                    current = await asyncio.get_running_loop().run_in_executor(
                        _INBOX_EXECUTOR,
                        _durable_inbox.get,
                        claimed_row_id,
                    )
                    if current and current.get("status") == "processing":
                        await asyncio.get_running_loop().run_in_executor(
                            _INBOX_EXECUTOR,
                            _durable_inbox.retry,
                            claimed_row_id,
                            "worker cancelled during shutdown",
                            0,
                            claimed_token,
                        )
                        logger.warning(
                            "♻️ [Inbox] requeue row=%s vì worker bị hủy",
                            claimed_row_id,
                        )
                except Exception:
                    logger.exception(
                        "❌ [Inbox] không requeue được row=%s khi worker bị hủy",
                        claimed_row_id,
                    )
            raise
        except FloodWaitError as exc:
            wait = max(0, int(exc.seconds))
            if row_id is not None and _durable_inbox is not None:
                await asyncio.get_running_loop().run_in_executor(
                    _INBOX_EXECUTOR, _durable_inbox.retry,
                    int(row_id), "ingest FloodWait", wait, claimed_token)
            logger.warning("[Ingest] FloodWait=%ss; callback remains nonblocking", wait)
            await asyncio.sleep(wait)
        except Exception as e:
            logger.error(f"❌ worker#{wid}: {e}")
            try:
                if _durable_inbox is not None:

                    await asyncio.get_running_loop().run_in_executor(
                        _INBOX_EXECUTOR,
                        _durable_inbox.retry_or_fail,
                        int(row_id),
                        str(e),
                        None,
                        claimed_token,
                    )
            except Exception:
                logger.exception("❌ [Worker #%s] retry_or_fail lỗi, row=%s", wid, row_id)
        finally:
            try:
                message_queue.task_done()
            except Exception:
                pass


async def _inbox_drain_loop():
    global _inbox_wakeup
    if _inbox_wakeup is None:
        _inbox_wakeup = asyncio.Event()
    while bot_state.is_running:
        try:
            drained_count = 0
            inbox = _durable_inbox

            queue_has_capacity = (
                message_queue is not None
                and not message_queue.full()
                and (
                    message_queue.maxsize <= 1
                    or message_queue.qsize() < int(message_queue.maxsize * 0.90)
                )
            )
            if inbox is not None:
                due_items = await asyncio.get_running_loop().run_in_executor(
                    _INBOX_STATE_EXECUTOR,
                    inbox.due_work_items,
                    int(getattr(Config, "TELEGRAM_INBOX_DRAIN_BATCH", 250)),
                    tuple(domain for domain, queue in _domain_queues.items() if queue.full()),
                    tuple(Config.DOMAIN_TO_CHANNEL_URL),
                )
                for durable_item in due_items:
                    item_id = int(durable_item["id"])
                    if item_id in bot_state._work_item_enqueued_ids:
                        continue
                    domain = str(durable_item.get("domain") or "")
                    if not domain:
                        continue
                    item = {
                        "work_item_id": item_id,
                        "inbox_id": int(durable_item["inbox_id"]),
                        "claim_token": durable_item.get("parent_claim_token"),
                        "code": durable_item["code"],
                        "msg_ts": durable_item.get("message_date"),
                        "domain": domain,
                        "target_url": durable_item.get("target_url") or "",
                        "fanout": bool(int(durable_item.get("fanout") or 0)),
                        "fanout_index": int(durable_item.get("fanout_index") or 0),
                        "pin_account": bool(int(durable_item.get("pin_account") or 0)),
                        "chat_id": int(durable_item.get("chat_id") or 0),
                        "source_kind": str(durable_item.get("source_kind") or "unknown"),
                    }
                    q = get_domain_queue(domain)
                    try:
                        item["_queued_perf"] = time.perf_counter()
                        q.put_nowait(item)
                        bot_state._work_item_enqueued_ids.add(item_id)
                        drained_count += 1
                    except asyncio.QueueFull:
                        continue
                if queue_has_capacity:
                    ids = await asyncio.get_running_loop().run_in_executor(
                        _INBOX_EXECUTOR,
                        inbox.pending_ids,
                        int(getattr(Config, "TELEGRAM_INBOX_DRAIN_BATCH", 250)),
                        tuple(Config.CHANNEL_CONFIG),
                    )
                    for row_id in ids:
                        if message_queue.full():
                            break
                        if int(row_id) in bot_state._inbox_enqueued_ids:
                            continue
                        try:
                            message_queue.put_nowait((time.perf_counter(), int(row_id)))
                            bot_state._inbox_enqueued_ids.add(int(row_id))
                            drained_count += 1
                        except asyncio.QueueFull:
                            break

            busy_interval = max(0.02, float(getattr(Config, "TELEGRAM_INBOX_DRAIN_INTERVAL", 0.05)))
            idle_interval = max(busy_interval, float(getattr(Config, "TELEGRAM_INBOX_IDLE_DRAIN_INTERVAL", 0.50)))
            interval = busy_interval if drained_count else idle_interval
            try:
                await asyncio.wait_for(_inbox_wakeup.wait(), timeout=interval)
                _inbox_wakeup.clear()
            except asyncio.TimeoutError:
                pass
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.debug("⚠️ [Inbox] drain lỗi: %s", exc)
            await asyncio.sleep(1.0)


async def ingest_worker(wid: int, ready: asyncio.Event | None = None):
    """Supervise a message worker consuming the shared bounded queue."""
    if ready is not None:
        ready.set()
    while bot_state.is_running:
        try:
            await message_worker(wid)
            if bot_state.is_running:
                logger.warning("Worker #%s exited unexpectedly; restarting", wid)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Worker #%s crashed; restarting", wid)
        if bot_state.is_running:
            await asyncio.sleep(1.0)


_async_pipeline_stats_task = None


async def _async_pipeline_stats():
    """30-second in-memory snapshot; never polls CDP or the Telegram network.

    Số liệu kỹ thuật (dict dài) chỉ ghi ở mức DEBUG để file log không phình ra.
    Mức thường chỉ nhắc khi hàng đợi bị dồn, tối đa 1 lần / 5 phút.
    """
    last_backlog_warn = 0.0
    while bot_state.is_running:
        await asyncio.sleep(30.)
        try:
            pool = getattr(browser_engine, "_tab_pool", None) if _BROWSER_ENGINE_OK else None
            tabs = {d: sum(bool(e.get("reserved") or e["lock"].locked()) for e in entries)
                    for d, entries in getattr(pool, "_domains", {}).items()}
            available = {d: len(_available_domain_accounts(d)) for d in _domain_accounts}
            counters = {**_async_ingest_counts,
                        **getattr(browser_engine, "ASYNC_SUBMIT_COUNTS", {})}
            ingest_q = RAW_INGEST_QUEUE.qsize()
            ocr_q = _ocr_media_queue.qsize() if _ocr_media_queue is not None else 0
            domain_q = {d: q.qsize() for d, q in _domain_queues.items()}
            logger.debug("[Async-Ingest] ingest_qsize=%s ingress_pending=%s ocr_qsize=%s domain_qsize=%s "
                         "active_tabs=%s accounts_available=%s counters=%s",
                         ingest_q, getattr(_ingress_stage, "pending", 0),
                         ocr_q, domain_q, tabs, available, counters)
            backlog = max([ingest_q, ocr_q, *domain_q.values()])
            now_mono = time.monotonic()
            if backlog >= 20 and now_mono - last_backlog_warn >= 300.0:
                last_backlog_warn = now_mono
                logger.warning(
                    "⚠️ Hàng đợi đang dồn: chờ xử lý=%s, chờ đọc ảnh=%s, chờ gửi code=%s",
                    ingest_q, ocr_q, sum(domain_q.values()),
                )
        except Exception:
            logger.exception("[Async-Ingest] snapshot failed")


async def start_message_workers():
    global message_queue, _inbox_wakeup, _async_pipeline_stats_task

    n = max(1, int(getattr(Config, "MESSAGE_WORKERS", 6)))

    if message_queue is None:
        message_queue = RAW_INGEST_QUEUE
        qm = get_queue_manager()
        if qm is not None:
            qm.register("message", message_queue, on_drop=_on_message_queue_drop)

    message_workers[:] = [task for task in message_workers if not task.done()]

    if _inbox_wakeup is None:
        _inbox_wakeup = asyncio.Event()

    while len(message_workers) < n:
        active_ids = {
            int(match.group(1))
            for task in message_workers
            if (match := re.fullmatch(r"ingest-worker-(\d+)", task.get_name()))
        }
        worker_id = next(worker_id for worker_id in range(1, n + 1) if worker_id not in active_ids)
        spawned = False
        for attempt in range(1, 4):
            ready = asyncio.Event()
            try:
                task = asyncio.create_task(
                    ingest_worker(worker_id, ready), name=f"ingest-worker-{worker_id}"
                )
            except RuntimeError as exc:
                logger.warning(
                    "Worker %s spawn attempt %s/3 failed: %s",
                    worker_id, attempt, exc,
                )
                await asyncio.sleep(0.1 * attempt)
                continue
            message_workers.append(task)
            startup_results = await asyncio.gather(
                _wait_message_worker_start(task, ready),
                return_exceptions=True,
            )
            startup_error = next(
                (result for result in startup_results if isinstance(result, BaseException)),
                None,
            )
            if startup_error is not None:
                logger.warning(
                    "Worker %s startup attempt %s/3 failed: %s",
                    worker_id, attempt, startup_error,
                )
            if not task.done():
                spawned = True
                break
            message_workers.remove(task)
            if not task.cancelled():
                error = task.exception()
                logger.warning(
                    "Worker %s exited during startup attempt %s/3: %s",
                    worker_id, attempt, error or "worker exited",
                )
            else:
                logger.warning(
                    "Worker %s was cancelled during startup attempt %s/3",
                    worker_id, attempt,
                )
            await asyncio.sleep(0.1 * attempt)
        if not spawned:
            raise RuntimeError(
                f"Could not start configured message worker {worker_id} after 3 attempts"
            )

    global _inbox_drain_task
    if _inbox_drain_task is None or _inbox_drain_task.done():
        _inbox_drain_task = asyncio.create_task(_inbox_drain_loop(), name="inbox-drain")
    if _async_pipeline_stats_task is None or _async_pipeline_stats_task.done():
        _async_pipeline_stats_task = asyncio.create_task(_async_pipeline_stats(), name="async-ingest-stats")

    logger.info("🚀 %s workers started", len(message_workers))


async def _wait_message_worker_start(task: asyncio.Task, ready: asyncio.Event) -> None:
    await ready.wait()
    await asyncio.sleep(0)
    if task.done():
        task.result()


def clear_old_message_queue() -> int:

    if message_queue is None:
        return 0

    removed = 0
    while True:
        try:
            item = message_queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        try:
            if isinstance(item, RawIngressItem):
                _raw_ingest_pending.discard(item.key)
            elif isinstance(item, tuple) and len(item) == 2:
                bot_state._inbox_enqueued_ids.discard(int(item[1]))
                _inbox_message_cache.pop(int(item[1]), None)
        except (TypeError, ValueError):
            pass
        try:
            message_queue.task_done()
        except Exception:
            pass

        removed += 1

    if removed:
        logger.warning(
            "🧹 Đã xóa %s tin cũ khỏi message queue",
            removed,
        )

    return removed


async def _clear_runtime_state(**ctx) -> str:

    lines = ["🧹 DỌN DẸP RUNTIME"]

    mq_removed = clear_old_message_queue()
    lines.append(f"• Message queue: đã xoá {mq_removed} item")

    domain_removed_total = 0
    for domain, q in list(_domain_queues.items()):
        removed = 0
        while True:
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                q.task_done()
            except Exception:
                pass
            removed += 1
        domain_removed_total += removed
        if removed:
            lines.append(f"  - domain '{domain}': {removed} item")
    lines.append(f"• Domain queues: đã xoá {domain_removed_total} item")

    counts = {
        "site_code_seen": len(bot_state._site_code_seen),
        "processed_hash": len(bot_state._processed_message_hashes),
        "inflight_codes": len(bot_state._inflight_codes),
        "inflight_accounts": len(bot_state._inflight_accounts),
        "inbox_ids": len(bot_state._inbox_enqueued_ids),
        "message_cache": len(_inbox_message_cache),
    }
    bot_state._site_code_seen.clear()
    bot_state._processed_message_hashes.clear()
    bot_state._inbox_enqueued_ids.clear()
    bot_state._work_item_enqueued_ids.clear()
    _inbox_message_cache.clear()
    lines.append(
        "• Cache RAM: " + ", ".join(f"{key}={value}" for key, value in counts.items())
    )

    if _durable_inbox is not None:
        n = await asyncio.get_running_loop().run_in_executor(
            _INBOX_EXECUTOR,
            _durable_inbox.discard_unfinished,
            "manual_clear_command",
        )
        lines.append(f"• Durable inbox: đã ignore {n} dòng pending/processing")
    else:
        lines.append("• Durable inbox: chưa sẵn sàng — bỏ qua")

    lines.append("✅ Xong — code_history.db được GIỮ NGUYÊN")
    return "\n".join(lines)


async def setup_telegram_handler():

    global _receiver, _ingress_stage, _ingress_stage_task, _channel_poll_task

    if bot_state.handler_registered:
        return

    chats = []
    for k in Config.CHANNEL_CONFIG.keys():
        try:
            chats.append(int(k))
        except Exception:
            continue

    if not chats:
        logger.error("❌ Không có chat nào trong CHANNEL_CONFIG")
        return
    source_filter = bool(getattr(Config, "TELEGRAM_FILTER_AT_SOURCE", True))

    clear_old_message_queue()
    await start_message_workers()

    await asyncio.sleep(0)

    def accept(message) -> bool:

        message_date = getattr(message, "date", None)
        if message_date is None:
            return False
        if message_date.tzinfo is None:
            message_date = message_date.replace(tzinfo=timezone.utc)
        cutoff = BOT_START_TIME - timedelta(seconds=30)
        if getattr(Config, "TELEGRAM_CATCH_UP", False) or _RESTARTED_BY_LAUNCHER:
            cutoff = BOT_START_TIME - timedelta(minutes=5)
        return message_date >= cutoff

    def accept_backfill(message) -> bool:

        return getattr(message, "date", None) is not None

    def build_row(env) -> tuple:
        msg = env.message
        text = str(getattr(msg, "text", None) or getattr(msg, "message", None) or "")
        has_media = bool(getattr(msg, "media", None))
        content_hash = DurableInbox.content_hash(
            text, has_media, _spoiler_entity_signature(msg), media_id=_telegram_media_id(env)
        )
        return (
            env.chat_id,
            int(getattr(msg, "id", 0) or 0),
            getattr(msg, "date", None),
            env.edited,
            text,
            has_media,
            content_hash,
        )

    def persist(rows: list) -> list:
        inbox = _durable_inbox
        if inbox is None:
            return [None] * len(rows)
        return inbox.enqueue_many(rows)

    def persist_checkpointed(rows: list, chat_id: int, message_id: int, edit_ts: int):
        inbox = _durable_inbox
        if inbox is None:
            return None
        return inbox.enqueue_many_and_checkpoint(
            rows,
            chat_id=chat_id,
            message_id=message_id,
            edit_ts=edit_ts,
        )

    async def on_row(env, inbox_id: int) -> None:

        msg = env.message

        _inbox_message_cache[inbox_id] = msg
        if len(_inbox_message_cache) > _INBOX_MSG_CACHE_MAX:
            for cache_id in list(_inbox_message_cache)[: max(1, _INBOX_MSG_CACHE_MAX // 10)]:
                _inbox_message_cache.pop(cache_id, None)
        await _offer_raw_ingest((time.perf_counter(), inbox_id))
        bot_state._inbox_enqueued_ids.add(inbox_id)
        if _inbox_wakeup is not None:
            _inbox_wakeup.set()
        bot_state.last_accepted_message_at = time.monotonic()
        bot_state.accepted_message_count += 1
        cfg = Config.CHANNEL_CONFIG.get(env.chat_id, {})
        logger.info(
            "📨 [Telegram accepted] chat_id=%s name=%s message_id=%s media=%s source=%s edited=%s "
            "age=%.1fs ingress_to_durable_ms=%.1f queue=%s/%s",
            env.chat_id,
            cfg.get("name", ""),
            getattr(msg, "id", "?"),
            bool(getattr(msg, "media", None)),
            env.source,
            env.edited,
            env.age_s,
            (time.perf_counter() - env.seen_perf) * 1000.0,
            message_queue.qsize(),
            message_queue.maxsize,
        )

    stage = IngressStage(
        build_row=build_row,
        persist=persist,
        on_row=on_row,
        persist_checkpointed=persist_checkpointed,
        executor=_INBOX_INGRESS_EXECUTOR,
        maxsize=int(getattr(Config, "INGEST_QUEUE_MAXSIZE", 512)),
        retries=int(getattr(Config, "TELEGRAM_INBOX_ENQUEUE_RETRIES", 3) or 3),
        log=logger,
    )
    push_health = (TelegramPushHealth(
        client, logger,
        stall_seconds=float(getattr(Config, "TELEGRAM_PUSH_STALL_SECONDS", 900)),
        cooldown_seconds=float(getattr(Config, "TELEGRAM_PUSH_RECOVERY_COOLDOWN", 300)),
    ) if bool(getattr(Config, "TELEGRAM_PUSH_HEALTH_ENABLED", True)) else None)
    receiver = TelegramReceiver(
        client,
        chats,
        stage.put,
        accept=accept,
        accept_backfill=accept_backfill,
        commit_backfill=stage.commit_backfill_batch,
        bootstrap_checkpoint=(
            _durable_inbox.initialize_poll_checkpoint
            if _durable_inbox is not None else None
        ),
        cfg=ReceiverConfig.from_config(Config),
        log=logger,
        push_health=push_health,
    )
    if _durable_inbox is not None:
        checkpoints = await asyncio.get_running_loop().run_in_executor(
            _INBOX_INGRESS_EXECUTOR, _durable_inbox.load_poll_checkpoints
        )
        receiver.restore_checkpoints(checkpoints)
        logger.info("📍 [Poll] đã nạp checkpoint bền cho %s channel", len(checkpoints))

    async def h_new(ev):
        ev._raw_ingest_edited = False
        await process_telegram_message(ev)

    async def h_edit(ev):
        ev._raw_ingest_edited = True
        await process_telegram_message(ev)

    for old in (_channel_poll_task, _ingress_stage_task):
        if old is not None and not old.done():
            old.cancel()
    # Publish before either the ingress worker or event handlers can accept work.
    _receiver = receiver
    _ingress_stage = stage
    _ingress_stage_task = asyncio.create_task(stage.run(), name="ingress-stage")

    _diag_monitored = {-c - 1000000000000 for c in chats if c < -1000000000000}
    _diag = {"types": {}, "log_left": 10}
    raw_diagnostics = bool(getattr(Config, "TELEGRAM_RAW_DIAGNOSTICS", False))

    _poke_chats = set(chats)

    async def h_raw(update):

        bot_state.last_raw_update_at = time.monotonic()
        bot_state.raw_update_count += 1
        # Telegram báo "kênh có tin mới" (kênh lớn hay gửi kiểu này thay vì đẩy cả tin):
        # quét đúng kênh đó ngay, không chờ nhịp poll kế tiếp.
        if type(update).__name__ == "UpdateChannelTooLong":
            try:
                _poke_cid = -1000000000000 - int(update.channel_id)
                if _poke_cid in _poke_chats:
                    receiver.poke(_poke_cid, reason="ChannelTooLong")
            except Exception:
                pass
        if not raw_diagnostics:
            return
        try:
            name = type(update).__name__
            _diag["types"][name] = _diag["types"].get(name, 0) + 1
            msg = getattr(update, "message", None)
            cid = getattr(getattr(msg, "peer_id", None), "channel_id", None) or getattr(update, "channel_id", None)
            if cid in _diag_monitored and _diag["log_left"] > 0:
                _diag["log_left"] -= 1
                mdate = getattr(msg, "date", None)
                age = (time.time() - mdate.timestamp()) if mdate else -1.0
                logger.info("🔬 [PushDiag] %s channel_id=%s msg_id=%s age=%.1fs",
                            name, cid, getattr(msg, "id", None), age)
            if bot_state.raw_update_count % 500 == 0:
                logger.info("🔬 [PushDiag] loại update (cộng dồn): %s", _diag["types"])
        except Exception:
            pass

    push_unfiltered = bool(getattr(Config, "TELEGRAM_PUSH_UNFILTERED", True))
    new_builder = events.NewMessage() if push_unfiltered else (
        events.NewMessage(chats=chats) if source_filter else events.NewMessage()
    )
    edit_builder = events.MessageEdited() if push_unfiltered else (
        events.MessageEdited(chats=chats) if source_filter else events.MessageEdited()
    )
    client.add_event_handler(h_new, new_builder)
    client.add_event_handler(h_edit, edit_builder)
    client.add_event_handler(h_raw, events.Raw())
    if raw_diagnostics:
        logger.info("🔬 Telegram Raw diagnostics đang bật")

    # Enable delivery as soon as handlers exist, before potentially slow preload.
    if push_health is not None:
        try:
            await push_health.enable()
            logger.info("[Push-Health] Live update stream enabled before browser preload")
        except Exception as exc:
            logger.warning("[Push-Health] Startup enable failed: %s; poll fallback remains active", type(exc).__name__)

    poll_interval = float(getattr(Config, "CHANNEL_POLL_INTERVAL", 30.0) or 0.0)
    poll_enabled = (
        bool(getattr(Config, "TELEGRAM_POLL_FALLBACK_ENABLED", True))
        if bool(getattr(Config, "INGRESS_FIX_V2", False)) else poll_interval > 0
    )
    _channel_poll_task = (
        asyncio.create_task(receiver.run(), name="telegram-receiver")
        if poll_enabled else None
    )

    bot_state.handler_registered = True

    _prewarm_spec = os.getenv("MEDIA_DC_PREWARM", "").strip()
    if _prewarm_spec and hasattr(client, "prewarm_media_dc"):
        try:
            _pw_dc, _pw_lanes = (int(part) for part in _prewarm_spec.split(":", 1))
        except ValueError:
            logger.warning("MEDIA_DC_PREWARM không hợp lệ (cần dạng 1:6): %r", _prewarm_spec)
        else:
            client._media_prewarm_task = asyncio.create_task(
                client.prewarm_media_dc(_pw_dc, _pw_lanes), name="media-dc-prewarm")

    logger.info(
        "✅ Handler ready | %s channels | OCR-only=%s | push filter=%s | "
        "poll=%s",
        len(chats),
        sorted(getattr(Config, "OCR_ALLOWED_CHANNEL_IDS", set())),
        (source_filter and not push_unfiltered),
        f"push-first, poll fallback mỗi {poll_interval:.1f}s, "
        f"batch_size<={min(10, int(getattr(Config, 'TELEGRAM_DIALOG_BATCH_SIZE', 8)))}, "
        "max_concurrent=5"
        if poll_enabled else "push-only",
    )


async def _cleanup_scheduler():
    last_disk_check = 0.0
    last_inbox_sweep = 0.0
    last_ocr_tmp_cleanup = time.monotonic()
    inbox_sweep_interval = max(
        30.0,
        float(getattr(Config, "INBOX_PENDING_SWEEP_INTERVAL_SECONDS", 300.0)),
    )
    inbox_max_pending_age = max(
        30.0,
        float(getattr(Config, "INBOX_MAX_PENDING_AGE_SECONDS", 900.0)),
    )
    while bot_state.is_running:
        try:
            await asyncio.sleep(float(getattr(Config, "INPUT_CACHE_CLEANUP_INTERVAL", 30)))
            _prune_site_code_seen()
            _prune_processed_message_hashes()
            now = time.monotonic()
            if now - last_disk_check >= 300.0:
                await asyncio.get_running_loop().run_in_executor(
                    _MEDIA_EXECUTOR, ensure_disk_space, Path(__file__).resolve().parent)
                last_disk_check = now
            if now - last_ocr_tmp_cleanup >= 3600.0:
                await asyncio.get_running_loop().run_in_executor(
                    _MEDIA_EXECUTOR,
                    _cleanup_stale_ocr_tmp,
                    float(getattr(Config, "OCR_TEMP_DIR_MAX_AGE_SECONDS", 3 * 86400)),
                )
                last_ocr_tmp_cleanup = now
            if _durable_inbox is not None and now - last_inbox_sweep >= inbox_sweep_interval:
                cutoff = datetime.now(timezone.utc) - timedelta(seconds=inbox_max_pending_age)
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(_INBOX_EXECUTOR, _durable_inbox.reclaim_stale)
                purged = await loop.run_in_executor(
                    _INBOX_EXECUTOR,
                    _durable_inbox.ignore_pending_before,
                    cutoff,
                    "stale_pending_purge",
                )
                last_inbox_sweep = now
                if purged:
                    logger.warning(
                        "🧹 [Inbox-Sweep] Đã bỏ %s dòng 'pending' kẹt quá %.0f phút "
                        "(giftcode chắc chắn đã hết hạn)",
                        purged,
                        inbox_max_pending_age / 60.0,
                    )
        except asyncio.CancelledError:
            break
        except Exception:
            pass


async def _db_maintenance_loop():
    while bot_state.is_running:
        try:
            now = datetime.now()
            next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            await asyncio.sleep(max(1.0, (next_run - now).total_seconds()))
        except asyncio.CancelledError:
            break

        loop = asyncio.get_running_loop()
        base_dir = Path(__file__).resolve().parent
        try:
            await loop.run_in_executor(_MEDIA_EXECUTOR, cleanup_daily, base_dir)
            if datetime.now().weekday() == 0:
                await loop.run_in_executor(_DB_EXECUTOR, cleanup_weekly, base_dir)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Disk maintenance failed")


async def daily_reset_watchdog():
    while bot_state.is_running:
        try:
            await asyncio.sleep(60)
            _refresh_daily_state()
        except asyncio.CancelledError:
            break
        except Exception:
            pass


_INSTANCE_LOCK_FH = None
_INSTANCE_LOCK_OFFSET = 1024  # khoá vùng byte này để vẫn đọc được PID ở offset 0


def _instance_lock_path() -> Path:
    session = os.path.basename(str(getattr(Config, "SESSION_NAME", "") or "")) or "session_bot"
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", session)
    return Path(__file__).resolve().parent / "logs" / f".bot_instance_{safe}.lock"


def acquire_single_instance_lock() -> bool:
    """Chặn chạy 2 bản bot cùng một Telethon session.

    Hai tiến trình dùng chung file .session sẽ đụng khoá SQLite
    ("database is locked") và còn có thể làm lệch auth key. Khoá do HĐH giữ
    nên tự nhả khi tiến trình thoát/crash — không bao giờ bị "kẹt" như PID file.
    """
    global _INSTANCE_LOCK_FH
    if _INSTANCE_LOCK_FH is not None:
        return True
    path = _instance_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(path, "a+b")
    try:
        fh.seek(_INSTANCE_LOCK_OFFSET)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        holder = ""
        try:
            fh.seek(0)
            holder = fh.read(32).decode("ascii", "ignore").strip()
        except OSError:
            pass
        fh.close()
        logger.critical(
            "❌ Đã có một bản bot khác đang chạy cùng session '%s'%s. "
            "Hãy tắt bản đó (Ctrl+C hoặc `taskkill /PID <pid> /F`) rồi chạy lại. "
            "Bản này thoát để không làm hỏng session Telegram.",
            os.path.basename(str(getattr(Config, "SESSION_NAME", ""))) or "session_bot",
            f" (PID {holder})" if holder.isdigit() else "",
        )
        return False
    try:
        fh.truncate(0)
        fh.write(str(os.getpid()).encode("ascii"))
        fh.flush()
    except OSError:
        pass
    _INSTANCE_LOCK_FH = fh
    return True


def _is_sqlite_locked(exc: BaseException) -> bool:
    """True nếu là lỗi khoá tạm thời của SQLite (file session Telethon bị giữ)."""
    return isinstance(exc, sqlite3.OperationalError) and any(
        word in str(exc).lower() for word in ("locked", "busy")
    )


async def main():
    global client, _systems, BOT_START_TIME

    if not Config.API_ID or not Config.API_HASH.strip():
        logger.critical(
            "Thiếu API_ID hoặc API_HASH. Hãy điền hai biến này trong file .env "
            "trước khi chạy bot."
        )
        raise SystemExit(2)

    # Phải chạy trước mọi thứ khác (DB, Edge, Telethon) để bản trùng thoát sạch,
    # không chạm vào tài nguyên đang dùng chung của bản đang chạy.
    if not acquire_single_instance_lock():
        raise SystemExit(3)

    try:
        from reliable_telegram_client import ReliableTelegramClient
        client = ReliableTelegramClient(
            _build_telegram_session(),
            Config.API_ID,
            Config.API_HASH,
            device_model="Desktop Bot",
            system_version="Windows 10",
            app_version="1.0",
            connection=ConnectionTcpAbridged,
            connection_retries=getattr(Config, "TELEGRAM_CONNECTION_RETRIES", 0),
            retry_delay=1,
            auto_reconnect=getattr(Config, "TELEGRAM_AUTO_RECONNECT", False),
            timeout=getattr(Config, "TELEGRAM_CONNECT_TIMEOUT", 15.0),
            use_ipv6=False,
            flood_sleep_threshold=60,
            receive_updates=True,
            sequential_updates=False,
        )

        logger.info("[SYSTEM] INGRESS_FIX_V2=%s", Config.INGRESS_FIX_V2)
        logger.info("🚀 BOT v%s — HTTP/browser theo từng site", BOT_VERSION)
        logger.info("[Build] 2026-10-21-spoiler-dialog-batch-v11.0.2-poll-ocr-v4-2026-10-25-xx88-tabs-v5-2026-10-25 PATCH_MARKER_2026-10-25-v5-xx88-tabs-queue | PATCH_MARKER_2026-10-26-v9-core-ocr-sqlite | sites=%s | browser=%s",
                    sorted(Config.DOMAIN_TO_CHANNEL_URL),
                    "dual-edge" if Config.CHROME2_ENABLED else "single-edge")
        logger.info("[LLwin-Captcha] mode=%s",
                    "manual" if Config.LLWIN_MANUAL_SLIDER_MODE else "playwright-direct")
        logger.info("[OCR-STREAM] configured=%s domain=getcodexx8bonus.pages.dev policy=%s",
                    getattr(Config, "OCR_STREAMING_ENABLED", True), OCR_POLICY_VERSION)
        logger.info("[Tab-Slots] tab cố định/domain (stateless=%s, accounts=%s): %s",
                    _stateless_entry_enabled(), getattr(Config, "STATELESS_ENTRY_ACCOUNTS", "all"),
                    ", ".join(f"{_SITE_LOG_LABELS.get(d, d)}={_domain_slot_capacity(d)}"
                              for d in sorted(Config.DOMAIN_TO_CHANNEL_URL)))

        BOT_START_TIME = datetime.now(timezone.utc)

        logger.info(
            f"⏰ START: "
            f"{datetime.now().strftime('%H:%M:%S %d/%m/%Y')}"
        )
        logger.info(
            f"🆕 New-message-only mode | cutoff={BOT_START_TIME.isoformat()} | "
            f"catch_up={getattr(Config, 'TELEGRAM_CATCH_UP', False)} | "
            f"inbox_recovery={getattr(Config, 'TELEGRAM_INBOX_RECOVERY_MODE', 'at_least_once')}"
        )
        if _RESTARTED_BY_LAUNCHER:
            logger.warning(
                "🔁 [Supervisor] Bot vừa được run.bat tự chạy lại — nhận lại tin trong 5 phút "
                "trước lúc start để không mất tin lúc bot chết"
            )

        _systems = await init_systems()
        logger.warning("⭐ Init channels/accounts (HTTP/browser)...")
        await init_channels_and_accounts()
        browser_start_domains = {
            t["domain"] for t in build_unique_account_targets()
            if get_site_profile(t["domain"]) and not uses_direct_http(t["domain"])
        }
        await ensure_chromes_ready(browser_start_domains)
        await asyncio.sleep(0.5)

        await asyncio.get_running_loop().run_in_executor(
            _MEDIA_EXECUTOR,
            _cleanup_stale_ocr_tmp,
            float(getattr(Config, "OCR_TEMP_DIR_MAX_AGE_SECONDS", 3 * 86400)),
        )

        if getattr(Config, "MAX_CONCURRENT_OCR", 0) > 0:
            try:
                ocr_ready = await asyncio.get_running_loop().run_in_executor(
                    _OCR_EXECUTOR,
                    warmup_image_extractor,
                )
                if not ocr_ready:
                    raise RuntimeError("OCR engine/inference unavailable; see native runtime error above")
                logger.info("✅ OCR warm-up model + ONNX inference hoàn tất")
            except Exception as exc:
                logger.warning("⚠️ OCR warm-up thất bại, sẽ thử lại khi có ảnh: %s", exc)

        logger.warning("🔥 client.start()...")
        for _attempt in range(1, 4):
            try:
                await client.start()
                break
            except asyncio.IncompleteReadError as e:
                if _attempt == 3:
                    raise
                logger.warning(
                    "⚠️ client.start() socket đọc dở (%s) — reset kết nối và thử lại %s/3",
                    e,
                    _attempt,
                )
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(3 * _attempt)
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower():
                    raise
                if _attempt == 3:
                    raise RuntimeError(
                        "File session Telegram đang bị tiến trình khác giữ khoá "
                        f"('{Config.SESSION_NAME}.session'). Kiểm tra còn bản bot/python "
                        "nào đang chạy không (tasklist | findstr python), tắt hết rồi chạy lại."
                    ) from e
                logger.warning(
                    "⚠️ client.start() session SQLite đang bị khoá (%s) — thử lại %s/3",
                    e,
                    _attempt,
                )
                try:
                    await client.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(3 * _attempt)
            except (asyncio.TimeoutError, ConnectionError, OSError, EOFError) as e:
                if _attempt == 3:
                    raise
                logger.warning("⚠️ client.start() lỗi (%s) — thử lại %s/3", e, _attempt)
                await asyncio.sleep(3 * _attempt)
        logger.warning("🔥 OK")

        get_shutdown_handler().set_stop_callback(client.disconnect)

        dialogs_synced = await sync_telegram_dialogs()
        if not dialogs_synced:
            logger.warning(
                "⚠️ Telegram dialog sync không hoàn tất; tiếp tục bằng "
                "verify_channels_and_get_ids() để kiểm tra quyền truy cập thực tế"
            )

        if not await verify_telegram_session():
            raise RuntimeError("Telegram session verification failed")

        logger.info("Console log mode; timing and /status counters remain enabled")

        qm = init_queue_manager(
            soft_pct=float(
                getattr(
                    Config,
                    "QUEUE_SOFT_LIMIT_PCT",
                    0.75,
                )
            ),
            hard_pct=float(
                getattr(
                    Config,
                    "QUEUE_HARD_LIMIT_PCT",
                    0.95,
                )
            ),
            check_interval=float(
                getattr(
                    Config,
                    "QUEUE_CHECK_INTERVAL",
                    5.0,
                )
            ),
            cleanup_on_soft_limit=bool(
                getattr(
                    Config,
                    "QUEUE_CLEANUP_ON_SOFT_LIMIT",
                    False,
                )
            ),
            cleanup_target_pct=float(
                getattr(
                    Config,
                    "QUEUE_CLEANUP_TARGET_PCT",
                    0.50,
                )
            ),
        )

        await qm.start()

        logger.info(
            f"🧹 Queue manager ready "
            f"(soft={qm.soft_pct * 100:.0f}%, "
            f"hard={qm.hard_pct * 100:.0f}%, "
            f"interval={qm.check_interval:.0f}s)"
        )

        verified_channels = await verify_channels_and_get_ids()
        if len(verified_channels) != len(Config.CHANNEL_CONFIG):
            missing_channels = sorted(
                set(Config.CHANNEL_CONFIG) - set(verified_channels)
            )
            missing_text = ", ".join(str(chat_id) for chat_id in missing_channels)
            allow_partial = bool(getattr(Config, "ALLOW_PARTIAL_CHANNELS", True))
            if not allow_partial or not verified_channels:
                raise RuntimeError(
                    "Configured Telegram channels are inaccessible: " + missing_text
                )

            for _cid in missing_channels:
                Config.CHANNEL_CONFIG.pop(_cid, None)
            logger.warning(
                "⚠️ [Telegram] Bỏ qua %s kênh không truy cập được (%s) — "
                "tiếp tục với %s kênh hợp lệ",
                len(missing_channels),
                missing_text,
                len(verified_channels),
            )

        await setup_telegram_handler()

        browser_targets = [
            t for t in build_unique_account_targets()
            if get_site_profile(t["domain"]) and not uses_direct_http(t["domain"])
        ]
        if _BROWSER_ENGINE_OK and browser_targets:
            try:
                await browser_engine.preload_browsers_and_accounts(browser_targets)
            except Exception as e:
                logger.error(
                    f"❌ [Browser] preload lỗi ({e}) — submit sẽ được đánh dấu lỗi hạ tầng"
                )
            # Startup loaded the success markers before TabPool existed.
            # Seed the new pool too, so its tab watcher cannot list them again.
            for day, account_domain, username in tuple(bot_state._daily_used):
                if day == _today_str():
                    _notify_browser_exhausted(account_domain, username)

        start_domain_workers()
        start_ocr_media_workers()
        register_default_commands()
        command_registry.register("clear", _clear_runtime_state, timeout=15.0)

        setup_admin_commands(
            client,
            admin_id=Config.TELEGRAM_ADMIN_ID,
            context_provider=lambda: {
                "bot_state": bot_state,
                "systems": _systems,
            },
        )

        # catch_up() luôn được gọi; TELEGRAM_CATCH_UP chỉ đổi cutoff lọc message
        # (BOT_START_TIME-5 phút khi bật, -30 giây khi tắt) trong accept() của setup_telegram_handler.
        if getattr(Config, "TELEGRAM_CATCH_UP", False):
            logger.info("🔄 Startup catch_up: nhận message trong 5 phút trước khi start")
        else:
            logger.info("🔄 Startup catch_up có cutoff — chỉ nhận message sau thời điểm start")
        await client.catch_up()

        await start_message_workers()
        configured_workers = max(1, int(getattr(Config, "MESSAGE_WORKERS", 6)))
        started_workers = sum(not task.done() for task in message_workers)
        logger.info(
            "WORKER_CHECK configured=%s started=%s",
            configured_workers, started_workers,
        )
        if configured_workers != started_workers:
            logger.critical(
                "WORKER_CHECK failed: configured=%s started=%s",
                configured_workers, started_workers,
            )
            raise RuntimeError(
                f"Message worker startup mismatch: configured={configured_workers}, "
                f"started={started_workers}"
            )

        logger.info(
            f"✅ BOT READY! "
            f"{datetime.now().strftime('%H:%M:%S')}"
        )

        async def heartbeat():

            while bot_state.is_running:
                try:
                    await asyncio.sleep(
                        float(
                            getattr(
                                Config,
                                "HEARTBEAT_INTERVAL",
                                300.0,
                            )
                        )
                    )

                    now_mono = time.monotonic()
                    raw_age = (
                        "-" if bot_state.last_raw_update_at is None
                        else f"{now_mono - bot_state.last_raw_update_at:.0f}s"
                    )
                    accepted_age = (
                        "-" if bot_state.last_accepted_message_at is None
                        else f"{now_mono - bot_state.last_accepted_message_at:.0f}s"
                    )
                    peak_lag = bot_state.event_loop_lag_peak_ms
                    bot_state.event_loop_lag_peak_ms = 0.0
                    logger.info(
                        f"💓 {datetime.now().strftime('%H:%M:%S')} "
                        f"| connected={client.is_connected()} "
                        f"| raw_updates={bot_state.raw_update_count} "
                        f"(last={raw_age}) "
                        f"| accepted={bot_state.accepted_message_count} "
                        f"(last={accepted_age}) "
                        f"| loop_lag_peak={peak_lag:.0f}ms "
                        f"| tasks={len(_active_submit_tasks)} "
                        f"| q={message_queue.qsize() if message_queue else 0}"
                    )

                    try:
                        me = await asyncio.wait_for(
                            client.get_me(),
                            timeout=float(
                                getattr(
                                    Config,
                                    "TELEGRAM_HEARTBEAT_TIMEOUT",
                                    8.0,
                                )
                            ),
                        )

                        if not me:
                            logger.warning(
                                "⚠️ [Heartbeat] Telegram session "
                                "không phản hồi"
                            )

                    except asyncio.CancelledError:
                        raise

                    except Exception as e:
                        logger.warning(
                            f"⚠️ [Heartbeat] Kiểm tra kết nối lỗi: {e}"
                        )

                except asyncio.CancelledError:
                    break

                except Exception as e:
                    logger.warning(
                        f"⚠️ [Heartbeat] Lỗi không mong muốn: {e}"
                    )

        async def event_loop_probe():

            expected = time.monotonic() + 0.05
            consecutive_slow = 0
            while bot_state.is_running:
                await asyncio.sleep(0.05)
                now = time.monotonic()
                lag_ms = max(0.0, (now - expected) * 1000.0)
                bot_state.event_loop_lag_ms = lag_ms
                if lag_ms > bot_state.event_loop_lag_peak_ms:
                    bot_state.event_loop_lag_peak_ms = lag_ms
                if lag_ms >= 100.0:
                    consecutive_slow += 1
                    if consecutive_slow % 20 == 1:
                        logger.warning(
                            "⚠️ [EventLoop] lag=%.0fms | raw=%s | ingest_q=%s msg_q=%s ocr_q=%s | domains=%s",
                            lag_ms,
                            bot_state.raw_update_count,
                            _ingress_stage.pending if _ingress_stage else 0,
                            message_queue.qsize() if message_queue else 0,
                            _ocr_media_queue.qsize() if _ocr_media_queue else 0,
                            {d: q.qsize() for d, q in _domain_queues.items() if q.qsize() > 0},
                        )
                else:
                    consecutive_slow = 0
                expected = now + 0.05

        async def raw_update_watchdog():
            connection_since = None
            catch_up_attempted = False
            reconnect_attempted = False
            while bot_state.is_running:
                await asyncio.sleep(5.0)
                if bot_state.raw_update_count > 0:
                    continue
                if not client.is_connected():
                    connection_since = None
                    catch_up_attempted = False
                    reconnect_attempted = False
                    continue
                if connection_since is None:
                    connection_since = time.monotonic()
                    catch_up_attempted = False
                    reconnect_attempted = False

                silent_seconds = time.monotonic() - connection_since
                if silent_seconds >= 60.0 and not catch_up_attempted:
                    catch_up_attempted = True
                    logger.warning(
                        "[Push-Health] No raw updates for %.0fs; calling client.catch_up()",
                        silent_seconds,
                    )
                    try:
                        await asyncio.wait_for(
                            client.catch_up(),
                            timeout=max(5.0, float(getattr(
                                Config, "TELEGRAM_REQUEST_TIMEOUT", 20.0
                            ))),
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "[Push-Health] catch_up failed: %s: %s",
                            type(exc).__name__, exc,
                        )

                if bot_state.raw_update_count > 0:
                    continue
                if silent_seconds >= 120.0 and not reconnect_attempted:
                    reconnect_attempted = True
                    logger.error(
                        "[Push-Health] No raw updates for %.0fs; forcing Telegram reconnect",
                        silent_seconds,
                    )
                    try:
                        await asyncio.wait_for(client.disconnect(), timeout=5.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "[Push-Health] Forced disconnect failed: %s: %s",
                            type(exc).__name__, exc,
                        )
                    connection_since = None

        _bg = {
            asyncio.create_task(
                heartbeat(),
                name="hb",
            ),
            asyncio.create_task(
                event_loop_probe(),
                name="event-loop-probe",
            ),
            asyncio.create_task(
                raw_update_watchdog(),
                name="raw-update-watchdog",
            ),
            asyncio.create_task(
                _cleanup_scheduler(),
                name="cleanup",
            ),
            asyncio.create_task(
                daily_reset_watchdog(),
                name="daily",
            ),
            asyncio.create_task(
                _db_maintenance_loop(),
                name="db",
            ),
        }

        if _BROWSER_ENGINE_OK and browser_targets and getattr(Config, "USE_BROWSER_FOR_MULTI_SITE", True):
            _bg.add(asyncio.create_task(browser_engine.browser_watchdog(), name="browser-watchdog"))

        reconnect_attempts = 0
        max_reconnect_attempts = max(
            0,
            int(getattr(Config, "TELEGRAM_MAX_RECONNECT_ATTEMPTS", 5)),
        )
        reconnect_base_delay = min(300.0, max(
            30.0,
            float(getattr(Config, "TELEGRAM_RECONNECT_BASE_DELAY", 2.0)),
        ))
        reconnect_max_delay = max(
            reconnect_base_delay,
            min(300.0, float(getattr(Config, "TELEGRAM_RECONNECT_MAX_DELAY", 300.0))),
        )
        stable_connection_seconds = max(
            0.0,
            float(getattr(Config, "TELEGRAM_STABLE_CONNECTION_SECONDS", 30.0)),
        )
        connect_timeout = max(
            1.0,
            float(getattr(Config, "TELEGRAM_CONNECT_TIMEOUT", 15.0)),
        )
        connected_since = time.monotonic() if client.is_connected() else None

        def reconnect_delay(attempt: int, *, immediate_first=False) -> float:
            return watchdog_backoff_seconds(
                attempt,
                base_seconds=reconnect_base_delay,
                max_seconds=reconnect_max_delay,
                immediate_first=immediate_first,
            )

        while bot_state.is_running:
            try:
                if not client.is_connected():
                    reconnect_attempts += 1
                    if reconnect_attempts > max_reconnect_attempts:
                        logger.critical(
                            "🛑 [Telegram] Hết giới hạn reconnect (%s lần)",
                            max_reconnect_attempts,
                        )
                        _mark_unintended_stop("hết giới hạn reconnect")
                        bot_state.is_running = False
                        return

                    # A clean disconnect (including push-health recovery) can
                    # reconnect immediately once. Failed connections below
                    # retain their ordinary bounded exponential backoff.
                    delay = reconnect_delay(reconnect_attempts, immediate_first=True)
                    logger.warning(
                        "🔄 [Telegram] Mất kết nối — thử lần %s/%s sau %.1fs "
                        "(watchdog_attempt=%s)",
                        reconnect_attempts,
                        max_reconnect_attempts,
                        delay,
                        reconnect_attempts,
                    )
                    await asyncio.sleep(delay)
                    await asyncio.wait_for(
                        client.connect(),
                        timeout=connect_timeout,
                    )
                    connected_since = time.monotonic()

                await client.run_until_disconnected()

                if not bot_state.is_running:
                    break

                connection_age = (
                    time.monotonic() - connected_since
                    if connected_since is not None
                    else 0.0
                )
                if connection_age >= stable_connection_seconds:
                    reconnect_attempts = 0
                connected_since = None

            except asyncio.CancelledError:
                raise

            except AuthKeyDuplicatedError as e:
                logger.critical(
                    "🚨 [Telegram] AuthKeyDuplicated — dừng retry ngay: %s",
                    e,
                )
                bot_state.is_running = False
                try:
                    await client.disconnect()
                except Exception:
                    pass
                backup_corrupt_telegram_session()
                await send_auth_key_alert(e)
                raise SystemExit(78)

            except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                reconnect_attempts += 1
                if reconnect_attempts > max_reconnect_attempts:
                    logger.critical(
                        "🛑 [Telegram] Hết giới hạn reconnect (%s lần): %s",
                        max_reconnect_attempts,
                        e,
                    )
                    _mark_unintended_stop("hết giới hạn reconnect")
                    bot_state.is_running = False
                    return

                delay = reconnect_delay(reconnect_attempts)
                logger.warning(
                    "⚠️ [Telegram] Lỗi kết nối: %s — thử lại sau %.1fs "
                    "(lần %s/%s)",
                    e,
                    delay,
                    reconnect_attempts,
                    max_reconnect_attempts,
                )
                await asyncio.sleep(delay)

            except Exception as e:
                if _is_sqlite_locked(e):
                    # File session bị giữ khoá tạm thời (tiến trình khác / AV / sync).
                    # Trước đây rơi vào nhánh "dừng bot" => bot tự tắt và đóng luôn Edge.
                    reconnect_attempts += 1
                    if reconnect_attempts > max_reconnect_attempts:
                        logger.critical(
                            "🛑 [Telegram] Session SQLite vẫn bị khoá sau %s lần: %s",
                            max_reconnect_attempts,
                            e,
                        )
                        _mark_unintended_stop("session SQLite bị khoá")
                        bot_state.is_running = False
                        return
                    delay = reconnect_delay(reconnect_attempts)
                    logger.warning(
                        "⚠️ [Telegram] Session SQLite bị khoá: %s — giữ bot chạy, "
                        "thử lại sau %.1fs (lần %s/%s)",
                        e,
                        delay,
                        reconnect_attempts,
                        max_reconnect_attempts,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.exception(
                    "❌ [Telegram] Lỗi không mong muốn — dừng bot, không retry: %s",
                    e,
                )
                _mark_unintended_stop("lỗi Telegram không mong muốn")
                bot_state.is_running = False
                return

    except AuthKeyDuplicatedError as e:
        logger.critical(
            "🚨 [Telegram] AuthKeyDuplicated — dừng bot, không retry: %s",
            e,
        )
        bot_state.is_running = False
        try:
            await client.disconnect()
        except Exception:
            pass
        backup_corrupt_telegram_session()
        await send_auth_key_alert(e)

        raise SystemExit(78)

    except Exception as e:
        logger.critical(f"❌ Critical: {e}\n{traceback.format_exc()}")
        _mark_unintended_stop("lỗi nghiêm trọng")
        raise SystemExit(EXIT_UNINTENDED_STOP if _UNINTENDED_STOP else 1)
    finally:
        logger.info("\n🛑 Shutting down...")
        bot_state.is_running = False

        global _channel_poll_task, _ingress_stage_task
        if _channel_poll_task is not None:
            _channel_poll_task.cancel()
            await asyncio.gather(_channel_poll_task, return_exceptions=True)
            _channel_poll_task = None
        if _ingress_stage is not None:
            try:
                await _ingress_stage.drain(timeout=5.0)
            except Exception:
                pass
        if _ingress_stage_task is not None:
            _ingress_stage_task.cancel()
            await asyncio.gather(_ingress_stage_task, return_exceptions=True)
            _ingress_stage_task = None

        if message_queue is not None:
            try:
                await asyncio.wait_for(
                    message_queue.join(),
                    timeout=float(getattr(Config, "SHUTDOWN_MESSAGE_DRAIN_TIMEOUT", 8.0)),
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "⚠️ Shutdown: message queue chưa drain hết (q=%s); "
                    "các row còn lại sẽ được giữ trong DurableInbox",
                    message_queue.qsize(),
                )
            except Exception as exc:
                logger.warning("⚠️ Shutdown: lỗi drain message queue: %s", exc)

        if _ocr_media_queue is not None:
            try:
                await asyncio.wait_for(
                    _ocr_media_queue.join(),
                    timeout=float(getattr(Config, "SHUTDOWN_MESSAGE_DRAIN_TIMEOUT", 8.0)),
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "⚠️ Shutdown: OCR/media queue chưa drain hết (q=%s)",
                    _ocr_media_queue.qsize(),
                )
            except Exception as exc:
                logger.warning("⚠️ Shutdown: lỗi drain OCR/media queue: %s", exc)

        if _active_submit_tasks:
            active_submit_drain_timeout = min(
                120.0,
                max(
                    8.0,
                    float(getattr(Config, "SUBMIT_TIMEOUT_SECONDS", 45.0))
                    * max(1, int(getattr(Config, "MAX_RETRIES_PER_ACCOUNT", 1)))
                    + 5.0,
                ),
            )
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(_active_submit_tasks), return_exceptions=True),
                    timeout=active_submit_drain_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "⚠️ Shutdown: submit/domain item chưa drain sau %.1fs; sẽ hủy task còn lại",
                    active_submit_drain_timeout,
                )
                for t in list(_active_submit_tasks):
                    t.cancel()

        for w in message_workers:
            w.cancel()
        if message_workers:
            await asyncio.gather(*message_workers, return_exceptions=True)
        if _async_pipeline_stats_task is not None:
            _async_pipeline_stats_task.cancel()
            await asyncio.gather(_async_pipeline_stats_task, return_exceptions=True)
        for w in _ocr_media_workers:
            w.cancel()
        if _ocr_media_workers:
            await asyncio.gather(*_ocr_media_workers, return_exceptions=True)
        global _inbox_drain_task
        if _inbox_drain_task is not None:
            _inbox_drain_task.cancel()
            await asyncio.gather(_inbox_drain_task, return_exceptions=True)
            _inbox_drain_task = None
        domain_worker_tasks = [t for ws in _domain_workers.values() for t in ws]
        for t in domain_worker_tasks:
            t.cancel()
        if domain_worker_tasks:
            await asyncio.gather(*domain_worker_tasks, return_exceptions=True)
        try:
            qm = get_queue_manager()
            if qm is not None:
                await qm.stop()
        except Exception:
            pass

        background_tasks = set(locals().get("_bg", set())) | set(bot_state.bg_tasks)
        background_tasks.discard(asyncio.current_task())
        pending_background = {task for task in background_tasks if not task.done()}
        if pending_background:
            _, pending_background = await asyncio.wait(
                pending_background, timeout=5.0,
            )
            if pending_background:
                logger.warning(
                    "Cancelling %s background tasks that did not stop during shutdown",
                    len(pending_background),
                )
                for task in pending_background:
                    task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)

        if _history_queue is not None and _history_writer_task is not None:
            history_timeout = max(
                5.0,
                float(getattr(Config, "HISTORY_DRAIN_TIMEOUT_SECONDS", 15.0)),
            )
            try:
                await asyncio.wait_for(
                    _history_queue.put(None),
                    timeout=history_timeout,
                )
                await asyncio.wait_for(_history_queue.join(), timeout=history_timeout)
                await asyncio.wait_for(_history_writer_task, timeout=history_timeout)
            except asyncio.TimeoutError:
                logger.error(
                    "History writer did not drain before shutdown (queued=%s)",
                    _history_queue.qsize(),
                )
                _history_writer_task.cancel()
                await asyncio.gather(_history_writer_task, return_exceptions=True)
            except Exception:
                logger.exception("History writer failed during shutdown")

        try:
            stop_monitoring()
        except Exception:
            logger.exception("Health monitor shutdown failed")

        try:
            shutdown_ocr_executor(wait=True, cancel_futures=True)
        except Exception as e:
            logger.debug(f"⚠️ [OCR] shutdown executor lỗi (bỏ qua): {e}")

        if _http_submitter is not None:
            try:
                await _http_submitter.aclose()
            except Exception as e:
                logger.debug("⚠️ [HTTP] cleanup lỗi (%s)", type(e).__name__)

        if _BROWSER_ENGINE_OK:
            browser_engine.shutdown()
            try:
                await browser_engine.cleanup_browsers()
            except Exception as e:
                logger.debug(f"⚠️ [Browser] cleanup lỗi (bỏ qua): {e}")

        await shutdown_chromes()

        try:
            await asyncio.get_running_loop().run_in_executor(
                _MEDIA_EXECUTOR, build_daily_summary,
            )
        except Exception:
            logger.exception("Daily history summary failed during shutdown")

        # Finish outstanding database calls before closing either inbox connection.
        for executor_name, executor in (
            ("media", _MEDIA_EXECUTOR),
            ("db", _DB_EXECUTOR),
            ("inbox-ingress", _INBOX_INGRESS_EXECUTOR),
            ("inbox-state", _INBOX_STATE_EXECUTOR),
        ):
            try:
                executor.shutdown(wait=True, cancel_futures=True)
            except Exception as exc:
                logger.debug("⚠️ [%s executor] cleanup lỗi (bỏ qua): %s", executor_name, exc)

        global _durable_inbox
        if _durable_inbox is not None:
            try:
                _durable_inbox.close()
            except Exception:
                pass
            _durable_inbox = None

        if _systems and _systems.get("db"):
            try:
                _systems["db"].close()
            except Exception as exc:
                logger.debug("⚠️ [DB] cleanup lỗi (bỏ qua): %s", exc)
        get_shutdown_handler().notify_cleanup_done()
        logger.info("✅ Done")
        shutdown_logging()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped")
    if _UNINTENDED_STOP:
        sys.exit(EXIT_UNINTENDED_STOP)