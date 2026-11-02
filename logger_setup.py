

import logging
import logging.handlers
import os
import queue
import re
import sys
import gzip
import shutil
import unicodedata
import threading
import time
from collections import OrderedDict
from contextvars import ContextVar
from pathlib import Path

from config import get_bool_env as _config_get_bool_env
from config import get_int_env as _config_get_int_env


_log_context: ContextVar[str] = ContextVar("log_context", default="SYSTEM")
_warning_log_cache = OrderedDict()
_warning_log_lock = threading.Lock()
# PATCH_MARKER_2026-10-26-v9-core-ocr-sqlite: keep log I/O off producer threads.
_LOG_LISTENER = None
_LOG_QUEUE_HANDLER = None
_logging_lifecycle_lock = threading.RLock()


class _DroppingQueueHandler(logging.handlers.QueueHandler):
    def __init__(self, log_queue):
        super().__init__(log_queue)
        self.dropped_records = 0
        self._accepting = True

    def emit(self, record):
        if self._accepting:
            super().emit(record)

    def enqueue(self, record):
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            # Handler.handle holds the handler lock. Never write stderr or wait
            # for the listener on an event-loop/Telethon producer thread.
            self.dropped_records += 1


class _DrainingQueueListener(logging.handlers.QueueListener):
    def enqueue_sentinel(self):
        # Shutdown runs after producers stop; let the listener make room rather
        # than losing the sentinel when the bounded queue is full.
        self.queue.put(self._sentinel)


def warning_once(key, message, *args, interval=60.0):
    """Emit once per key/window at the source, consistently across all handlers."""
    now = time.monotonic()
    with _warning_log_lock:
        previous = _warning_log_cache.get(key)
        if previous is not None and now - previous < interval:
            return False
        _warning_log_cache[key] = now
        _warning_log_cache.move_to_end(key)
        while len(_warning_log_cache) > 8192:
            _warning_log_cache.popitem(last=False)
    logger.warning(message, *args)
    return True


class TelethonErrorOnlyFilter(logging.Filter):
    def filter(self, record):
        telethon_record = record.name == "telethon" or record.name.startswith("telethon.")
        legacy_forward = "[telethon]" in record.getMessage().lower()
        return record.levelno >= logging.ERROR or not (telethon_record or legacy_forward)


class TelethonErrorForwarder(logging.Handler):
    def __init__(self, target):
        super().__init__(logging.ERROR)
        self.target = target

    def emit(self, record):
        self.target.handle(record)


def configure_telethon_logging(target):
    """Gate the entire subtree at its parent handler, including explicit child levels.

    Logger filters/levels on ancestors are not reapplied to propagated records;
    the parent's ERROR handler is the gate for future sub-loggers as well.
    """
    parent = logging.getLogger("telethon")
    parent.setLevel(logging.ERROR)
    parent.propagate = False
    policy = TelethonErrorOnlyFilter()
    for name, child in list(logging.Logger.manager.loggerDict.items()):
        if not isinstance(child, logging.Logger) or not (name == "telethon" or name.startswith("telethon.")):
            continue
        child.setLevel(logging.ERROR)
        if child is not parent:
            child.propagate = True
        for handler in child.handlers[:]:
            if type(handler).__name__ in {"_TelethonFloodForwarder", "TelethonErrorForwarder"}:
                child.removeHandler(handler)
                handler.close()
            else:
                handler.addFilter(policy)
    parent.addHandler(TelethonErrorForwarder(target))
    for handler in [*target.handlers, *logging.getLogger().handlers]:
        if not any(isinstance(f, TelethonErrorOnlyFilter) for f in handler.filters):
            handler.addFilter(policy)


def set_log_context(tag: str):

    return _log_context.set(tag or "SYSTEM")


def reset_log_context(token) -> None:
    try:
        _log_context.reset(token)
    except Exception:
        pass


class ContextTagFilter(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        record.context_tag = _log_context.get()
        return True


def _slugify_for_filename(text: str) -> str:

    text = (text or "").replace("Đ", "D").replace("đ", "d")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^a-zA-Z0-9_.-]+", "_", text).strip("_")
    return text or "misc"


class PerChannelFileHandler(logging.Handler):

    def __init__(self, base_dir: str = "logs/channels", encoding: str = "utf-8", max_handlers: int = 64):
        super().__init__()
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.encoding = encoding
        self._handlers: OrderedDict[str, DailySizeRotatingFileHandler] = OrderedDict()
        self._max_handlers = max(1, int(max_handlers))
        self._formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%d/%m/%Y %H:%M:%S"
        )

    def _get_file_handler(self, tag: str) -> "DailySizeRotatingFileHandler":
        safe_tag = _slugify_for_filename(tag)
        handler = self._handlers.get(safe_tag)
        if handler is not None:
            self._handlers.move_to_end(safe_tag)
            return handler
        handler = DailySizeRotatingFileHandler(
            self.base_dir / f"{safe_tag}.log", encoding=self.encoding,
            maxBytes=LOG_ROTATION_MAX_BYTES, backupCount=LOG_ROTATION_BACKUP_COUNT,
            utc=LOG_USE_UTC,
        )
        handler.setFormatter(self._formatter)
        self._handlers[safe_tag] = handler
        while len(self._handlers) > self._max_handlers:
            _, old_handler = self._handlers.popitem(last=False)
            try:
                old_handler.close()
            except Exception:
                pass
        return handler

    def emit(self, record: logging.LogRecord) -> None:
        tag = getattr(record, "context_tag", "SYSTEM")
        if not tag or tag == "SYSTEM":
            return
        try:
            self._get_file_handler(tag).emit(record)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        for handler in self._handlers.values():
            try:
                handler.close()
            except Exception:
                pass
        super().close()


try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def _get_int_env(key: str, default: int) -> int:
    return _config_get_int_env(key, default)

def _get_bool_env(key: str, default: bool) -> bool:
    return _config_get_bool_env(key, default)


DEBUG_VERBOSE_MODE = os.getenv("DEBUG_VERBOSE_MODE", "false").lower() == "true"
TEST_LOGGING = os.getenv("AUTOBOT_TESTING") == "1" or "pytest" in sys.modules
LOG_FILE = (str(Path(__file__).resolve().parent / "logs" / "test_pytest.log")
            if TEST_LOGGING else os.getenv("LOG_FILE", "logs/bot_activity.log"))
LOG_DIR = str(Path(LOG_FILE).parent) if TEST_LOGGING else os.getenv("LOG_DIR", "logs")
LOG_ROTATION_BACKUP_COUNT = max(1, _get_int_env("LOG_ROTATION_BACKUP_COUNT", _get_int_env("LOG_BACKUP_DAYS", 3)))
LOG_USE_UTC = _get_bool_env("LOG_USE_UTC", False)
CONSOLE_LOG_LEVEL = os.getenv("CONSOLE_LOG_LEVEL", "INFO").upper()
COMPACT_CONSOLE_LOG = _get_bool_env("COMPACT_CONSOLE_LOG", True)
FILE_LOG_LEVEL = os.getenv("FILE_LOG_LEVEL", "INFO").upper()
LOG_ROTATION_MAX_BYTES = max(1024, _get_int_env("LOG_ROTATION_MAX_BYTES", 5 * 1024 * 1024))
LOG_ROTATION_BACKUP_COUNT_SIZE = LOG_ROTATION_BACKUP_COUNT
CONSOLE_COLOR = _get_bool_env("CONSOLE_COLOR", True)
CONSOLE_SUMMARY_ONLY = _get_bool_env("CONSOLE_SUMMARY_ONLY", True)
# Mỗi dòng trên cmd tối đa bấy nhiêu ký tự (phần dài hơn được cắt, file log vẫn đủ).
CONSOLE_MAX_LINE = max(60, _get_int_env("CONSOLE_MAX_LINE", 150))
# Cùng một loại cảnh báo chỉ hiện trên cmd 1 lần / số giây này (file log vẫn ghi đủ).
CONSOLE_WARN_THROTTLE_SECONDS = max(0, _get_int_env("CONSOLE_WARN_THROTTLE_SECONDS", 30))


PER_CHANNEL_LOGS_ENABLED = not TEST_LOGGING and _get_bool_env("PER_CHANNEL_LOGS_ENABLED", False)


def _enable_windows_ansi() -> None:

    if os.name != "nt":
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        STD_OUTPUT_HANDLE = -11
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        handle = kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
    except Exception:
        pass


def _disable_windows_quick_edit() -> None:
    if os.name != "nt":
        return
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetStdHandle.argtypes = [wintypes.DWORD]
        kernel32.GetStdHandle.restype = wintypes.HANDLE
        kernel32.GetConsoleMode.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel32.GetConsoleMode.restype = wintypes.BOOL
        kernel32.SetConsoleMode.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel32.SetConsoleMode.restype = wintypes.BOOL
        handle = kernel32.GetStdHandle(wintypes.DWORD(-10).value)
        mode = wintypes.DWORD()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, (mode.value & ~0x0040) | 0x0080)
    except Exception:
        pass


CONSOLE_SKIP_PHRASES = [
    "Error processing line",
    "Remainder of file ignored",
    "protobuf",
    "NoneType",
    "loader",
    "MESSAGE RAW",
    "text_len=",
    "Stealth JS loaded",
    "add_init_script",
    "Cannot setup route",
    "TabPool:",
    "tab riêng (persistent)",
    "Chat ",
    " not in config",
    "Cleanup done",
    "Cleanup error",
    "browser_minimize error",
    "history_writer",
    "Cannot write code history",
    "Cannot enqueue",
    "History queue",
    "⚠️ auto_solve error",
    "⚠️ Error finding input",
    "popup không mong đợi",
    "Đóng 0 popup",
    "TASK CANCELLED",
    "⏭️ Chat",
    "nspkg",

    "[HANDLER]",
    "[HANDLER-EDIT]",
    "🔔",
    "Browser-Watchdog] Tất cả tab OK",
    "Telegram session OK",
    "Đã kết nối — dùng context hiện có",
    "🗂️ Tab-",
    "Worker #",
    "Edge restored",
    "Scrolled to input fields",
    "Clicked domain-specific button",

    "✅ [Telegram channel]",
    "[Telegram accepted]",
    "[Domain-Worker#",
    "[Startup]",
    "[TabPool-Memory]",
    "[Heartbeat]",
    "[EXTRACT]",
    "[SUBMIT_STAGE]",
    "[SPOILER]",
    "[PLAIN-TEXT]",
    "[EXTRACT-MERGED]",
    "[MARKER]",
    "[Browser] SUBMIT",
    "────────────────",
]

class CleanConsoleFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        for phrase in CONSOLE_SKIP_PHRASES:
            if phrase in msg:
                return False
        return True


class ConsoleSummaryFilter(logging.Filter):

    INFO_SIGNALS = (
        "🤖 BOT RUNNING",
        "💚 [",
        "🗓️ Ngày mới",
        "✅ Đã nhận code",
        "📥 Đã xếp hàng",
        "SUCCESS",
        "FAILED",
        "NO_RESULT",
        "PENDING_VERIFICATION",
        "SITE_UI_CHANGED",
        "INFRA_FAILURE",
        "ACCOUNT_BLOCKED",
        "RATE_LIMIT",
        "Cloudflare=passed",
        "Cloudflare=pending",
        "đã bấm nút Xác thực",
        "Cloudflare đã xác minh từ trước",
        "✅ Session:",
        "[Edge-CDP] ✅",
        "[TabPool] Đã gán",
        "domain workers",
        "[Inbox] recovery_mode=",
        "TẮT BOT AN TOÀN",
        "TAT BOT AN TOAN",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        if not CONSOLE_SUMMARY_ONLY or record.levelno >= logging.WARNING:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return True
        return any(signal in message for signal in self.INFO_SIGNALS)


class ConsoleRepeatFilter(logging.Filter):

    WINDOW_SECONDS = 5.0

    def __init__(self) -> None:
        super().__init__()
        self._last_seen: dict[str, float] = {}
        self._lock = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno != logging.WARNING:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return True
        if not any(marker in message for marker in ("Turnstile", "PENDING_VERIFICATION")):
            return True
        now = time.monotonic()
        with self._lock:
            previous = self._last_seen.get(message, 0.0)
            self._last_seen[message] = now

            if len(self._last_seen) > 256:
                cutoff = now - self.WINDOW_SECONDS
                self._last_seen = {
                    key: value for key, value in self._last_seen.items() if value >= cutoff
                }
        return now - previous >= self.WINDOW_SECONDS

class ConsoleWarnThrottleFilter(logging.Filter):
    """Trên cmd: cùng một mẫu cảnh báo chỉ hiện 1 lần / cửa sổ thời gian.

    Chỉ áp dụng cho WARNING (ERROR/CRITICAL luôn hiện) và chỉ cho handler của cmd;
    file log không bị ảnh hưởng nên vẫn có đủ thông tin để tra cứu.
    """

    _NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")

    def __init__(self, window: float = CONSOLE_WARN_THROTTLE_SECONDS) -> None:
        super().__init__()
        self.window = float(window)
        self._last_seen: dict[tuple, float] = {}
        self._lock = threading.Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        if self.window <= 0 or record.levelno != logging.WARNING:
            return True
        try:
            # Qua QueueHandler thì tham số đã được ghép vào câu; coi các câu chỉ khác
            # nhau ở con số (ID kênh, số giây...) là cùng một loại cảnh báo.
            text = self._NUMBER_RE.sub("#", record.getMessage())[:160]
        except Exception:
            return True
        key = (record.name, text)
        now = time.monotonic()
        with self._lock:
            previous = self._last_seen.get(key)
            if previous is not None and now - previous < self.window:
                return False
            self._last_seen[key] = now
            if len(self._last_seen) > 512:
                cutoff = now - self.window
                self._last_seen = {k: v for k, v in self._last_seen.items() if v >= cutoff}
        return True


class ColoredFormatter(logging.Formatter):

    LEVEL_COLORS = {
        'DEBUG':    '\033[90m',
        'INFO':     '\033[32m',
        'WARNING':  '\033[33m',
        'ERROR':    '\033[31m',
        'CRITICAL': '\033[1;97;41m',
    }
    RESET = '\033[0m'
    ICONS = {
        'DEBUG':    '·',
        'INFO':     '✓',
        'WARNING':  '!',
        'ERROR':    '✗',
        'CRITICAL': '!!',
    }

    IMPORTANT_STYLES = [
        ("SUCCESS", '\033[1;30;42m'),
        ("FAILED", '\033[1;97;41m'),
        ("Too Many Requests", '\033[1;30;43m'),
    ]

    def format(self, record: logging.LogRecord) -> str:
        try:
            msg = record.getMessage()
        except Exception:
            msg = str(record.msg)

        if record.levelname in ("WARNING", "ERROR", "CRITICAL"):
            color = self.LEVEL_COLORS.get(record.levelname, "")
        else:
            color = ""

        for keyword, style in self.IMPORTANT_STYLES:
            if keyword in msg:
                color = style
                break

        icon = self.ICONS.get(record.levelname, ' ')
        rec = logging.makeLogRecord(record.__dict__)
        rec.levelname = icon
        # Dòng quá dài (danh sách ID kênh, dict kỹ thuật...) sẽ bị cmd tự xuống dòng
        # thành "tơ nhện"; cắt gọn trên cmd, file log vẫn giữ nguyên đầy đủ.
        if len(msg) > CONSOLE_MAX_LINE:
            rec.msg = msg[:CONSOLE_MAX_LINE].rstrip() + " …(xem chi tiết trong logs/bot_activity.log)"
            rec.args = ()
        line = super().format(rec)

        if not CONSOLE_COLOR or not color:
            return line
        return f"{color}{line}{self.RESET}"

def _ensure_log_dir(path: str):
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

def _timed_rotator(source: str, dest: str):
    temporary = dest + ".tmp"
    try:
        with open(source, "rb") as f_in:
            with gzip.open(temporary, "wb", compresslevel=1) as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.replace(temporary, dest)
        os.remove(source)
    except Exception:
        try:
            os.remove(temporary)
        except OSError:
            pass
        # Let Handler.handleError report this; logging here would recurse.
        raise

def _timed_namer(default_name: str) -> str:
    return default_name + "." + time.strftime("%H%M%S") + "." + str(time.time_ns()) + ".gz"


class DailySizeRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """Rotate at midnight or the size limit; bound all daily/size archives together."""

    def __init__(self, filename, *, maxBytes=10 * 1024 * 1024, backupCount=3,
                 encoding="utf-8", utc=False):
        super().__init__(filename, when="midnight", interval=1,
                         backupCount=max(1, int(backupCount)), encoding=encoding, utc=utc)
        self.maxBytes = max(1, int(maxBytes))
        self.namer = _timed_namer
        self.rotator = _timed_rotator
        self._rotation_retry_at = 0.0

    def emit(self, record):
        try:
            if self.shouldRollover(record):
                try:
                    self.doRollover()
                except Exception:
                    self._rotation_retry_at = time.monotonic() + 60.0
                    self.handleError(record)
            # Rotation failure must not discard the new record. FileHandler
            # reopens the original log in append mode if rollover closed it.
            logging.FileHandler.emit(self, record)
        except Exception:
            self.handleError(record)

    def shouldRollover(self, record):
        if time.monotonic() < self._rotation_retry_at:
            return False
        if super().shouldRollover(record):
            return True
        if self.stream is None:
            self.stream = self._open()
        self.stream.seek(0, os.SEEK_END)
        size = self.stream.tell()
        encoded = (self.format(record) + self.terminator).replace("\n", os.linesep).encode(self.encoding, errors="replace")
        return size > 0 and size + len(encoded) >= self.maxBytes

    def doRollover(self):
        midnight = self.rolloverAt
        size_only = time.time() < midnight
        super().doRollover()
        if size_only:
            # A size split must not postpone the next midnight rollover.
            self.rolloverAt = midnight

    def getFilesToDelete(self):
        base = Path(self.baseFilename)
        pattern = re.compile(re.escape(base.name) + r"\.(\d{4}-\d{2}-\d{2})(?:\.(\d{6})\.(\d+))?\.gz$")
        archives = []
        for path in base.parent.iterdir():
            match = pattern.fullmatch(path.name)
            if match and path.is_file() and not path.is_symlink():
                archives.append(((match[1], int(match[3] or 0)), str(path)))
        archives.sort()
        return [path for _, path in archives[:-self.backupCount]]

def setup_logger() -> logging.Logger:
    with _logging_lifecycle_lock:
        return _setup_logger_locked()


def _setup_logger_locked() -> logging.Logger:
    global _LOG_LISTENER, _LOG_QUEUE_HANDLER
    _ensure_log_dir(LOG_DIR)
    log_parent = str(Path(LOG_FILE).expanduser().parent)
    if log_parent not in ("", "."):
        _ensure_log_dir(log_parent)

    if CONSOLE_COLOR:
        _enable_windows_ansi()
    _disable_windows_quick_edit()

    logger = logging.getLogger("autobot")
    logger.propagate = TEST_LOGGING
    configure_telethon_logging(logger)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    startup_warnings = []

    console_handler = logging.StreamHandler()
    try:
        console_level = getattr(logging, CONSOLE_LOG_LEVEL, logging.INFO)
    except Exception:
        console_level = logging.INFO

    if DEBUG_VERBOSE_MODE:
        console_handler.setLevel(logging.DEBUG)

        console_handler.setFormatter(ColoredFormatter('[%(asctime)s] [%(levelname)s] [%(context_tag)-18.18s] %(message)s', datefmt='%H:%M:%S'))

        startup_warnings.append("🔊 DEBUG_VERBOSE_MODE=true — console filter TẮT, hiện mọi log")
    else:
        console_handler.setLevel(console_level)
        console_handler.addFilter(CleanConsoleFilter())
        console_handler.addFilter(ConsoleSummaryFilter())
        console_handler.addFilter(ConsoleRepeatFilter())
        console_handler.addFilter(ConsoleWarnThrottleFilter())
        if COMPACT_CONSOLE_LOG:
            console_handler.setFormatter(ColoredFormatter('%(levelname)s %(message)s'))
        else:
            console_handler.setFormatter(ColoredFormatter('[%(asctime)s] %(levelname)s [%(context_tag)-18.18s] %(message)s', datefmt='%H:%M:%S'))

    file_handler = None
    try:
        file_handler = DailySizeRotatingFileHandler(
            filename=LOG_FILE,
            maxBytes=LOG_ROTATION_MAX_BYTES,
            backupCount=LOG_ROTATION_BACKUP_COUNT,
            encoding="utf-8",
            utc=LOG_USE_UTC,
        )
        file_handler.setLevel(getattr(logging, FILE_LOG_LEVEL, logging.INFO))

        file_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)-8s] [%(context_tag)-22.22s] %(message)s', datefmt='%d/%m/%Y %H:%M:%S'))
        try:
            file_handler.suffix = "%Y-%m-%d"
        except Exception:
            pass

        file_handler.rotator = _timed_rotator
        file_handler.namer = _timed_namer

    except Exception:
        if file_handler is not None:
            file_handler.close()
        file_handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_ROTATION_MAX_BYTES,
            backupCount=LOG_ROTATION_BACKUP_COUNT_SIZE,
            encoding="utf-8",
        )
        file_handler.setLevel(getattr(logging, FILE_LOG_LEVEL, logging.INFO))
        file_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)-8s] [%(context_tag)-22.22s] %(message)s', datefmt='%d/%m/%Y %H:%M:%S'))
        startup_warnings.append("TimedRotatingFileHandler không khả dụng — chuyển sang RotatingFileHandler kích thước")

    channel_handler = None
    if PER_CHANNEL_LOGS_ENABLED:
        try:
            channel_handler = PerChannelFileHandler(base_dir=os.path.join(LOG_DIR, "channels"))
            channel_handler.setLevel(logging.INFO)
            channel_handler.addFilter(CleanConsoleFilter())
        except Exception as e:
            if channel_handler is not None:
                channel_handler.close()
            channel_handler = None
            startup_warnings.append(f"⚠️ Không khởi tạo được log riêng theo kênh: {e}")

    handlers = [console_handler]
    if file_handler is not None:
        handlers.append(file_handler)
    if channel_handler is not None:
        handlers.append(channel_handler)
    log_queue = queue.Queue(maxsize=50000)
    queue_handler = _DroppingQueueHandler(log_queue)
    queue_handler.setLevel(min(handler.level for handler in handlers))
    queue_handler.addFilter(ContextTagFilter())
    listener = _DrainingQueueListener(log_queue, *handlers, respect_handler_level=True)
    logger.addHandler(queue_handler)
    listener.start()
    _LOG_QUEUE_HANDLER = queue_handler
    _LOG_LISTENER = listener

    for warning in startup_warnings:
        logger.warning(warning)

    if not DEBUG_VERBOSE_MODE:
        try:
            logging.getLogger("asyncio").setLevel(logging.WARNING)
            logging.getLogger("urllib3").setLevel(logging.WARNING)
            logging.getLogger("playwright").setLevel(logging.WARNING)
        except Exception:
            pass

    configure_telethon_logging(logger)

    logger.propagate = TEST_LOGGING
    return logger




def shutdown_logging() -> None:
    global _LOG_LISTENER, _LOG_QUEUE_HANDLER
    with _logging_lifecycle_lock:
        listener = _LOG_LISTENER
        if listener is None:
            return
        queue_handler = _LOG_QUEUE_HANDLER
        target = logging.getLogger("autobot")
        if queue_handler is not None:
            queue_handler.acquire()
            try:
                queue_handler._accepting = False
                target.removeHandler(queue_handler)
            finally:
                queue_handler.release()
        try:
            listener.stop()
        except Exception:
            # Retain the listener reference so a later shutdown can retry.
            if queue_handler is not None:
                queue_handler._accepting = True
                target.addHandler(queue_handler)
            return
        for handler in listener.handlers:
            try:
                handler.close()
            except Exception:
                pass
        if queue_handler is not None:
            queue_handler.close()
        _LOG_LISTENER = None
        _LOG_QUEUE_HANDLER = None


logger = setup_logger()
