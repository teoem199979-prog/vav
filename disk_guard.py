"""Conservative disk housekeeping. Never delete databases, credentials or profiles.

Media is eligible only under media/ or data/media/, and only in processed/
or with a sibling <filename>.done marker. No unmarked media is inferred done.
Browser cleanup targets cache contents only, never profile/session directories.
"""
import logging
import os
import re
import shutil
import stat
import tempfile
import threading
import time
from pathlib import Path

from sqlite_support import connect_database, ensure_performance_indexes

logger = logging.getLogger("autobot." + __name__)
_CACHE_NAMES = {"cache", "code cache", "gpucache"}
_PRIVATE_DIRS = {"sessions", "session storage", "local storage", "indexeddb", "network"}
_SKIP_DIRS = {".git", ".venv312", "__pycache__", ".vscode", ".aws", ".codex"}
_MEDIA_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v"}
_PROFILE_PREFIXES = ("chrome_bot_profile_", "edge_bot_profile_")
_ACTIVE_OCR_TMP = set()
_OCR_TMP_LOCK = threading.RLock()
_OCR_TMP_CLEANUP_LOCK = threading.Lock()


def register_ocr_tmp(path):
    """Protect a live OCR job's temporary directory until its finally block."""
    with _OCR_TMP_LOCK:
        _ACTIVE_OCR_TMP.add(os.path.normcase(os.path.abspath(path)))


def release_ocr_tmp(path):
    with _OCR_TMP_LOCK:
        _ACTIVE_OCR_TMP.discard(os.path.normcase(os.path.abspath(path)))


def _ocr_tmp_owner_alive(pid):
    if pid == os.getpid():
        return True
    try:
        import psutil
        return psutil.pid_exists(pid)
    except Exception:
        # An unknown owner is not permission to remove its working directory.
        return True


def _other_ocr_process_running():
    """Old, ownerless ocr_* directories need an additional process guard."""
    try:
        import psutil
        try:
            ancestors = {parent.pid for parent in psutil.Process(os.getpid()).parents()}
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            ancestors = set()
        own_script = (Path(__file__).resolve().parent / "main_script.py").resolve()
        for process in psutil.process_iter(["pid", "name", "cmdline", "cwd"]):
            if process.info["pid"] == os.getpid():
                continue
            try:
                args = process.info.get("cmdline")
                if args is None and "python" in str(process.info.get("name", "")).lower():
                    return True
                for arg in (args or ()):
                    script = Path(str(arg))
                    if script.name.lower() != "main_script.py":
                        continue
                    if process.info["pid"] in ancestors:
                        cwd = process.info.get("cwd")
                        if script.is_absolute() or cwd:
                            resolved = (script if script.is_absolute() else Path(cwd) / script).resolve()
                            if resolved == own_script:
                                # Windows' venv launcher waits for this child;
                                # it cannot own a concurrent OCR job itself.
                                continue
                    return True
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                return True
        return False
    except Exception:
        return True


def _stale_ocr_tree_size(root, directory, cutoff, *, budget=None):
    """Preflight the complete tree; None means leave every file untouched."""
    def abort_walk(error):
        raise error

    total = 0
    count = 0
    for folder, dirs, files in os.walk(directory, followlinks=False, onerror=abort_walk):
        parent = Path(folder)
        for path in [parent, *[parent / name for name in dirs + files]]:
            count += 1
            if budget is not None:
                budget[0] -= 1
                if budget[0] < 0:
                    return None
            if count > 10000 or not _inside(root, path):
                return None
            name = path.name.lower()
            if (_protected(path) or ".session" in name
                    or name.startswith(_PROFILE_PREFIXES)):
                return None
            info = path.lstat()
            if info.st_mtime >= cutoff:
                return None
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
            elif not stat.S_ISDIR(info.st_mode):
                return None
    return total


def _cleanup_stale_ocr_tmp(max_age_seconds=3 * 86400):
    """Remove abandoned OCR temp trees, never a live job or a linked tree.

    New jobs use ocr_pid<pid>_<random> names and register_ocr_tmp(). Legacy ocr_*
    trees are eligible only when no other AutoBot process may still own them.
    The age applies to every descendant, not just the outer directory.
    """
    result = {"directories_removed": 0, "bytes_removed": 0, "scanned": 0}
    if not _OCR_TMP_CLEANUP_LOCK.acquire(blocking=False):
        return result
    try:
        age = max(3600.0, float(max_age_seconds))
        cutoff = time.time() - age
        root = Path(tempfile.gettempdir()).absolute()
        if _linked(root) or not root.is_dir():
            return result
        root = root.resolve(strict=True)
        legacy_busy = None
        candidates = 0
        tree_budget = [10000]
        with os.scandir(root) as entries:
            for entry in entries:
                result["scanned"] += 1
                if result["scanned"] > 10000 or tree_budget[0] <= 0:
                    break
                if not re.fullmatch(r"ocr_[A-Za-z0-9_]+", entry.name):
                    continue
                candidates += 1
                if candidates > 128:
                    break
                path = root / entry.name
                try:
                    if path.parent != root or not _inside(root, path) or not path.is_dir():
                        continue
                    if path.stat().st_mtime >= cutoff:
                        continue
                    owner = re.match(r"ocr_pid(\d+)_", entry.name)
                    if owner and int(owner[1]) != os.getpid() and _ocr_tmp_owner_alive(int(owner[1])):
                        continue
                    if not owner:
                        if legacy_busy is None:
                            legacy_busy = _other_ocr_process_running()
                        if legacy_busy:
                            continue
                    key = os.path.normcase(os.path.abspath(path))
                    with _OCR_TMP_LOCK:
                        if key in _ACTIVE_OCR_TMP:
                            continue
                    size = _stale_ocr_tree_size(root, path, cutoff, budget=tree_budget)
                    if size is None:
                        continue
                    # Recheck the absolute target immediately before recursion.
                    with _OCR_TMP_LOCK:
                        if key in _ACTIVE_OCR_TMP or not _inside(root, path):
                            continue
                        shutil.rmtree(path)
                    result["directories_removed"] += 1
                    result["bytes_removed"] += size
                except OSError as exc:
                    logger.debug("OCR temp cleanup skipped %s: %s", path, exc)
        if result["directories_removed"]:
            logger.info("OCR temp cleanup removed %s abandoned directories (%s bytes)",
                        result["directories_removed"], result["bytes_removed"])
        return result
    except (OSError, TypeError, ValueError) as exc:
        logger.debug("OCR temp cleanup skipped: %s", exc)
        return result
    finally:
        _OCR_TMP_CLEANUP_LOCK.release()


def _linked(path):
    info = path.lstat()
    return path.is_symlink() or bool(getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _inside(root, path):
    """Reject links/junctions at every level, including a linked parent folder."""
    try:
        relative = path.absolute().relative_to(root)
        current = root
        if _linked(current):
            return False
        for part in relative.parts:
            current = current / part
            if _linked(current):
                return False
        return path.resolve().is_relative_to(root)
    except (OSError, ValueError):
        return False


def _protected(path):
    name = path.name.lower()
    return (name in {".env", "config.py", "cookies", "cookies-journal", "login data",
                     "login data-journal", "local state", "preferences", "secure preferences",
                     "history", "web data"}
            or name.endswith((".db", ".db-wal", ".db-shm", ".db-journal", ".sqlite", ".sqlite3",
                              ".session", ".session-journal", ".session-wal", ".session-shm")))


def _walk(root, directory, *, skip_profiles=False):
    if not _inside(root, directory) or not directory.is_dir():
        return
    for folder, dirs, files in os.walk(directory, followlinks=False):
        parent = Path(folder)
        dirs[:] = [name for name in dirs if name.lower() not in _SKIP_DIRS | _PRIVATE_DIRS
                   and not (skip_profiles and (name.lower() == "backups" or name.lower().startswith(_PROFILE_PREFIXES)))
                   and _inside(root, parent / name)]
        for name in files:
            path = parent / name
            if _inside(root, path):
                yield path


def _delete(root, path):
    parts = [part.lower() for part in path.relative_to(root).parts[:-1]]
    if any(part.startswith(_PROFILE_PREFIXES) for part in parts):
        if not any(part in _CACHE_NAMES for part in parts) or any(part in _PRIVATE_DIRS for part in parts):
            return 0
    if _protected(path) or not _inside(root, path) or not path.is_file():
        return 0
    try:
        size = path.stat().st_size
        path.unlink()
        return size
    except OSError as exc:
        logger.debug("Disk cleanup skipped %s: %s", path, exc)
        return 0


def _active_logs():
    active = set()
    loggers = [logging.getLogger(), *[obj for obj in logging.Logger.manager.loggerDict.values()
                                    if isinstance(obj, logging.Logger)]]
    for log in loggers:
        for handler in log.handlers:
            children = getattr(handler, "_handlers", {})
            for candidate in [handler, *list(children.values())]:
                filename = getattr(candidate, "baseFilename", None)
                if filename:
                    active.add(Path(filename).resolve())
    return active


def cleanup_daily(base_dir):
    root = Path(base_dir).absolute()
    removed = 0
    now = time.time()
    active = _active_logs()
    for path in _walk(root, root / "logs"):
        if re.search(r"\.log(?:$|[.\-_])", path.name, re.IGNORECASE):
            if path.resolve() not in active and now - path.stat().st_mtime > 3 * 86400:
                removed += _delete(root, path)
    for directory in (root / "media", root / "data" / "media"):
        for path in _walk(root, directory):
            if path.suffix.lower() not in _MEDIA_SUFFIXES:
                continue
            marker = path.with_name(path.name + ".done")
            processed = "processed" in path.relative_to(directory).parts[:-1]
            marked = _inside(root, marker) and marker.is_file() and now - marker.stat().st_mtime > 86400
            if (processed or marked) and now - path.stat().st_mtime > 86400:
                removed += _delete(root, path)
                if not path.exists() and marked:
                    removed += _delete(root, marker)
    profiles = (*root.glob("chrome_bot_profile_*"), *root.glob("edge_bot_profile_*"))
    for profile in profiles:
        if not _inside(root, profile) or not profile.is_dir():
            continue
        for folder, dirs, _ in os.walk(profile, followlinks=False):
            parent = Path(folder)
            safe = [name for name in dirs if name.lower() not in _PRIVATE_DIRS and _inside(root, parent / name)]
            dirs[:] = [name for name in safe if name.lower() not in _CACHE_NAMES]
            for name in safe:
                if name.lower() in _CACHE_NAMES:
                    for path in _walk(root, parent / name):
                        removed += _delete(root, path)
    logger.info("Daily disk cleanup freed %s bytes", removed)
    return removed


def _purge_done(connection):
    removed = 0
    for table in ("telegram_inbox", "durable_inbox"):
        columns = {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}
        if not {"id", "status", "created_at"} <= columns:
            continue
        stamp = "COALESCE(completed_at, created_at)" if "completed_at" in columns else "created_at"
        query = f'SELECT id FROM "{table}" WHERE status IN (\'done\', \'completed\') AND julianday({stamp}) < julianday(\'now\', \'-30 days\')'
        child_columns = {row[1] for row in connection.execute('PRAGMA table_info("telegram_inbox_items")')}
        if table == "telegram_inbox" and {"inbox_id", "status"} <= child_columns:
            query += " AND NOT EXISTS (SELECT 1 FROM telegram_inbox_items i WHERE i.inbox_id=telegram_inbox.id AND i.status NOT IN ('done','completed','ignored'))"
            connection.execute(f"DELETE FROM telegram_inbox_items WHERE inbox_id IN ({query})")
            removed += connection.execute(f"DELETE FROM telegram_inbox WHERE id IN ({query})").rowcount
        else:
            removed += connection.execute(f'DELETE FROM "{table}" WHERE id IN ({query})').rowcount
    return removed


def cleanup_weekly(base_dir):
    root = Path(base_dir).absolute()
    result = {"bytes_removed": 0, "rows_removed": 0, "databases_vacuumed": 0}
    backups = root / "backups"
    if _inside(root, backups) and backups.is_dir():
        archives = [path for path in backups.glob("*.zip") if _inside(root, path) and path.is_file()]
        archives.sort(key=lambda path: (path.stat().st_mtime, path.name), reverse=True)
        for path in archives[3:]:
            result["bytes_removed"] += _delete(root, path)
    for path in _walk(root, root, skip_profiles=True):
        if path.suffix.lower() != ".db":
            continue
        connection = None
        try:
            connection = connect_database(path, timeout=5.0)
            connection.execute("PRAGMA foreign_keys=ON")
            with connection:
                ensure_performance_indexes(connection)
                result["rows_removed"] += _purge_done(connection)
            connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
            # VACUUM can need roughly twice the DB size in temporary free space.
            if shutil.disk_usage(root).free >= 2 * path.stat().st_size + 16 * 1024 * 1024:
                connection.execute("VACUUM")
                result["databases_vacuumed"] += 1
            else:
                logger.warning("Skipping VACUUM without sufficient temporary disk space: %s", path)
        except Exception as exc:
            logger.warning("Database maintenance skipped %s: %s", path, exc)
        finally:
            if connection is not None:
                connection.close()
    logger.info("Weekly disk cleanup: %s", result)
    return result


def ensure_disk_space(base_dir, min_free_gb=2.0):
    root = Path(base_dir).absolute()
    minimum = max(0.0, float(min_free_gb)) * 1024 ** 3
    usage = shutil.disk_usage(root)
    if usage.used / usage.total > 0.85 or usage.free < minimum:
        cleanup_daily(root)
        cleanup_weekly(root)
        usage = shutil.disk_usage(root)
    if usage.used / usage.total > 0.95 or usage.free < minimum:
        logger.critical("Disk remains unsafe: %.1f%% used, %.2f GiB free", usage.used / usage.total * 100, usage.free / 1024 ** 3)
        return False
    return True
