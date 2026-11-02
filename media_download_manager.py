from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import time
import weakref
from collections import deque
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

from logger_setup import logger
from media_identity import telegram_media_identity
from telethon.errors import FloodWaitError
import sys

_partial_locks = {}
_active_partial_dirs = set()
_media_dc_locks = weakref.WeakKeyDictionary()


def _main_module():
    mod = sys.modules.get("__main__")
    if mod is not None and hasattr(mod, "client"):
        return mod
    import main_script as mod
    return mod


# Tăng thời gian timeout mặc định cho toàn bộ tiến trình tải file lớn
DEFAULT_DOWNLOAD_TIMEOUT_SECONDS = 300.0

@dataclass
class DownloadMetrics:
    path: Optional[str]
    size_bytes: Optional[int]
    elapsed_ms: float
    bytes_per_sec: Optional[float]
    attempts: int
    dedup_hit: bool


class MediaDownloadManager:

    def __init__(
        self,
        max_concurrent: int = 2,
        retries: int = 1,
        retry_delay: float = 0.5,
        max_size_bytes: Optional[int] = None,
        fast_download: bool = False,
        fast_min_bytes: int = 8 * 1024 * 1024,  # Nâng ngưỡng lên 8MB để tránh chia nhỏ file vừa/nhỏ gây overhead
        fast_workers: int = 4,
        fast_chunk_kb: int = 512,
    ):
        self.semaphore = asyncio.Semaphore(max(1, int(max_concurrent)))
        self.retries = max(0, int(retries))
        self.retry_delay = max(0.0, float(retry_delay))

        self.max_size_bytes = int(max_size_bytes) if max_size_bytes else None
        self.fast_download = bool(fast_download)
        self.fast_min_bytes = max(1024 * 1024, int(fast_min_bytes))
        self.fast_workers = max(2, min(8, int(fast_workers)))
        self.fast_chunk_size = max(128, min(2048, int(fast_chunk_kb))) * 1024
        self._inflight: dict[tuple[Any, ...], asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()
        self._fallback_dc_locks = {}

    async def _ensure_client_connected(self, client: Any) -> None:
        if client is None or client.is_connected():
            return
        async with self._connect_lock:
            if client.is_connected():
                return
            main_module = _main_module()
            config = getattr(main_module, "Config", None)
            timeout = max(
                1.0,
                float(getattr(config, "TELEGRAM_CONNECT_TIMEOUT", 20.0)),
            )
            logger.warning(
                "⚠️ [Download] Telegram disconnected — reconnect trước khi tải media"
            )
            await asyncio.wait_for(client.connect(), timeout=timeout)
            if not client.is_connected():
                raise ConnectionError("Telegram client chưa connected sau reconnect")

    async def _warm_media_sender(self, client: Any, dc_id: Any) -> None:
        if not dc_id or dc_id == getattr(getattr(client, 'session', None), 'dc_id', None):
            return
        borrow = getattr(client, '_borrow_exported_sender', None)
        give_back = getattr(client, '_return_exported_sender', None)
        if not callable(borrow) or not callable(give_back):
            return
        from config import Config
        self._revive_dead_exported_senders(client)
        task = asyncio.create_task(borrow(dc_id))
        try:
            await asyncio.wait_for(task, timeout=2 * max(
                5.0, float(getattr(Config, 'MEDIA_DC_SETUP_TIMEOUT_SECONDS', 20.0))) + 1.0)
        finally:
            if task.done() and not task.cancelled() and task.exception() is None:
                returned = asyncio.create_task(give_back(task.result()))
                try:
                    await asyncio.shield(returned)
                except asyncio.CancelledError:
                    await returned
                    raise

    @staticmethod
    async def _reset_idle_media_sender(client: Any, dc_id: Any) -> bool:
        if not dc_id or dc_id == getattr(getattr(client, 'session', None), 'dc_id', None):
            return False
        lock = getattr(client, '_borrow_sender_lock', None)
        borrowed = getattr(client, '_borrowed_senders', None)
        if lock is None or not isinstance(borrowed, dict):
            return False
        async with lock:
            pair = borrowed.get(dc_id)
            if pair is None:
                return False
            state, sender = pair
            if getattr(state, '_n', None) != 0:
                logger.warning('[Media-DC] reset deferred dc=%s active_borrows=%s', dc_id, getattr(state, '_n', None))
                return False
            borrowed.pop(dc_id)
            failed = getattr(client, 'note_media_sender_failed', None)
            if callable(failed):
                failed(dc_id, sender)
            try:
                await asyncio.wait_for(sender.disconnect(), 5.0)
            except Exception:
                logger.warning('[Media-DC] old sender disconnect failed dc=%s', dc_id)
            logger.warning('[Media-DC] evicted stalled sender dc=%s; next attempt creates a fresh transport', dc_id)
            return True

    @staticmethod
    def _revive_dead_exported_senders(client: Any) -> int:
        borrowed = getattr(client, "_borrowed_senders", None)
        if not isinstance(borrowed, dict):
            return 0
        revived = 0
        for dc_id, pair in list(borrowed.items()):
            try:
                state, sender = pair
                if sender is not None and not sender.is_connected():
                    state._connected = False
                    revived += 1
                    logger.warning(
                        "⚠️ [Download] Sender media DC %s đã rớt — đánh dấu nối lại", dc_id
                    )
            except Exception:
                continue
        return revived

    @staticmethod
    def key_for(event: Any) -> tuple[Any, Any, Any]:
        message = getattr(event, "message", None)
        media = getattr(message, "media", None)
        media_id = None
        try:
            if getattr(media, "document", None) is not None:
                media_id = getattr(media.document, "id", None)
            elif getattr(media, "photo", None) is not None:
                media_id = getattr(media.photo, "id", None)
        except Exception:
            media_id = None
        return (
            getattr(event, "chat_id", None),
            getattr(message, "id", None),
            media_id,
        )

    async def download(
        self, event: Any, output_dir: str | Path, timeout: Optional[float] = None,
    ) -> DownloadMetrics:
        timeout_s = float(timeout) if timeout and float(timeout) > 0 else DEFAULT_DOWNLOAD_TIMEOUT_SECONDS

        key = (*self.key_for(event), str(Path(output_dir).resolve()))
        async with self._lock:
            current = self._inflight.get(key)
            if current is None or current.done():
                task = asyncio.create_task(self._download_once(event, output_dir))
                self._inflight[key] = task
                task.add_done_callback(
                    lambda completed, _key=key, _task=task: asyncio.create_task(
                        self._remove_inflight_when_done(_key, _task)
                    )
                )
                dedup_hit = False
            else:
                task = current
                dedup_hit = True

        try:
            try:
                result = await asyncio.wait_for(asyncio.shield(task), timeout=timeout_s)
            except asyncio.TimeoutError:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                logger.error(f"❌ [Download] Timeout {timeout_s:g}s chờ tải xong (key={key})")
                result = DownloadMetrics(
                    path=None, size_bytes=None, elapsed_ms=timeout_s * 1000.0,
                    bytes_per_sec=None, attempts=0, dedup_hit=dedup_hit,
                )

            if dedup_hit:
                result = DownloadMetrics(
                    path=result.path,
                    size_bytes=result.size_bytes,
                    elapsed_ms=result.elapsed_ms,
                    bytes_per_sec=result.bytes_per_sec,
                    attempts=result.attempts,
                    dedup_hit=True,
                )
            return result
        except asyncio.CancelledError:
            if not dedup_hit:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            raise
        finally:
            if not dedup_hit and task.done():
                async with self._lock:
                    if self._inflight.get(key) is task:
                        self._inflight.pop(key, None)

    async def _remove_inflight_when_done(self, key, task) -> None:
        async with self._lock:
            if self._inflight.get(key) is task:
                self._inflight.pop(key, None)

    async def _download_once(self, event: Any, output_dir: str | Path) -> DownloadMetrics:
        started = time.perf_counter()
        attempts = 0
        path = None

        async with self._dc_transfer_gate(event), self.semaphore:
            for attempts in range(1, self.retries + 2):
                try:
                    path = await self._download_with_size_check(event, output_dir)
                    if path:
                        break
                except asyncio.CancelledError:
                    raise
                except FloodWaitError as exc:
                    logger.warning('[Download] FLOOD_WAIT seconds=%s; no immediate retry/fallback', exc.seconds)
                    break
                except Exception as e:
                    if attempts > self.retries or getattr(e, 'media_retry_owned', False):
                        logger.error(f"❌ [Download] Thất bại sau {attempts} lần thử: {e}")
                        break
                    else:
                        wait_s = self.retry_delay * attempts
                        logger.warning(
                            f"⚠️ [Download] Lần {attempts} lỗi ({e}) → retry sau {wait_s:.1f}s"
                        )
                        await asyncio.sleep(wait_s)

        elapsed = max(time.perf_counter() - started, 1e-9)
        size_bytes = None
        if path:
            try:
                size_bytes = Path(path).stat().st_size
            except OSError:
                pass

        bytes_per_sec = round(size_bytes / elapsed, 2) if size_bytes is not None else None

        if path and size_bytes is not None:
            logger.debug(
                f"✅ [Download] {Path(path).name} — {size_bytes / 1024:.1f}KB, "
                f"{elapsed * 1000:.0f}ms"
                + (f", {bytes_per_sec / 1_000_000:.2f}MB/s" if bytes_per_sec else "")
                + f" (lần {attempts})"
            )

        return DownloadMetrics(
            path=str(path) if path else None,
            size_bytes=size_bytes,
            elapsed_ms=round(elapsed * 1000, 2),
            bytes_per_sec=bytes_per_sec,
            attempts=attempts,
            dedup_hit=False,
        )

    def _dc_transfer_gate(self, event: Any):
        message = getattr(event, 'message', None)
        media = getattr(message, 'media', None)
        document = getattr(media, 'document', None) or getattr(media, 'photo', None)
        dc_id = getattr(document, 'dc_id', None)
        if not dc_id:
            return nullcontext()
        client = getattr(_main_module(), 'client', None)
        if client is not None and dc_id and dc_id != getattr(getattr(client, 'session', None), 'dc_id', None):
            try:
                locks = _media_dc_locks.setdefault(client, {})
                lock = locks.setdefault(dc_id, asyncio.Lock())
            except TypeError:
                lock = self._fallback_dc_locks.setdefault((id(client), dc_id), asyncio.Lock())
            return lock
        return nullcontext()

    async def _download_with_size_check(self, event: Any, output_dir: str | Path) -> Optional[str]:
        message = getattr(event, "message", None)
        media = getattr(message, "media", None) if message else None

        declared_size = None
        try:
            if getattr(media, "document", None) is not None:
                declared_size = getattr(media.document, "size", None)
        except Exception:
            declared_size = None

        if self.max_size_bytes and declared_size and declared_size > self.max_size_bytes:
            raise ValueError(
                f"File quá khổ (khai báo trước): {declared_size / 1_000_000:.1f}MB > "
                f"{self.max_size_bytes / 1_000_000:.1f}MB giới hạn"
            )

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        telegram_client = None
        if self.fast_download and declared_size and declared_size >= self.fast_min_bytes:
            try:
                _ms = _main_module()
                telegram_client = getattr(_ms, "client", None)
                if telegram_client is not None and getattr(media, "document", None) is not None:
                    await self._ensure_client_connected(telegram_client)
                    fast_path = await self._multipart_download(
                        telegram_client, message, output_dir, declared_size,
                        chat_id=getattr(event, "chat_id", None),
                    )
                    if fast_path:
                        return fast_path
                    logger.warning("⚠️ [Download] Multipart không trả file, fallback Telethon")
            except asyncio.CancelledError:
                raise
            except FloodWaitError:
                raise
            except Exception as exc:
                if getattr(exc, 'completed_chunks', 0) or getattr(exc, 'media_retry_owned', False):
                    raise
                logger.warning("⚠️ [Download] Multipart lỗi, fallback Telethon: %s", exc)
        
        downloader = getattr(event, "download_media", None)
        if downloader is None and message is not None:
            downloader = getattr(message, "download_media", None)
        if downloader is not None:
            _ms = _main_module()
            telegram_client = telegram_client or getattr(_ms, "client", None)
            await self._ensure_client_connected(telegram_client)
            path = await downloader(file=str(output_dir))
        else:
            _ms = _main_module()
            telegram_client = telegram_client or getattr(_ms, "client", None)
            if telegram_client is None or message is None:
                return None
            await self._ensure_client_connected(telegram_client)
            path = await telegram_client.download_media(message, file=str(output_dir))

        if not path:
            return None

        if self.max_size_bytes:
            try:
                actual_size = Path(path).stat().st_size
            except OSError:
                actual_size = None
            if actual_size is not None and actual_size > self.max_size_bytes:
                try:
                    Path(path).unlink(missing_ok=True)
                except Exception:
                    pass
                raise ValueError(
                    f"File tải về quá khổ: {actual_size / 1_000_000:.1f}MB > "
                    f"{self.max_size_bytes / 1_000_000:.1f}MB giới hạn"
                )

        return path

    async def _multipart_download(
        self, client: Any, message: Any, output_dir: str | Path, file_size: int,
        *, chat_id: Any = None,
    ) -> Optional[str]:
        chat_id = chat_id if chat_id is not None else getattr(message, "chat_id", None)
        identity = (str(chat_id), str(getattr(message, "id", "")), telegram_media_identity(message), int(file_size))
        key = (asyncio.get_running_loop(), identity)
        lock = _partial_locks.setdefault(key, asyncio.Lock())
        async with lock:
            try:
                return await self._multipart_download_locked(client, message, output_dir, file_size, chat_id=chat_id)
            finally:
                if not lock._waiters:
                    _partial_locks.pop(key, None)

    async def _multipart_download_locked(
        self, client: Any, message: Any, output_dir: str | Path, file_size: int,
        *, chat_id: Any = None,
    ) -> Optional[str]:
        chat_id = chat_id if chat_id is not None else getattr(message, "chat_id", None)
        media_id = telegram_media_identity(message)
        if chat_id is None or media_id is None:
            raise ValueError("Multipart download requires a chat ID and stable media ID")
        media_key = {
            "chat_id": str(chat_id),
            "message_id": str(getattr(message, "id", "")),
            "media_id": media_id,
            "file_size": int(file_size),
        }
        session = json.dumps(media_key, sort_keys=True, separators=(",", ":"))
        file_key = hashlib.sha256(session.encode("utf-8")).hexdigest()
        file_info = getattr(message, "file", None)
        name = getattr(file_info, "name", None) or ""
        if not name:
            ext = getattr(file_info, "ext", None) or ""
            name = f"telegram_{getattr(message, 'id', 'media')}{ext}"
        suffix = Path(name).suffix
        if len(suffix) > 12 or not suffix[1:].isalnum():
            suffix = ""
        final_path = Path(output_dir) / f"telegram_{file_key}{suffix}"
        from config import Config
        
        # Đã tăng thời gian chờ mỗi chunk lên 40.0s (hoặc cấu hình tùy chỉnh qua Config)
        chunk_timeout = float(getattr(Config, "FAST_DOWNLOAD_CHUNK_TIMEOUT_SECONDS", 40.0))
        
        partial_dir = Path(getattr(Config, "OCR_VIDEO_FILE_CACHE_DIR", "data/ocr_video_cache")).parent / "ocr_video_partial" / file_key
        partial_dir.mkdir(parents=True, exist_ok=True)
        temp_path = partial_dir / (final_path.name + ".multipart.part")
        manifest_path = temp_path.with_name(temp_path.name + ".json")
        if final_path.is_file():
            try:
                if final_path.stat().st_size == file_size:
                    for stale in (temp_path, manifest_path):
                        stale.unlink(missing_ok=True)
                    try:
                        partial_dir.rmdir()
                    except OSError:
                        pass
                    logger.debug("[Download] cache hit %s", final_path.name)
                    return str(final_path)
                final_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.debug("[Download] cache inspect failed: %s", exc)
        channel = getattr(Config, "CHANNEL_CONFIG", {}).get(chat_id, {})
        log_chunk_timing = "llwincode.com" in str(channel.get("url", "")).lower()
        chunk = min(self.fast_chunk_size, 512 * 1024)
        chunks = max(1, (file_size + chunk - 1) // chunk)
        if file_size < 8 * 1024 * 1024:
            size_workers = 2
        elif file_size < 16 * 1024 * 1024:
            size_workers = 6 if log_chunk_timing else 4
        else:
            size_workers = 8
        # PATCH_MARKER_2026-11-02-llwin-download-workers-8
        # LLwin video ~10.5MB = 21 chunk. Tin 2146 (23:36 02/11): chunk trung
        # binh 27.2s, 6 lane can ceil(21/6)=4 vong ~109s, vuot timeout tai
        # (tuoi ma con lai 114.2s) nen chi xong 20/21 chunk roi bi huy.
        # 8 lane can 3 vong ~82s, du cho OCR + submit trong 120s. Chi ap cho
        # LLwin (log_chunk_timing); site khac giu nguyen fast_workers/size.
        if log_chunk_timing and 8 * 1024 * 1024 <= file_size < 16 * 1024 * 1024:
            size_workers = 8
            workers = min(max(self.fast_workers, 8), size_workers, chunks)
        else:
            workers = min(self.fast_workers, size_workers, chunks)
        document = getattr(getattr(message, "media", None), "document", None)
        file_dc = getattr(document, "dc_id", None)
        session_dc = getattr(getattr(client, "session", None), "dc_id", None)
        channel_factory = getattr(client, 'new_media_download_channel', None)
        isolated = bool(file_dc and session_dc and file_dc != session_dc and callable(channel_factory))
        if file_dc and session_dc and file_dc != session_dc and not isolated:
            workers = min(workers, max(1, int(getattr(Config, 'MEDIA_DC_MAX_PARALLEL_REQUESTS', 2))))
        done: set[int] = set()
        try:
            saved = json.loads(manifest_path.read_text(encoding="utf-8"))
            if (
                saved.get("file_size") == file_size
                and saved.get("chunk_size") == chunk
                and saved.get("session") == session
                and saved.get("media_key") == media_key
            ):
                done = {int(i) for i in saved.get("chunks_done", []) if 0 <= int(i) < chunks}
        except (OSError, ValueError, TypeError, AttributeError):
            done = set()
        if not temp_path.exists() or temp_path.stat().st_size != file_size:
            with temp_path.open("wb") as handle:
                handle.truncate(file_size)
            done.clear()
        resumed_count = len(done)
        manifest_lock = asyncio.Lock()
        retry_count = 0
        chunk_attempts = {}
        chunk_errors = {}
        started = time.perf_counter()

        manifest_writes = 0
        last_manifest_write = 0.0

        def write_manifest_sync() -> None:
            nonlocal manifest_writes, last_manifest_write
            payload = {
                "file_size": file_size,
                "chunk_size": chunk,
                "chunks_done": sorted(done),
                "session": session,
                "media_key": media_key,
            }
            tmp_manifest = manifest_path.with_name(manifest_path.name + ".tmp")
            with tmp_manifest.open("w", encoding="utf-8") as stream:
                stream.write(json.dumps(payload))
            os.replace(tmp_manifest, manifest_path)
            manifest_writes += 1
            last_manifest_write = time.perf_counter()

        async def save_manifest(force: bool = False) -> None:
            if not force and time.perf_counter() - last_manifest_write < 1.0:
                return
            write_manifest_sync()

        async def fetch_chunk(index: int, handle, download_client=client) -> None:
            nonlocal retry_count
            if index in done:
                return
            offset = index * chunk
            remaining = min(chunk, file_size - offset)
            for _ in range(1 if isolated else 3):
                attempt = chunk_attempts.get(index, 0) + 1
                if attempt > 3:
                    raise chunk_errors[index]
                chunk_attempts[index] = attempt
                received = 0
                chunk_started = time.perf_counter()
                try:
                    await self._ensure_client_connected(client)
                    chunk_started = time.perf_counter()
                    chunk_status = "incomplete"
                    stream = None
                    try:
                        stream = download_client.iter_download(
                            message,
                            offset=offset,
                            limit=1,
                            chunk_size=chunk,
                            request_size=chunk,
                            file_size=file_size,
                        )
                        try:
                            data = await asyncio.wait_for(anext(stream), timeout=chunk_timeout)
                        except StopAsyncIteration:
                            data = b""
                        if data:
                            block = bytes(data)[:remaining]
                            if block:
                                handle.seek(offset)
                                handle.write(block)
                                received += len(block)
                        chunk_status = "complete" if received == remaining else "short"
                    except asyncio.CancelledError:
                        chunk_status = "cancelled"
                        raise
                    except asyncio.TimeoutError as exc:
                        chunk_status = "timeout"
                        raise TimeoutError(
                            f"Multipart chunk {index + 1}/{chunks} stalled for {chunk_timeout:g}s"
                        ) from exc
                    except Exception:
                        chunk_status = "error"
                        raise
                    finally:
                        if stream is not None:
                            close = getattr(stream, "close", None) or getattr(stream, "aclose", None)
                            if close is not None:
                                try:
                                    await close()
                                except Exception as exc:
                                    logger.warning("[Download] iterator close failed: %s", type(exc).__name__)
                        if log_chunk_timing:
                            logger.info(
                                "[LLWIN-Download-Chunk] message=%s file_dc=%s session_dc=%s "
                                "chunk=%s/%s attempt=%s bytes=%s expected=%s iter_ms=%.1f status=%s",
                                getattr(message, "id", None), file_dc, session_dc, index + 1,
                                chunks, attempt, received, remaining,
                                (time.perf_counter() - chunk_started) * 1000, chunk_status,
                            )
                    if received != remaining:
                        raise IOError(f"multipart chunk mismatch at {offset}: {received}/{remaining} bytes")
                    note_success = getattr(download_client, 'note_media_download_success', None)
                    if isolated and callable(note_success):
                        note_success(file_dc)
                    handle.flush()
                    async with manifest_lock:
                        done.add(index)
                        await save_manifest()
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    chunk_errors[index] = exc
                    exc.media_request_started = chunk_started
                    if isolated:
                        raise
                    if isinstance(exc, (TimeoutError, ConnectionError)):
                        raise
                    if attempt >= 3:
                        raise
                    main_down = not client.is_connected()
                    revived = 0
                    if isinstance(exc, ConnectionError):
                        revived = self._revive_dead_exported_senders(client)
                    if not main_down and not revived and not isinstance(exc, (TimeoutError, ConnectionError, IOError)):
                        raise
                    retry_count += 1
                    await self._ensure_client_connected(client)
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))

        _active_partial_dirs.add(partial_dir.resolve())
        try:
            for transport_attempt in range(1, 2 if isolated else 4):
                lanes = []
                channels = []
                failure = None
                lanes_ok = False
                try:
                    if not isolated:
                        await self._warm_media_sender(client, file_dc)
                    active_workers = workers if transport_attempt == 1 else 1
                    pending = deque(index for index in range(chunks) if index not in done)
                    if isolated:
                        channels = [channel_factory(file_dc, lane=i, chat_id=chat_id,
                                                    message_id=getattr(message, 'id', None))
                                    for i in range(active_workers)]
                    async def recovery_lane(lane):
                        nonlocal retry_count
                        download_client = channels[lane] if isolated else client
                        with temp_path.open('r+b') as handle:
                            while pending:
                                index = pending.popleft()
                                for attempt in range(1, 4 if isolated else 2):
                                    request_started = time.perf_counter()
                                    try:
                                        if isolated:
                                            await download_client.prepare()
                                        await fetch_chunk(index, handle, download_client)
                                        break
                                    except (TimeoutError, ConnectionError, OSError) as exc:
                                        if not isolated or attempt >= 3:
                                            raise
                                        retry_count += 1
                                        await download_client.recover(since=getattr(
                                            exc, 'media_request_started', request_started))
                                        logger.warning('[Media-DC] recover lane=%s chunk=%s/%s attempt=%s; healthy lanes continue',
                                                       lane, index + 1, chunks, attempt + 1)
                                        await self._ensure_client_connected(client)
                                        await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
                    lanes = [asyncio.create_task(recovery_lane(lane)) for lane in range(active_workers)]
                    await asyncio.gather(*lanes)
                    lanes_ok = True
                except Exception as exc:
                    if isolated:
                        exc.media_retry_owned = True
                        exc.completed_chunks = len(done)
                        raise
                    if not isinstance(exc, (TimeoutError, ConnectionError)):
                        raise
                    failure = exc
                finally:
                    for lane in lanes:
                        if not lane.done():
                            lane.cancel()
                    await asyncio.gather(*lanes, return_exceptions=True)
                    await asyncio.gather(
                        *((channel.release() if lanes_ok and hasattr(channel, "release")
                           else channel.aclose()) for channel in channels),
                        return_exceptions=True)
                if failure is None:
                    break
                await self._reset_idle_media_sender(client, file_dc)
                if transport_attempt >= 3:
                    failure.completed_chunks = len(done)
                    raise failure
                retry_count += 1
                await self._ensure_client_connected(client)
                await asyncio.sleep(0.5 * (2 ** (transport_attempt - 1)))
            if len(done) != chunks:
                raise IOError(f"multipart manifest incomplete: {len(done)}/{chunks}")
            try:
                temp_path.replace(final_path)
            except OSError:
                shutil.copyfile(temp_path, final_path)
                temp_path.unlink()
            manifest_path.unlink(missing_ok=True)
            try:
                partial_dir.rmdir()
            except OSError:
                pass
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            logger.info(
                "[METRIC] step=media_download multipart_ms=%.1f bytes=%s "
                "chunks_total=%s chunks_resumed=%s workers=%s retry_count=%s speed_MBps=%.2f",
                elapsed_ms,
                file_size,
                chunks,
                resumed_count,
                workers,
                retry_count,
                (file_size / max(time.perf_counter() - started, 1e-9)) / 1_000_000,
            )
            return str(final_path)
        except BaseException:
            try:
                write_manifest_sync()
            except Exception:
                pass
            raise
        finally:
            _active_partial_dirs.discard(partial_dir.resolve())