from __future__ import annotations

import asyncio
import inspect
import logging
import math
import random
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Iterable, Optional
from config import Config
from telethon.errors import RpcCallFailError

_log = logging.getLogger("autobot." + __name__)

# PATCH_MARKER_2026-10-28-ingest-bounded-dedup-full
# PATCH_MARKER_2026-10-26-v6-async-ingest: main reuses this as message_queue.
RAW_INGEST_QUEUE = asyncio.Queue(maxsize=int(getattr(Config, "INGEST_QUEUE_MAXSIZE", 512)))


@dataclass
class RawIngressItem:
    event: Any
    key: tuple
    edited: bool
    seen_perf: float


def _message_dedup_key(chat_id: int, message: Any, edited: bool) -> tuple:
    edit_dt = getattr(message, "edit_date", None)
    edit_ts = int(edit_dt.timestamp()) if edit_dt else 0
    spoiler_positions = tuple(
        (getattr(entity, "offset", None), getattr(entity, "length", None))
        for entity in (getattr(message, "entities", None) or [])
        if type(entity).__name__ == "MessageEntitySpoiler"
    )
    return int(chat_id), int(message.id), edit_ts, spoiler_positions


def _ingress_dedup_key(chat_id: int, message: Any, edited: bool) -> tuple:
    return _message_dedup_key(chat_id, message, edited)


def _ingress_age_limit(chat_id: int, cfg: Any) -> float:
    channel = getattr(Config, "CHANNEL_CONFIG", {}).get(int(chat_id), {})
    if "xx88code.com" in str(channel.get("url", "")).lower():
        return max(0.0, float(getattr(cfg, "xx88_max_age_seconds", 40.0)))
    ocr_only = set(getattr(Config, "OCR_MEDIA_ONLY_CHANNEL_IDS", ()))
    if int(chat_id) in ocr_only:
        return max(0.0, float(getattr(cfg, "ocr_max_age_seconds", 45.0)))
    return max(0.0, float(getattr(cfg, "max_age_seconds", 120.0)))


@dataclass
class Envelope:

    chat_id: int
    message: Any
    edited: bool
    source: str
    seen_perf: float
    age_s: float
    # The sink calls this on the event loop after confirming durable storage,
    # including a row already present in the DB (persist result 0).
    on_persisted: Optional[Callable[[], None]] = None


class OfferResult(str, Enum):

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    FILTERED = "filtered"
    INVALID = "invalid"
    SINK_FAILED = "sink_failed"


@dataclass
class ReceiverConfig:
    ingress_fix_v2: bool = False
    poll_interval: float = 1.0
    request_timeout: float = 5.0
    fetch_timeout: float = 8.0
    entity_timeout: float = 10.0
    stall_reconnect_seconds: float = 45.0
    reconnect_cooldown_seconds: float = 120.0
    peers_refresh_seconds: float = 300.0
    stat_interval: float = 60.0
    dedup_size: int = 10000
    backfill_batch_size: int = 100
    backfill_deadline_seconds: float = 300.0
    dialog_batch_size: int = 50
    dialog_batch_retries: int = 3
    dialog_retry_jitter_seconds: float = 1.0
    dedupe_enabled: bool = True
    dedupe_ttl_seconds: float = 240.0
    age_drop_enabled: bool = True
    max_age_seconds: float = 120.0
    xx88_max_age_seconds: float = 40.0
    ocr_max_age_seconds: float = 45.0

    max_adaptive_interval: float = 10.0

    flood_decay_after: int = 60
    # PATCH_MARKER_2026-10-25-v4-poll-ocr
    # Retained for compatibility with callers that still provide these fields.
    flood_decay_factor: float = 0.90


    @classmethod
    def from_config(cls, cfg: Any) -> "ReceiverConfig":
        def f(name: str, default: float) -> float:
            try:
                value = float(getattr(cfg, name, default))
            except (TypeError, ValueError):
                return default
            return value if value > 0 else default

        def i(name: str, default: int) -> int:
            try:
                value = int(getattr(cfg, name, default))
            except (TypeError, ValueError):
                return default
            return max(1, value)

        ingress_fix_v2 = bool(getattr(cfg, "INGRESS_FIX_V2", False))
        # Honor the configured poll floor; FloodWait still slows requests.
        poll_floor = max(1.0, f("POLL_MIN_INTERVAL", 2.0 if ingress_fix_v2 else 10.0))
        return cls(
            ingress_fix_v2=ingress_fix_v2,
            poll_interval=min(15.0, max(poll_floor, f("CHANNEL_POLL_INTERVAL", 12.0))),
            request_timeout=max(2.0, f("CHANNEL_POLL_REQUEST_TIMEOUT", 5.0)),
            fetch_timeout=f("CHANNEL_POLL_FETCH_TIMEOUT", 8.0),
            entity_timeout=max(3.0, f("TELEGRAM_CHANNEL_TIMEOUT", 10.0)),
            stall_reconnect_seconds=f("POLL_STALL_RECONNECT_SECONDS", 45.0),
            reconnect_cooldown_seconds=f("POLL_RECONNECT_COOLDOWN_SECONDS", 120.0),
            backfill_batch_size=i("CHANNEL_BACKFILL_BATCH_SIZE", 100),
            backfill_deadline_seconds=min(300.0, max(1.0, f("CHANNEL_BACKFILL_DEADLINE_SEC", 300.0))),
            dialog_batch_size=min(10, i("TELEGRAM_DIALOG_BATCH_SIZE", 8)),
            dialog_batch_retries=min(3, i("TELEGRAM_DIALOG_BATCH_RETRIES", 3)),
            dialog_retry_jitter_seconds=max(0.0, f("TELEGRAM_DIALOG_RETRY_JITTER_SEC", 1.0)),
            dedupe_enabled=bool(getattr(cfg, "TELEGRAM_INGRESS_DEDUPE_ENABLED", True)),
            dedup_size=min(10000, i("TELEGRAM_INGRESS_DEDUPE_MAX_ENTRIES", 10000)),
            dedupe_ttl_seconds=min(240.0, max(1.0, f("TELEGRAM_INGRESS_DEDUPE_TTL_SEC", 240.0))),
            age_drop_enabled=ingress_fix_v2 and bool(getattr(cfg, "TELEGRAM_INGRESS_AGE_DROP_ENABLED", True)),
            max_age_seconds=max(0.0, f("INGRESS_MAX_AGE_SEC", 120.0)),
            xx88_max_age_seconds=max(0.0, f("INGRESS_XX88_MAX_AGE_SEC", 40.0)),
            ocr_max_age_seconds=max(0.0, f("INGRESS_OCR_MAX_AGE_SEC", 45.0)),
            max_adaptive_interval=max(15.0, f("CHANNEL_POLL_MAX_INTERVAL", 60.0)),
            flood_decay_after=i("CHANNEL_POLL_FLOOD_DECAY_AFTER", 60),
            flood_decay_factor=min(0.99, max(0.50, f("CHANNEL_POLL_FLOOD_DECAY_FACTOR", 0.90))),
        )


class TelethonAdapter:

    def __init__(self, client: Any):
        self.client = client

    def make_request(self, peers: list):
        from telethon.tl.functions.messages import GetPeerDialogsRequest

        return GetPeerDialogsRequest(peers=peers)

    def make_peer(self, entity: Any):
        from telethon.tl.types import InputDialogPeer

        return InputDialogPeer(entity)

    def peer_id(self, peer: Any) -> int:
        from telethon.utils import get_peer_id

        return int(get_peer_id(peer))

    def flood_seconds(self, exc: BaseException) -> Optional[int]:
        try:
            from telethon.errors import FloodWaitError
        except ImportError:
            return None
        if isinstance(exc, FloodWaitError):
            return int(getattr(exc, "seconds", 5) or 5)
        return None

    async def call(self, request):

        client = self.client
        sender = getattr(client, "_sender", None)
        send = getattr(sender, "send", None)
        if send is None:
            return await client(request)
        from telethon import utils
        from telethon.errors import FloodWaitError

        await request.resolve(client, utils)
        # Raw calls expose FloodWait to the receiver instead of sleeping inside
        # Telethon, but must still honor waits learned by concurrent public calls.
        waits = getattr(client, "_flood_waited_requests", {})
        remaining = float(waits.get(request.CONSTRUCTOR_ID, 0)) - time.time()
        if remaining > 0:
            raise FloodWaitError(request, capture=math.ceil(remaining))
        try:
            result = await send(request)
        except FloodWaitError as exc:
            if isinstance(waits, dict):
                waits[request.CONSTRUCTOR_ID] = max(
                    waits.get(request.CONSTRUCTOR_ID, 0), time.time() + exc.seconds,
                )
            raise
        try:
            client.session.process_entities(result)
        except Exception:
            pass
        return result

    def history_flood_seconds(self) -> int:
        from telethon.tl.functions.messages import GetHistoryRequest

        deadline = getattr(self.client, "_flood_waited_requests", {}).get(
            GetHistoryRequest.CONSTRUCTOR_ID, 0,
        )
        return max(0, math.ceil(deadline - time.time()))

    def bind_dialog_message(self, message: Any, response: Any) -> Any:
        """Give a raw dialog message the same bindings as a public history read."""
        from telethon import utils
        entities = {utils.get_peer_id(entity): entity
                    for entity in (*(getattr(response, "users", None) or []),
                                   *(getattr(response, "chats", None) or []))}
        message._finish_init(self.client, entities, None)
        return message


class TelegramReceiver:

    def __init__(
        self,
        client: Any,
        chats: Iterable[int],
        sink: Callable[[Envelope], None],
        *,
        adapter: Any = None,
        accept: Optional[Callable[[Any], bool]] = None,
        accept_backfill: Optional[Callable[[Any], bool]] = None,
        commit_backfill: Optional[Callable[[int, list[Envelope], tuple[int, int]], Any]] = None,
        bootstrap_checkpoint: Optional[Callable[[int, int, int], tuple[int, int]]] = None,
        cfg: Optional[ReceiverConfig] = None,
        log: Optional[logging.Logger] = None,
        clock: Callable[[], float] = time.monotonic,
        push_health: Any = None,
    ):
        self._client = client
        self._push_health = push_health
        self._chats = tuple(int(c) for c in chats)
        self._chat_set = set(self._chats)

        self._history_pause_until = 0.0
        self._peer_by_chat: dict[int, Any] = {}
        self._peer_chat_ids: list[int] = []
        self._sink = sink
        self._adapter = adapter or TelethonAdapter(client)
        self._accept = accept

        self._accept_backfill = accept_backfill
        self._commit_backfill = commit_backfill
        self._bootstrap_checkpoint = bootstrap_checkpoint
        self._cfg = cfg or ReceiverConfig()
        self._cfg.dedup_size = min(10000, max(1, int(self._cfg.dedup_size)))
        self._cfg.dedupe_ttl_seconds = min(240.0, max(1.0, float(self._cfg.dedupe_ttl_seconds)))
        self._log = log or _log
        self._clock = clock

        self._seen: "OrderedDict[tuple, Any]" = OrderedDict()
        # Suppress duplicate deliveries in RAM without claiming durable storage.
        self._pending_ingress: set[tuple] = set()

        self._state: dict[int, tuple[int, int]] = {}
        # One observed version per configured channel. Poll cursors only carry
        # second-resolution dates, so they cannot detect same-second edits.
        self._dialog_versions: dict[int, tuple] = {}
        self._fetch_q: "asyncio.Queue | None" = None
        self._pending_targets: dict[int, tuple[int, int]] = {}
        self._fetch_scheduled: set[int] = set()
        self._fetch_deadlines: dict[int, float] = {}
        self._fetch_retries: dict[int, int] = {}
        self._run_active = False
        self._peers: Optional[list] = None
        self._peers_refresh_at = 0.0
        self._pause_until = 0.0
        self._flood_until = 0.0
        self._poll_semaphore = asyncio.Semaphore(5)
        self._started_at = clock()
        self._last_ok: Optional[float] = None
        self._last_reconnect = float("-inf")
        self._err_streak = 0
        self._last_err_log = float("-inf")
        # Giảm log: gộp các cảnh báo lặp lại (FloodWait, quét lỗi...) thành 1 dòng/phút.
        self._flood_log_at = float("-inf")
        self._flood_log_hits = 0
        self._throttled_logs: dict[str, tuple[float, int]] = {}
        self._quiet_stat_periods = 0

        self._interval = self._cfg.poll_interval
        self._ok_streak = 0
        # Poke: Telegram vừa báo kênh có thay đổi -> quét ngay kênh đó, không chờ nhịp poll.
        self._poke_inflight: set[int] = set()
        self._poke_again: set[int] = set()
        self._poke_last: dict[int, float] = {}
        self._poke_count = 0
        self._reset_stats()

        self.inflight_requests = 0
        self.max_inflight_requests = 0

    def restore_checkpoints(self, checkpoints: dict[int, tuple[int, int]]) -> None:

        for cid, value in checkpoints.items():
            key = int(cid)
            if key in self._chat_set:
                message_id, edit_ts = value
                self._state[key] = (int(message_id), int(edit_ts))

    def _reset_stats(self) -> None:
        self._st = {
            "ok": 0, "timeout": 0, "error": 0, "flood": 0, "lat_sum": 0.0, "lat_max": 0.0,
            "push": 0, "push_seen": 0, "poll": 0, "dup": 0, "rejected": 0,
            "dedupe_dropped": 0, "age_drop": 0,
            "push_late": 0, "push_late_age_sum": 0., "push_late_age_n": 0, "push_late_age_max": 0.,
            "age_sum": 0.0, "age_n": 0, "age_max": 0.0,
        }

    def _log_stats(self) -> None:
        s = self._st
        ok = s["ok"]
        age_avg = s["age_sum"] / s["age_n"] if s["age_n"] else 0.0
        self._log.debug(
            "[Rx-Stat] poll ok=%s timeout=%s error=%s flood=%s interval=%.1fs "
            "push=%s poll=%s duplicates=%s push_seen=%s push_late=%s "
            "age_avg=%.1fs age_max=%.1fs dedupe_dropped=%s age_drop=%s",
            ok, s["timeout"], s["error"], s["flood"], self._interval,
            s["push"], s["poll"], s["dup"], s["push_seen"], s["push_late"],
            age_avg, s["age_max"], s["dedupe_dropped"], s["age_drop"],
        )
        quiet_limit = max(1, int(600 // max(1.0, float(self._cfg.stat_interval))))
        if s["push"] + s["poll"] or s["timeout"] + s["error"]:
            self._quiet_stat_periods = 0
            self._log.info(
                "Telegram %.0fs: push=%s poll=%s errors=%s timeouts=%s floods=%s "
                "RPC_avg=%.0fms message_age_avg=%.1fs max=%.1fs",
                self._cfg.stat_interval, s["push"], s["poll"], s["error"], s["timeout"], s["flood"],
                s["lat_sum"] / ok if ok else 0.0, age_avg, s["age_max"],
            )
        else:
            self._quiet_stat_periods += 1
            if self._quiet_stat_periods >= quiet_limit:
                self._quiet_stat_periods = 0
                self._log.info("Telegram: no new messages from monitored channels for ~%.0f minutes",
                               quiet_limit * self._cfg.stat_interval / 60.0)
        self._reset_stats()

    def _warn_throttled(self, key: str, msg: str, *args: Any, interval: float = 60.0) -> bool:
        """Ghi cảnh báo tối đa 1 lần/`interval` giây cho mỗi `key`; phần bị gộp được đếm."""
        now = self._clock()
        last, hidden = self._throttled_logs.get(key, (float("-inf"), 0))
        if now - last < interval:
            self._throttled_logs[key] = (last, hidden + 1)
            return False
        self._throttled_logs[key] = (now, 0)
        if hidden:
            msg = f"{msg} (+{hidden} lần tương tự đã được gộp)"
        self._log.warning(msg, *args)
        return True


    def _mark_persisted(self, key: tuple) -> None:
        """Called on the event loop only after the sink confirms a DB commit."""
        self._pending_ingress.discard(key)
        if self._cfg.ingress_fix_v2 and self._cfg.dedupe_enabled:
            self._seen[key] = self._clock()
            self._seen.move_to_end(key)
            self._prune_seen()
        elif not self._cfg.ingress_fix_v2:
            self._seen[key] = None
            self._seen.move_to_end(key)
            while len(self._seen) > self._cfg.dedup_size:
                self._seen.popitem(last=False)

    def _prune_seen(self) -> None:
        cutoff = self._clock() - self._cfg.dedupe_ttl_seconds
        while self._seen:
            key, seen_at = next(iter(self._seen.items()))
            if seen_at > cutoff:
                break
            self._seen.pop(key, None)
        while len(self._seen) > self._cfg.dedup_size:
            self._seen.popitem(last=False)

    def _message_age(self, message: Any) -> float:
        message_date = getattr(message, "date", None)
        if message_date is None:
            return -1.0
        if message_date.tzinfo is None:
            message_date = message_date.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - message_date).total_seconds())

    def _drop_if_old(self, cid: int, message: Any, age: float) -> bool:
        if (not self._cfg.ingress_fix_v2 or not self._cfg.age_drop_enabled
                or age < 0):
            return False
        threshold = _ingress_age_limit(cid, self._cfg)
        if age <= threshold:
            return False
        self._st["age_drop"] += 1
        self._log.info(
            "INGRESS_DROP_OLD chat=%s msg=%s age=%.1f",
            cid, getattr(message, "id", "?"), age,
        )
        return True

    async def _offer_detailed(
        self, chat_id: Any, message: Any, *, edited: bool, source: str
    ) -> OfferResult:

        try:
            cid = int(chat_id)
        except (TypeError, ValueError):
            return OfferResult.INVALID
        if cid not in self._chat_set or message is None:
            return OfferResult.INVALID
        mid = getattr(message, "id", None)
        if mid is None:
            return OfferResult.INVALID
        try:
            if self._accept is not None and not self._accept(message):
                self._st["rejected"] += 1
                return OfferResult.FILTERED
        except Exception as exc:
            self._log.error("❌ [Rx] accept lỗi chat=%s message=%s: %r", cid, mid, exc)
            return OfferResult.SINK_FAILED

        key = (
            _ingress_dedup_key(cid, message, edited)
            if self._cfg.ingress_fix_v2 and self._cfg.dedupe_enabled
            else _message_dedup_key(cid, message, edited)
        )
        age = self._message_age(message)
        if self._cfg.dedupe_enabled:
            if self._cfg.ingress_fix_v2:
                self._prune_seen()
            if key in self._seen or key in self._pending_ingress:
                self._st["dup"] += 1
                self._st["dedupe_dropped"] += 1
                return OfferResult.DUPLICATE
        if self._drop_if_old(cid, message, age):
            if self._cfg.ingress_fix_v2 and self._cfg.dedupe_enabled:
                self._seen[key] = self._clock()
                self._prune_seen()
            return OfferResult.FILTERED

        env = Envelope(
            cid, message, bool(edited), source,
            (getattr(self, "_raw_offer_seen_perf", None) or time.perf_counter()) if source == "push" else time.perf_counter(), age,
            on_persisted=lambda: self._mark_persisted(key),
        )
        if self._cfg.dedupe_enabled:
            self._pending_ingress.add(key)
        try:
            result = self._sink(env)
            if inspect.isawaitable(result):
                await result
        except asyncio.QueueFull:
            self._pending_ingress.discard(key)
            self._st["rejected"] += 1
            return OfferResult.SINK_FAILED
        except Exception as exc:
            self._pending_ingress.discard(key)
            self._log.error("❌ [Rx] sink lỗi chat=%s message=%s: %r", cid, mid, exc)
            return OfferResult.SINK_FAILED

        metric_source = "poll" if source == "backfill" else source
        self._st[metric_source if metric_source in ("push", "poll") else "push"] += 1
        if age >= 0:
            self._st["age_sum"] += age
            self._st["age_n"] += 1
            self._st["age_max"] = max(self._st["age_max"], age)
        if self._push_health is not None and source in ("poll", "backfill"):
            self._push_health.note_poll(age)
        if source == "poll":
            self._log.info(
                "📡 [%s] chat=%s message_id=%s edited=%s tuổi=%.1fs",
                "Poll", cid, mid, edited, age,
            )
        return OfferResult.ACCEPTED

    async def on_push(self, event: Any, *, edited: bool = False) -> None:

        started = time.perf_counter()
        try:
            # Recovery only delays outgoing RPCs, never received messages.
            result = await self._offer_detailed(
                getattr(event, "chat_id", None), getattr(event, "message", None),
                edited=edited, source="push",
            )
            # A late duplicate proves connectivity, but not timely delivery.
            if getattr(event, "chat_id", None) in self._chat_set:
                self._st["push_seen"] += 1
                if self._push_health is not None and result in (
                    OfferResult.ACCEPTED, OfferResult.DUPLICATE
                ):
                    message = getattr(event, "message", None)
                    date = (getattr(message, "edit_date", None) if edited else None) or getattr(message, "date", None)
                    age = None
                    if date is not None:
                        if date.tzinfo is None:
                            date = date.replace(tzinfo=timezone.utc)
                        age = max(0.0, (datetime.now(timezone.utc) - date).total_seconds())
                    self._push_health.note_push(age, first_delivery=result == OfferResult.ACCEPTED)
                    if result == OfferResult.DUPLICATE:
                        self._st["push_late"] += 1
                        if age is not None:
                            self._st["push_late_age_sum"] += age
                            self._st["push_late_age_n"] += 1
                            self._st["push_late_age_max"] = max(self._st["push_late_age_max"], age)
            if result == OfferResult.ACCEPTED:
                self._log.info(
                    "[Ingress] push ok latency_ms=%.2f chat=%s message=%s",
                    (time.perf_counter() - started) * 1000.0,
                    getattr(event, "chat_id", None),
                    getattr(getattr(event, "message", None), "id", "?"),
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._log.error("❌ [Rx] push handler lỗi: %r", exc)

    async def run(self) -> None:
        if self._cfg.ingress_fix_v2 and self._run_active:
            self._log.warning("⚠️ [Rx] poll loop already running; refusing duplicate runner")
            return
        if self._cfg.ingress_fix_v2:
            self._run_active = True
        self._fetch_q = asyncio.Queue(maxsize=max(1, len(self._chats)))

        self._fetch_scheduled.clear()

        for cid in tuple(self._pending_targets):
            self._fetch_deadlines[cid] = (
                self._clock() + max(1.0, float(self._cfg.backfill_deadline_seconds))
            )
            self._fetch_retries[cid] = 0
            self._schedule_fetch(cid)
        fetcher = asyncio.create_task(self._fetch_loop(), name="rx-fetch")
        push_health_task = (asyncio.create_task(self._push_health.run(), name="rx-push-health")
                            if self._push_health is not None else None)
        try:
            await self._poll_loop()
        finally:
            tasks = [t for t in (fetcher, push_health_task) if t is not None]
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self._cfg.ingress_fix_v2:
                self._run_active = False

    def poke(self, chat_id: Any, *, reason: str = "raw") -> bool:
        """Quét NGAY một kênh khi Telegram báo có thay đổi (vd. UpdateChannelTooLong).

        Telegram không phải lúc nào cũng đẩy nguyên tin của kênh lớn; thay vào đó nó
        chỉ báo "kênh có tin mới". Thay vì chờ tới nhịp poll kế tiếp (2-3s), ta hỏi
        đúng kênh đó ngay (1 request GetPeerDialogs). Gộp các lần báo dồn dập:
        mỗi kênh tối đa 1 request đang bay, báo thêm trong lúc đó thì chạy lại 1 lần.
        Trả về True nếu đã lên lịch quét.
        """
        try:
            cid = int(chat_id)
        except (TypeError, ValueError):
            return False
        peer = self._peer_by_chat.get(cid)
        if peer is None or cid not in self._chat_set:
            return False
        now = self._clock()
        if now < self._pause_until or not self._client.is_connected():
            return False
        if cid in self._poke_inflight:
            self._poke_again.add(cid)
            return True
        self._poke_inflight.add(cid)
        self._poke_count += 1
        if self._poke_count <= 5 or self._poke_count % 200 == 0:
            self._log.info("⚡ [Poke] chat=%s lý do=%s (lần %s)", cid, reason, self._poke_count)
        asyncio.create_task(self._poke_run(cid, peer), name=f"rx-poke-{cid}")
        return True

    async def _poke_run(self, cid: int, peer: Any) -> None:
        try:
            while True:
                self._poke_again.discard(cid)
                async with self._poll_semaphore:
                    if self._clock() < self._pause_until:
                        return
                    await self._poll_once([peer], chat_ids=[cid])
                self._poke_last[cid] = self._clock()
                if cid not in self._poke_again:
                    return
                # Có báo thêm khi đang quét: chạy lại đúng 1 lượt, cách nhau >= 0.2s.
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._log.debug("[Poke] chat=%s lỗi: %r", cid, exc)
        finally:
            self._poke_inflight.discard(cid)
            self._poke_again.discard(cid)


    def _history_wait_seconds(self) -> float:
        cached_wait = getattr(self._adapter, "history_flood_seconds", None)
        now = self._clock()
        if cached_wait is not None:
            self._history_pause_until = max(self._history_pause_until, now + cached_wait())
        return max(0.0, self._history_pause_until - now)


    async def _poll_loop(self) -> None:
        cfg = self._cfg
        last_stat = self._clock()
        err_backoff = 0.0
        while True:
            now = self._clock()
            if now - last_stat >= cfg.stat_interval:
                self._log_stats()
                last_stat = now

            if not self._client.is_connected():
                await asyncio.sleep(0.5)
                continue

            if now < self._pause_until:
                await asyncio.sleep(min(1.0, self._pause_until - now))
                continue
            if (cfg.ingress_fix_v2
                    and not bool(getattr(Config, "TELEGRAM_POLL_FALLBACK_ENABLED", True))):
                await asyncio.sleep(1.0)
                continue


            if self._peers is None or (
                self._peers_refresh_at and now >= self._peers_refresh_at
            ):
                resolved = await self._resolve_peers()
                if not resolved:
                    self._log.error("❌ [Rx] không resolve được kênh nào — thử lại sau 5s")
                    await asyncio.sleep(5.0)
                    continue
                self._peers = resolved

            started = self._clock()
            peers = self._peers or []
            peer_to_chat = {id(peer): cid for cid, peer in self._peer_by_chat.items()}
            peer_chat_ids = [peer_to_chat.get(id(peer), 0) for peer in peers]
            batch_size = max(1, min(10, self._cfg.dialog_batch_size))
            batches = [
                (peers[offset:offset + batch_size], peer_chat_ids[offset:offset + batch_size])
                for offset in range(0, len(peers), batch_size)
            ]
            outcomes = await asyncio.gather(*(
                self._poll_batch(peer_batch, chat_batch)
                for peer_batch, chat_batch in batches
            ))
            elapsed = self._clock() - started
            flood = "flood" in outcomes
            failed_batches = sum(outcome not in ("ok", "flood") for outcome in outcomes)
            if flood:
                scanned = sum(
                    len(batch[0]) for batch, outcome in zip(batches, outcomes)
                    if outcome != "flood"
                )
                # Đã có 1 dòng gộp về FloodWait ở _on_flood; dòng này chỉ để gỡ lỗi.
                self._log.debug(
                    "[Poll-Cycle] Scanned %s/%s channels in %.2fs; pausing for FloodWait",
                    scanned, len(peers), elapsed,
                )
                await asyncio.sleep(max(0.0, self._pause_until - self._clock()))
                continue

            # Mỗi vài giây một vòng quét bình thường -> không ghi vào log thường.
            self._log.debug(
                "[Poll-Cycle] Scanned %s channels in %.2fs (batches=%s failed=%s)",
                len(peers), elapsed, len(batches), failed_batches,
            )
            if not failed_batches:
                err_backoff = 0.0
                await asyncio.sleep(max(0.0, self._interval - (self._clock() - started)))
            else:
                err_backoff = min(5.0, max(0.5, err_backoff * 2 or 0.5))
                await asyncio.sleep(err_backoff)

    async def _poll_batch(self, peers: list, chat_ids: list[int]) -> str:
        retries = max(
            1,
            min(3, self._cfg.dialog_batch_retries) if self._cfg.ingress_fix_v2 else 1,
        )
        async with self._poll_semaphore:
            for attempt in range(retries):
                if self._clock() < self._pause_until:
                    return "flood"
                try:
                    outcome = await self._poll_once(peers, chat_ids=chat_ids)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._log.debug("[Poll-Cycle] batch failed chats=%s", chat_ids)
                    self._warn_throttled(
                        "poll_batch_failed",
                        "⚠️ [Rx] Quét một nhóm kênh bị lỗi (%s) — bot sẽ thử lại",
                        type(exc).__name__,
                    )
                    outcome = "error"
                if outcome in ("ok", "flood"):
                    return outcome
                if attempt + 1 < retries:
                    await asyncio.sleep(random.uniform(0.2, 0.8))
            if peers:
                self._log.debug("chunk_failed chat_ids=%s", chat_ids)
                self._warn_throttled(
                    "chunk_failed",
                    "⚠️ [Rx] Quét %s kênh chưa thành công (%s) — sẽ thử lại ở vòng sau",
                    len(chat_ids), outcome,
                )
            return outcome

    def _hold_push_health(self, secs: float) -> None:
        """Trong lúc đang bị FloodWait, đừng để Push-Health gọi thêm catch_up()."""
        hold = getattr(self._push_health, "hold", None)
        if hold is not None:
            try:
                hold(float(secs))
            except Exception:
                pass

    async def _resolve_peers(self, chats=None) -> list:
        out: list = []
        resolved_chat_ids: list[int] = []
        missing = 0
        for cid in (self._chats if chats is None else chats):
            try:
                ent = await asyncio.wait_for(
                    self._client.get_input_entity(cid), timeout=self._cfg.entity_timeout
                )
                peer = self._adapter.make_peer(ent)
                out.append(peer)
                resolved_chat_ids.append(int(cid))
                if chats is None:
                    # Chỉ ghi nhận peers của lần resolve chính: _select_poll_peers
                    # so khớp theo danh tính object trong self._peers.
                    self._peer_by_chat[int(cid)] = peer
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                missing += 1
                self._log.warning("⚠️ [Rx] không resolve được chat=%s: %s", cid, exc)
        if chats is None:
            self._peer_chat_ids = resolved_chat_ids
            self._peers_refresh_at = (
                self._clock() + self._cfg.peers_refresh_seconds if missing else 0.0
            )
        return out

    async def _poll_once(self, peers: list, *, chat_ids: list[int] | None = None) -> str:
        cfg = self._cfg
        t0 = self._clock()
        self.inflight_requests += 1
        self.max_inflight_requests = max(self.max_inflight_requests, self.inflight_requests)
        try:
            request = self._adapter.make_request(peers)
            call = getattr(self._adapter, "call", None)
            resp = await asyncio.wait_for(
                call(request) if call is not None else self._client(request),
                timeout=cfg.request_timeout,
            )
        except asyncio.CancelledError:
            raise
        except RpcCallFailError as exc:
            self._st["error"] += 1
            self._log.warning(
                "⚠️ [Rx] GetPeerDialogs RpcCallFailError: %r", exc,
            )
            return "rpc_call_fail"
        except asyncio.TimeoutError:
            self._st["timeout"] += 1
            if not cfg.ingress_fix_v2:
                await self._maybe_reconnect()
            return "timeout"
        except Exception as exc:
            secs = self._adapter.flood_seconds(exc)
            if secs:
                self._on_flood(secs, t0, chat_ids=chat_ids)
                return "flood"
            self._st["error"] += 1
            self._err_streak += 1
            now = self._clock()
            if now - self._last_err_log >= 30.0:
                self._last_err_log = now
                self._log.warning(
                    "⚠️ [Rx] poll lỗi (%s lần liên tiếp): %s: %r",
                    self._err_streak, type(exc).__name__, exc,
                )
            if self._err_streak >= 3:
                self._peers = None
            if not cfg.ingress_fix_v2:
                await self._maybe_reconnect()
            return "error"
        finally:
            self.inflight_requests -= 1

        lat_ms = (self._clock() - t0) * 1000.0
        self._last_ok = self._clock()
        self._err_streak = 0
        self._last_ok_start = t0
        self._st["ok"] += 1
        self._note_ok_for_interval()
        self._st["lat_sum"] += lat_ms
        self._st["lat_max"] = max(self._st["lat_max"], lat_ms)
        try:
            await self._apply(resp)
        except Exception as exc:
            self._log.warning("⚠️ [Rx] xử lý phản hồi lỗi: %s: %r", type(exc).__name__, exc)
        return "ok"

    def _on_flood(
        self, secs: int, t0: float, *, chat_ids: list[int] | None = None
    ) -> None:
        if not self._cfg.ingress_fix_v2:
            cfg = self._cfg
            now = self._clock()
            self._pause_until = max(self._pause_until, now + secs)
            self._hold_push_health(secs)
            self._last_ok = now
            self._ok_streak = 0
            self._st["flood"] += 1
            raw = max(1, secs - 1)
            cap = max(cfg.poll_interval, cfg.max_adaptive_interval)
            if self._last_ok_start is None:
                new_interval = self._interval
            elif raw <= cfg.max_adaptive_interval:
                learned = max(0.0, t0 - self._last_ok_start) + raw + 0.2
                new_interval = min(cap, max(self._interval, learned))
            else:
                new_interval = min(cap, max(cfg.poll_interval, self._interval * 1.5))
            if new_interval > self._interval + 0.05 or now - self._last_flood_log >= 30.0:
                self._last_flood_log = now
                self._log.warning(
                    "⏳ [Rx] FloodWait %ss từ Telegram — nghỉ poll, nhịp poll %.1fs -> %.1fs",
                    secs, self._interval, new_interval,
                )
            self._interval = new_interval
            self._learned_floor = new_interval
            self._floor_probe_at = now
            return
        now = self._clock()
        wait = max(0, int(secs)) + 1
        new_flood = now >= self._flood_until  # nhiều nhóm kênh cùng báo flood = 1 sự kiện
        self._flood_until = max(self._flood_until, now + wait)
        self._pause_until = self._flood_until
        self._hold_push_health(secs)
        self._last_ok = now
        self._st["flood"] += 1
        self._flood_log_hits += 1
        # Nhịp poll có thể thấp (2-4s): sau FloodWait thì giãn dần, ổn định lại thì hạ.
        self._ok_streak = 0
        if new_flood:
            self._interval = min(
                max(self._cfg.poll_interval, self._cfg.max_adaptive_interval),
                max(self._cfg.poll_interval + 1.0, self._interval * 1.5),
            )
        # Chi tiết từng lần (kèm danh sách kênh) chỉ ghi ở mức DEBUG.
        self._log.debug(
            "[Rx] FloodWait %ss; next poll after %.1fs (poll_mode=push-first/poll-fallback "
            "chunk_chats=%s)",
            secs, wait, chat_ids or [],
        )
        # Telegram thường báo FloodWait cho từng nhóm kênh cùng lúc -> chỉ ghi 1 dòng/phút.
        if now - self._flood_log_at >= 60.0:
            hits = self._flood_log_hits
            self._flood_log_at = now
            self._flood_log_hits = 0
            self._log.info(
                "⏳ Telegram yêu cầu bot chờ %ss (FloodWait) rồi mới quét tiếp — bình thường, "
                "bot tự xử lý | %s lần gần đây",
                secs, hits,
            )

    def _note_ok_for_interval(self) -> None:
        cfg = self._cfg
        self._ok_streak += 1
        if self._interval > cfg.poll_interval and self._ok_streak >= cfg.flood_decay_after:
            self._ok_streak = 0
            self._interval = cfg.poll_interval
            self._learned_floor = cfg.poll_interval

    async def _maybe_reconnect(self) -> None:
        cfg = self._cfg
        now = self._clock()
        since_ok = now - (self._last_ok if self._last_ok is not None else self._started_at)
        if since_ok < cfg.stall_reconnect_seconds:
            return
        if now - self._last_reconnect < cfg.reconnect_cooldown_seconds:
            return
        self._last_reconnect = now
        self._peers = None
        self._log.warning(
            "🔌 [Rx] %.0fs không có phản hồi OK — đóng connection để watchdog reconnect", since_ok
        )
        try:
            await asyncio.wait_for(self._client.disconnect(), timeout=5.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._log.warning("⚠️ [Rx] disconnect lỗi: %s: %r", type(exc).__name__, exc)

    async def _initial_checkpoint(self, cid: int, top: int, edit_ts: int) -> tuple[int, int]:
        if self._bootstrap_checkpoint is None:
            checkpoint = (max(0, top - 1), 0) if top > 0 else (top, edit_ts)
        else:
            # The durable inbox can wait for a SQLite lock or another worker.
            # Never do that on the loop that also handles Telegram and submit.
            initial_id = max(0, top - 1) if top > 0 else top
            checkpoint = await asyncio.to_thread(self._bootstrap_checkpoint, cid, initial_id, 0)
            if inspect.isawaitable(checkpoint):
                checkpoint = await checkpoint
        # Hot and regular polls may initialize the same chat concurrently.
        # Preserve progress made by the other task while storage was awaited.
        return self._state.setdefault(cid, checkpoint)

    async def _apply(self, resp: Any, *, source: str = "poll") -> None:

        ad = self._adapter
        raw_msgs: dict = {}
        for rm in getattr(resp, "messages", None) or []:
            peer = getattr(rm, "peer_id", None)
            if peer is not None:
                raw_msgs[(ad.peer_id(peer), int(rm.id))] = rm

        for dialog in getattr(resp, "dialogs", None) or []:
            cid = ad.peer_id(dialog.peer)
            if cid not in self._chat_set:
                continue
            top = int(getattr(dialog, "top_message", 0) or 0)
            if top <= 0:
                continue

            raw = raw_msgs.get((cid, top))
            edit_dt = getattr(raw, "edit_date", None) if raw is not None else None
            edit_ts = int(edit_dt.timestamp()) if edit_dt else 0
            previous_version = self._dialog_versions.get(cid)
            version = _message_dedup_key(cid, raw, bool(edit_dt)) if raw is not None else None
            if version is not None:
                self._dialog_versions[cid] = version
            changed_content = version is not None and (
                previous_version is None or version != previous_version
            )
            checkpoint = self._state.get(cid)
            if checkpoint is None:
                checkpoint = await self._initial_checkpoint(cid, top, edit_ts)
                if top > checkpoint[0] or (top == checkpoint[0] and edit_ts > checkpoint[1]):
                    await self._offer_dialog_top(cid, raw, resp, checkpoint, source)
                    self._request_backfill(cid, (top, edit_ts))
                elif (previous_version is not None and changed_content
                      and (top, edit_ts) == checkpoint):
                    await self._offer_dialog_top(cid, raw, resp, checkpoint, source)
                continue
            if top < checkpoint[0] or (top == checkpoint[0] and edit_ts < checkpoint[1]):
                continue
            if top > checkpoint[0] or (top == checkpoint[0] and edit_ts > checkpoint[1]):
                await self._offer_dialog_top(cid, raw, resp, checkpoint, source)
                self._request_backfill(cid, (top, edit_ts))
            elif changed_content:
                # The full top message is already in this dialog response.
                # Offer its changed content without a redundant history RPC or
                # advancing a cursor that cannot represent the new version.
                await self._offer_dialog_top(cid, raw, resp, checkpoint, source)

    async def _offer_dialog_top(self, cid, message, response, checkpoint, source):
        # The dialog response already includes text, spoiler entities and media.
        # Deliver through durable ingress now; only full backfill may advance
        # the checkpoint, so intervening messages remain recoverable.
        if message is None:
            return
        try:
            bind = getattr(self._adapter, "bind_dialog_message", None)
            if bind is not None:
                message = bind(message, response)
            result = await self._offer_detailed(
                cid, message,
                edited=bool(getattr(message, "edit_date", None)),
                source=source,
            )
            if result == OfferResult.SINK_FAILED:
                # Retry an unchanged same-second version after ingress recovers.
                self._dialog_versions.pop(cid, None)
        except Exception as exc:
            self._dialog_versions.pop(cid, None)
            self._log.warning("[Rx] dialog top binding failed chat=%s: %s; backfill retained", cid, type(exc).__name__)

    def _request_backfill(self, cid: int, target: tuple[int, int]) -> None:

        previous = self._pending_targets.get(cid)
        if previous is None:
            self._pending_targets[cid] = target
            self._fetch_deadlines[cid] = (
                self._clock() + max(1.0, float(self._cfg.backfill_deadline_seconds))
            )
            self._fetch_retries[cid] = 0
        elif target > previous:
            self._pending_targets[cid] = target
            self._fetch_deadlines[cid] = (
                self._clock() + max(1.0, float(self._cfg.backfill_deadline_seconds))
            )
            self._fetch_retries[cid] = 0
        self._schedule_fetch(cid)

    def _schedule_fetch(self, cid: int) -> None:
        if self._fetch_q is None or cid in self._fetch_scheduled:
            return
        self._fetch_scheduled.add(cid)
        self._fetch_q.put_nowait(cid)

    async def _fetch_loop(self) -> None:

        assert self._fetch_q is not None
        while True:
            cid = await self._fetch_q.get()
            completed = False
            try:
                completed = await self._process_fetch_job(cid)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.warning(
                    "⚠️ [Rx] backfill lỗi chat=%s: %s: %r — checkpoint giữ nguyên",
                    cid, type(exc).__name__, exc,
                )
                self._fetch_scheduled.discard(cid)
            if completed:
                self._fetch_deadlines.pop(cid, None)
                self._fetch_retries.pop(cid, None)
                continue
            deadline = self._fetch_deadlines.get(cid, 0.0)
            retries = self._fetch_retries.get(cid, 0)
            if retries < 3 and self._clock() < deadline:
                self._fetch_retries[cid] = retries + 1
                await asyncio.sleep(min(1.0, 0.2 * (2 ** retries)))
                self._schedule_fetch(cid)
            else:
                self._log.warning(
                    "[Rx] backfill gave up cid=%s after %s attempts; checkpoint retained",
                    cid, retries + 1,
                )
                self._pending_targets.pop(cid, None)
                self._fetch_deadlines.pop(cid, None)
                self._fetch_retries.pop(cid, None)

    async def _process_fetch_job(self, cid: int) -> bool:

        try:
            while cid in self._pending_targets:
                target = self._pending_targets[cid]
                checkpoint = self._state.get(cid)
                if checkpoint is None:
                    checkpoint = await self._initial_checkpoint(cid, 0, 0)
                    if target <= checkpoint:
                        self._pending_targets.pop(cid, None)
                        return True

                if target <= checkpoint:
                    self._pending_targets.pop(cid, None)
                    return True

                if not await self._drain_to_target(
                    cid, checkpoint, target,
                    deadline=self._fetch_deadlines.get(cid),
                ):
                    return False

                self._state[cid] = target
                self._log.info(
                    "📥 [Rx] backfill hoàn tất chat=%s checkpoint=%s target=%s",
                    cid, checkpoint[0], target[0],
                )
                if self._pending_targets.get(cid, target) <= target:
                    self._pending_targets.pop(cid, None)
                    return True

            return True
        finally:

            self._fetch_scheduled.discard(cid)

    def _prepare_backfill_envelope(
        self, cid: int, message: Any, *, edited: bool, local_keys: set[tuple]
    ) -> tuple[OfferResult, Optional[Envelope], Optional[tuple]]:

        if message is None or int(cid) not in self._chat_set:
            return OfferResult.INVALID, None, None
        mid = getattr(message, "id", None)
        if mid is None:
            return OfferResult.INVALID, None, None
        validator = self._accept_backfill or self._accept
        try:
            if validator is not None and not validator(message):
                self._st["rejected"] += 1
                return OfferResult.FILTERED, None, None
        except Exception as exc:
            self._log.error("❌ [Rx] accept_backfill lỗi chat=%s message=%s: %r", cid, mid, exc)
            return OfferResult.SINK_FAILED, None, None

        key = (
            _ingress_dedup_key(cid, message, edited)
            if self._cfg.ingress_fix_v2 and self._cfg.dedupe_enabled
            else _message_dedup_key(cid, message, edited)
        )
        age = self._message_age(message)
        if self._cfg.dedupe_enabled:
            if self._cfg.ingress_fix_v2:
                self._prune_seen()
            if key in local_keys or key in self._seen or key in self._pending_ingress:
                self._st["dup"] += 1
                self._st["dedupe_dropped"] += 1
                return OfferResult.DUPLICATE, None, key
        if self._drop_if_old(int(cid), message, age):
            if self._cfg.ingress_fix_v2 and self._cfg.dedupe_enabled:
                local_keys.add(key)
                self._seen[key] = self._clock()
                self._prune_seen()
            return OfferResult.FILTERED, None, key
        if self._cfg.dedupe_enabled and key in local_keys:
            self._st["dup"] += 1
            self._st["dedupe_dropped"] += 1
            return OfferResult.DUPLICATE, None, key
        local_keys.add(key)

        env = Envelope(int(cid), message, bool(edited), "backfill", time.perf_counter(), age)
        return OfferResult.ACCEPTED, env, key

    def _mark_backfill_committed(self, items: list[tuple[Envelope, tuple]]) -> None:

        for env, key in items:
            self._pending_ingress.discard(key)
            if key in self._seen:
                self._st["dup"] += 1
                self._st["dedupe_dropped"] += 1
            else:
                if self._cfg.dedupe_enabled:
                    self._seen[key] = (
                        self._clock() if self._cfg.ingress_fix_v2 else None
                    )
                    self._seen.move_to_end(key)
                self._st["poll"] += 1
                if env.age_s >= 0:
                    self._st["age_sum"] += env.age_s
                    self._st["age_n"] += 1
                    self._st["age_max"] = max(self._st["age_max"], env.age_s)
            if self._cfg.ingress_fix_v2:
                self._prune_seen()
            else:
                while len(self._seen) > self._cfg.dedup_size:
                    self._seen.popitem(last=False)

    async def _commit_backfill_batch(
        self, cid: int, items: list[tuple[Envelope, tuple]], checkpoint: tuple[int, int]
    ) -> bool:
        envelopes = [env for env, _key in items]
        if self._commit_backfill is not None:
            try:
                committed = self._commit_backfill(cid, envelopes, checkpoint)
                if inspect.isawaitable(committed):
                    committed = await committed
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.error(
                    "❌ [Rx] commit backfill lỗi chat=%s cursor=%s: %r", cid, checkpoint, exc
                )
                return False
            if not committed:
                return False
            self._mark_backfill_committed(items)
            self._state[cid] = checkpoint
            return True

        for env in envelopes:
            result = await self._offer_detailed(
                cid, env.message, edited=env.edited, source="backfill"
            )
            if result == OfferResult.SINK_FAILED:
                return False
        self._state[cid] = checkpoint
        return True

    async def _drain_to_target(
        self,
        cid: int,
        checkpoint: tuple[int, int],
        target: tuple[int, int],
        *,
        deadline: float | None = None,
    ) -> bool:

        checkpoint_id, _checkpoint_edit_ts = checkpoint
        target_id, target_edit_ts = target
        timeout = max(0.1, float(self._cfg.fetch_timeout))
        local_deadline = self._clock() + max(
            1.0, float(self._cfg.backfill_deadline_seconds)
        )
        if deadline is not None:
            local_deadline = min(local_deadline, deadline)
        if target_id != checkpoint_id and self._history_wait_seconds() > 0:
            return False  # Keep checkpoint; dialog polling and push continue.

        try:
            if target_id == checkpoint_id:
                remaining = local_deadline - self._clock()
                if remaining <= 0:
                    return False
                result = await asyncio.wait_for(
                    self._client.get_messages(cid, ids=[target_id]),
                    timeout=min(timeout, remaining),
                )
                if isinstance(result, (list, tuple)):
                    messages = [message for message in result if message]
                else:
                    messages = [result] if result else []
                items: list[tuple[Envelope, tuple]] = []
                keys: set[tuple] = set()
                for message in messages:
                    disposition, env, key = self._prepare_backfill_envelope(
                        cid, message, edited=True, local_keys=keys
                    )
                    if disposition == OfferResult.SINK_FAILED:
                        return False
                    if env is not None and key is not None:
                        items.append((env, key))

                return await self._commit_backfill_batch(cid, items, target)

            iterator = self._client.iter_messages(
                cid,
                limit=max(1, target_id - checkpoint_id),
                reverse=True,
                min_id=checkpoint_id,
                max_id=target_id + 1,
                wait_time=0,
            )
            cursor = checkpoint_id
            last_seen_id = checkpoint_id
            scanned_in_batch = 0
            batch: list[tuple[Envelope, tuple]] = []
            local_keys: set[tuple] = set()
            batch_size = min(200, max(1, int(self._cfg.backfill_batch_size)))
            try:
                while True:
                    remaining = local_deadline - self._clock()
                    if remaining <= 0:
                        self._log.warning(
                            "[Rx] backfill gave up cid=%s deadline exceeded; checkpoint retained",
                            cid,
                        )
                        return False
                    try:
                        message = await asyncio.wait_for(
                            iterator.__anext__(), timeout=min(timeout, remaining)
                        )
                    except StopAsyncIteration:
                        break

                    message_id = int(getattr(message, "id", 0) or 0)
                    if message_id <= checkpoint_id:
                        continue
                    if message_id > target_id:
                        raise RuntimeError(
                            f"history iterator crossed target: id={message_id} target={target_id}"
                        )
                    if message_id < last_seen_id:
                        raise RuntimeError(
                            f"history iterator is not ascending: previous={last_seen_id} id={message_id}"
                        )
                    if message_id == last_seen_id:
                        continue

                    disposition, env, key = self._prepare_backfill_envelope(
                        cid, message, edited=False, local_keys=local_keys
                    )
                    if disposition == OfferResult.SINK_FAILED:
                        return False
                    if env is not None and key is not None:
                        batch.append((env, key))
                    cursor = message_id
                    last_seen_id = message_id
                    scanned_in_batch += 1

                    if scanned_in_batch >= batch_size:

                        if not await self._commit_backfill_batch(
                            cid, batch, (cursor, 0)
                        ):
                            return False
                        batch = []
                        local_keys = set()
                        scanned_in_batch = 0

                return await self._commit_backfill_batch(
                    cid, batch, (target_id, target_edit_ts)
                )
            finally:
                close = getattr(iterator, "aclose", None)
                if close is not None:
                    try:
                        await close()
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            flood_seconds = self._adapter.flood_seconds(exc)
            if not flood_seconds and target_id != checkpoint_id:
                flood_seconds = math.ceil(self._history_wait_seconds())
            if flood_seconds:
                self._history_pause_until = max(
                    self._history_pause_until, self._clock() + max(1, flood_seconds)
                )
                self._log.warning(
                    "⚠️ [Rx] FloodWait %ss khi backfill chat=%s; giữ checkpoint",
                    flood_seconds, cid,
                )
            else:
                self._log.warning(
                    "⚠️ [Rx] backfill lỗi chat=%s checkpoint=%s target=%s: %s: %r",
                    cid, checkpoint, target, type(exc).__name__, exc,
                )
            return False


class IngressStage:

    def __init__(
        self,
        *,
        build_row: Callable[[Envelope], tuple],
        persist: Callable[[list], list],
        on_row: Callable[[Envelope, int], Any],
        persist_checkpointed: Optional[Callable[[list, int, int, int], Any]] = None,
        executor: Any = None,
        max_batch: int = 64,
        maxsize: int = 4096,
        retries: int = 3,
        log: Optional[logging.Logger] = None,
    ):
        self._build_row = build_row
        self._persist = persist
        self._persist_checkpointed = persist_checkpointed
        self._on_row = on_row
        self._executor = executor
        self.maxsize = min(1000, max(1, int(maxsize)))
        self._max_batch = min(200, self.maxsize, max(1, int(max_batch)))
        self._retries = min(3, max(1, int(retries)))
        self._log = log or _log
        self._q: asyncio.Queue[Envelope] = asyncio.Queue(maxsize=self.maxsize)
        self._slots = asyncio.Semaphore(self.maxsize)
        self._inflight_count = 0
        self.rejected = 0
        self.batches = 0
        self.rows_written = 0
        self.duplicates = 0
        self.lost = 0
        self.blocked_puts = 0

    async def put(self, env: Envelope) -> None:
        if self._slots.locked():
            self.blocked_puts += 1
        await self._slots.acquire()
        try:
            self._q.put_nowait(env)
        except BaseException:
            self._slots.release()
            raise

    @property
    def pending(self) -> int:
        return self._q.qsize() + self._inflight_count

    async def run(self) -> None:
        while True:
            batch = [await self._q.get()]
            while len(batch) < self._max_batch:
                try:
                    batch.append(self._q.get_nowait())
                except asyncio.QueueEmpty:
                    break
            self._inflight_count += len(batch)
            retry_later = False
            try:
                retry_later = not await self._flush(batch)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.error("❌ [Ingress] flush lỗi: %r", exc)
                retry_later = True
            if retry_later:
                await asyncio.sleep(0.5)

    async def commit_backfill_batch(
        self, chat_id: int, envelopes: list[Envelope], checkpoint: tuple[int, int]
    ) -> bool:

        if self._persist_checkpointed is None:
            self._log.error("❌ [Ingress] thiếu persist_checkpointed; không tiến cursor poll")
            return False
        try:
            rows = [self._build_row(env) for env in envelopes]
        except Exception as exc:
            self._log.error("❌ [Ingress] build backfill row lỗi: %r", exc)
            return False

        loop = asyncio.get_running_loop()
        results = None
        for attempt in range(self._retries):
            try:
                results = await loop.run_in_executor(
                    self._executor,
                    self._persist_checkpointed,
                    rows,
                    int(chat_id),
                    int(checkpoint[0]),
                    int(checkpoint[1]),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log.warning("⚠️ [Ingress] atomic backfill persist lỗi: %r", exc)
                results = None

            if results is not None and len(results) == len(envelopes) and not any(
                row_id is None for row_id in results
            ):
                break
            if attempt + 1 < self._retries:
                self._log.warning(
                    "⚠️ [Ingress] atomic checkpoint chưa xác nhận; retry %s/%s",
                    attempt + 1, self._retries - 1,
                )
                await asyncio.sleep(min(0.5, 0.05 * (2 ** attempt)))

        if results is None or len(results) != len(envelopes) or any(r is None for r in results):
            self._log.error(
                "❌ [Ingress] atomic backfill chưa commit chat=%s checkpoint=%s",
                chat_id, checkpoint,
            )
            return False

        self.batches += 1
        for env, row_id in zip(envelopes, results):
            if env.on_persisted is not None:
                env.on_persisted()
            if int(row_id) == 0:
                self.duplicates += 1
            else:
                self.rows_written += 1
                await self._deliver(env, int(row_id))
        return True

    async def _flush(self, batch: list) -> bool:
        """Persist a batch, retaining unconfirmed envelopes on any exit path."""
        loop = asyncio.get_running_loop()
        pending = list(batch)
        try:
            for attempt in range(self._retries):
                try:
                    rows = [self._build_row(e) for e in pending]
                    results = await loop.run_in_executor(self._executor, self._persist, rows)
                    if not isinstance(results, (list, tuple)) or len(results) != len(pending):
                        raise ValueError("persist must return one result per envelope")
                    if any(rid is not None and (type(rid) is not int or rid < 0) for rid in results):
                        raise ValueError("persist results must be nonnegative row IDs or None")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._log.error("❌ [Ingress] persist lỗi: %r", exc)
                    results = [None] * len(pending)
                self.batches += 1
                committed = []
                retry = []
                for env, rid in zip(pending, results):
                    if rid is None:
                        retry.append(env)
                    else:
                        if env.on_persisted is not None:
                            env.on_persisted()
                        if rid == 0:
                            self.duplicates += 1
                        else:
                            self.rows_written += 1
                            committed.append((env, rid))
                # Update before delivery awaits so cancellation cannot requeue
                # rows already confirmed in the durable inbox.
                confirmed = len(pending) - len(retry)
                pending = retry
                self._inflight_count -= confirmed
                for _ in range(confirmed):
                    self._slots.release()
                for env, rid in committed:
                    await self._deliver(env, rid)
                if not pending:
                    return True
                if attempt + 1 < self._retries:
                    self._log.warning(
                        "⚠️ [Ingress] ghi DB chưa được %s tin, thử lại %s/%s",
                        len(pending), attempt + 1, self._retries - 1,
                    )
                    await asyncio.sleep(min(0.5, 0.05 * (2 ** attempt)))
            self._log.error("❌ [Ingress] DB chưa lưu %s tin; giữ batch để thử lại", len(pending))
            return False
        finally:
            if pending:
                for env in pending:
                    self._q.put_nowait(env)
                self._inflight_count -= len(pending)

    async def _deliver(self, env: Envelope, row_id: int) -> None:
        try:
            result = self._on_row(env, row_id)
            if inspect.isawaitable(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._log.error("❌ [Ingress] on_row lỗi row=%s: %r", row_id, exc)

    async def drain(self, timeout: float = 5.0) -> bool:

        deadline = time.monotonic() + timeout
        while self.pending and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        return not self.pending