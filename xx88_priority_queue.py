

import asyncio
import random

from logger_setup import logger

# PATCH_MARKER_2026-10-28-queue-bounded-priority

XX88_SPOILER_CHANNEL_ID = -1002817093108
LLWIN_SPOILER_CHANNEL_ID = -1003873843218


def is_priority_xx88_spoiler(item):
    if not isinstance(item, dict):
        return False
    try:
        chat_id = int(item.get('chat_id') or 0)
    except (TypeError, ValueError):
        return False
    return (
        (chat_id == XX88_SPOILER_CHANNEL_ID or (
            chat_id == LLWIN_SPOILER_CHANNEL_ID
            and str(item.get('domain') or '').lower() == 'llwincode.com'))
        and str(item.get('source_kind') or item.get('source') or '').lower() == 'spoiler'
    )


class XX88SpoilerPriorityQueue(asyncio.Queue):

    def __init__(self, maxsize=0):
        super().__init__(maxsize=maxsize)
        self._on_drop = None
        self.dropped_count = 0

    def set_drop_callback(self, callback):
        self._on_drop = callback

    def drop_oldest(self):
        if self.empty():
            return None
        item = self._queue.popleft()
        self.task_done()
        self.dropped_count += 1
        if self._on_drop is not None:
            try:
                self._on_drop(item)
            except Exception:
                logger.exception("[Queue] drop callback failed")
        logger.warning("[Queue] queue full, dropped oldest item: %s", _item_label(item))
        return item

    def put_nowait(self, item):
        if self.full():
            self.drop_oldest()
        super().put_nowait(item)

    async def put(self, item):
        self.put_nowait(item)

    def _put(self, item):
        if not is_priority_xx88_spoiler(item):
            return super()._put(item)

        insertion = 0
        for pending in self._queue:
            if not is_priority_xx88_spoiler(pending):
                break
            insertion += 1
        self._queue.insert(insertion, item)


def _item_label(item):
    if isinstance(item, dict):
        return item.get("code") or item.get("work_item_id") or item.get("inbox_id") or "unknown"
    return str(item)


class RandomCodeQueue(asyncio.Queue):
    """Draw uniformly from the codes currently queued, without replacement."""

    def _get(self):
        index = random.randrange(len(self._queue))
        item = self._queue[index]
        del self._queue[index]
        return item


class RandomBoundedCodeQueue(XX88SpoilerPriorityQueue):
    """Random draws with the existing oldest-drop and durable recovery policy."""

    _put = asyncio.Queue._put
    _get = RandomCodeQueue._get
