from __future__ import annotations

_OCR_ONLY_CHANNEL_IDS = frozenset((-1002817093108, -1004395536862))


def _is_ocr_allowed_channel(chat_id, event=None) -> bool:
    try:
        return int(chat_id) in _OCR_ONLY_CHANNEL_IDS
    except (TypeError, ValueError):
        return False
