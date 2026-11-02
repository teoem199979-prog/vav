"""Stable Telegram media identity shared by ingress and OCR caching."""
from typing import Any


def telegram_media_identity(message: Any) -> str | None:
    media = getattr(message, "media", None)
    for kind in ("document", "photo", "video", "animation", "audio", "voice", "video_note", "sticker"):
        item = getattr(media, kind, None)
        if item is None:
            item = getattr(message, kind, None)
        if isinstance(item, (list, tuple)):
            item = item[-1] if item else None
        if item is None:
            continue
        unique_id = getattr(item, "file_unique_id", None)
        if unique_id is not None:
            return f"{kind}:unique:{unique_id}"
        item_id = getattr(item, "id", None)
        if item_id is not None:
            return f"{kind}:{item_id}"
        file_id = getattr(item, "file_id", None)
        if file_id is not None:
            return f"{kind}:file:{file_id}"
    for item in (media, getattr(message, "file", None), message):
        unique_id = getattr(item, "file_unique_id", None)
        if unique_id is not None:
            return f"media:unique:{unique_id}"
    media_id = getattr(media, "id", None)
    if media_id is not None:
        return f"{type(media).__name__}:{media_id}"
    media_id = getattr(message, "media_id", None)
    if media_id is not None:
        return f"media:{media_id}"
    return None
