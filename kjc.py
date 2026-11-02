
from __future__ import annotations
import re
from urllib.parse import urlsplit
from telethon.tl.types import MessageEntitySpoiler, MessageEntityCustomEmoji
from config import Config
from code_identity import normalize_code
from code_validator import CodeValidator
from logger_setup import logger
from code_extraction import extract_tokens_from_line, unique_keep_order

_KJC_SPECIAL_CHANNEL_IDS = Config.KJC_SPECIAL_CHANNEL_IDS


_KJC_BROADCAST_URLS = Config.KJC_BROADCAST_URLS


# PATCH_MARKER_2026-10-28-extraction-order-spoiler-first


def _configured_kjc_target(chat_id):
    channel = getattr(Config, "CHANNEL_CONFIG", {}).get(int(chat_id), {})
    url = str(channel.get("url", "") or "")
    host = (urlsplit(url if "://" in url else "//" + url).hostname or "").lower()
    aliases = {domain for domain, _target in _LOCAL_SITE_ALIASES.values()}
    domain = next(
        (known for known in aliases if host == known or host.endswith("." + known)),
        None,
    )
    return domain, url or _KJC_BROADCAST_URLS.get(domain)


def is_kjc_special_channel(chat_id) -> bool:
    try:
        return int(chat_id) in _KJC_SPECIAL_CHANNEL_IDS
    except Exception:
        return False


# PATCH_MARKER_2026-10-26-parallel-llwin-local-labels
_LOCAL_SITE_ALIASES = {
    "XX88": ("xx88code.com", "https://xx88code.com"),
    "MM88": ("livemm88.net", "https://livemm88.net/nhap-code"),
    "RR88": ("liverr88.net", "https://liverr88.net/nhap-code"),
    "GG88": ("gg88live.tv", "https://gg88live.tv/nhap-code"),
    "V79": ("v79code.com", "https://v79code.com/"),
    "LLWIN": ("llwincode.com", "https://llwincode.com/"),
}
_LOCAL_SITE_LABEL = re.compile(
    r"(?<![\w/\.])(?:XX88|MM88|RR88|GG88|V79|LLWIN)(?![\w.])", re.I
)


def _sticker_document_ids(message):
    ids = []
    sticker = getattr(message, "sticker", None)
    if sticker:
        document = getattr(sticker, "document", None)
        document_id = (
            getattr(document, "id", None)
            or getattr(sticker, "document_id", None)
            or getattr(sticker, "id", None)
        )
        if document_id is not None:
            ids.append(document_id)

    media = getattr(message, "media", None)
    document = getattr(media, "document", None)
    attributes = getattr(document, "attributes", ()) or ()
    is_sticker = bool(sticker) or any(
        type(attribute).__name__ == "DocumentAttributeSticker"
        for attribute in attributes
    )
    if is_sticker and getattr(document, "id", None) is not None:
        ids.append(document.id)
    return tuple(dict.fromkeys(ids))


def _entity_char_span(text, entity):
    """Telegram offsets count UTF-16 units, including emoji surrogate pairs."""
    raw = text.encode("utf-16-le")
    start, length = int(entity.offset), int(entity.length)
    if start < 0 or length <= 0 or (start+length)*2 > len(raw):
        return None
    return (len(raw[:start*2].decode("utf-16-le", errors="ignore")),
            len(raw[:(start+length)*2].decode("utf-16-le", errors="ignore")))


def extract_kjc_routed_items(event, channel_name="") -> list[dict]:
    """Associate each spoiler with its local text/custom-emoji site label.

    A row may contain several labelled code groups. A label-only preceding
    line can introduce the next row. Footer brands and unrelated article text
    never supply a message-wide default, and there is no broadcast fallback.
    """
    if not is_kjc_special_channel(getattr(event, "chat_id", None)):
        return []
    message = getattr(event, "message", None)
    text = (getattr(message, "message", None) or getattr(message, "text", None) or "")
    if not text:
        return []
    entities = getattr(message, "entities", None) or []
    spoilers, emoji, barriers = [], [], []
    mapping = {
        **{str(key): value for key, value in getattr(Config, "KJC_CUSTOM_EMOJI_DOMAINS", {}).items()},
        **{str(key): value for key, value in getattr(Config, "KJC_STICKER_DOMAINS", {}).items()},
        **{str(key): value for key, value in getattr(Config, "KJC_BRAND_MAPPING", {}).items()},
    }
    separators = {str(value) for value in getattr(Config, "KJC_CODE_SEPARATOR_EMOJI_IDS", ())}
    known_document_found = False
    unknown_document_ids = set()
    mapped_emoji_domains = set()
    for entity in entities:
        if not isinstance(entity, (MessageEntitySpoiler, MessageEntityCustomEmoji)):
            continue
        try:
            span = _entity_char_span(text, entity)
        except (AttributeError, TypeError, ValueError, UnicodeError):
            continue
        if span is None:
            continue
        if isinstance(entity, MessageEntitySpoiler):
            spoilers.append(span)
        else:
            try:
                brand_id = str(entity.document_id)
            except AttributeError:
                continue
            domain = mapping.get(brand_id)
            emoji.append((*span, domain))
            if domain is not None:
                known_document_found = True
                mapped_emoji_domains.add(domain)
            elif brand_id not in separators:
                known_document_found = True
                unknown_document_ids.add(brand_id)
                barriers.append(span)

    sticker_domains = set()
    for raw_id in _sticker_document_ids(message):
        brand_id = str(raw_id)
        domain = mapping.get(brand_id)
        if domain is None:
            known_document_found = True
            unknown_document_ids.add(brand_id)
            barriers.append((0, len(text)))
        else:
            known_document_found = True
            sticker_domains.add(domain)
    for brand_id in sorted(unknown_document_ids):
        logger.warning("UNKNOWN_BRAND_EMOJI ID=%s", brand_id)
    if len(sticker_domains) > 1:
        logger.warning("⚠️ [KJC] Conflicting sticker brand IDs; skipping message")
        return []

    mapped_emoji = [(a, b, domain) for a, b, domain in emoji if domain]
    # Telegram's sticker document has no text offset, so use its brand only
    # for spoilers without a nearer custom-emoji brand label.
    sticker_domain = next(iter(sticker_domains), None)
    fallback_domain, _fallback_url = _configured_kjc_target(
        getattr(event, "chat_id", None)
    )
    spoilers = sorted(set(spoilers))
    if not spoilers:
        return []

    def in_spoiler(start, end):
        return any(start < b and end > a for a, b in spoilers)

    labels = [(a, b, domain) for a, b, domain in mapped_emoji if not in_spoiler(a, b)]
    preserve_llwin_text_fallback = (
        mapped_emoji_domains == {"llwincode.com"}
        and not unknown_document_ids
        and not sticker_domains
    )
    if not known_document_found or preserve_llwin_text_fallback:
        labels.extend((m.start(), m.end(), _LOCAL_SITE_ALIASES[m.group().upper()][0])
                      for m in _LOCAL_SITE_LABEL.finditer(text) if not in_spoiler(m.start(), m.end()))
    labels.sort()
    urls = {domain: url for domain, url in _LOCAL_SITE_ALIASES.values()}
    active = set(getattr(Config, "ACTIVE_DOMAINS", ()) or ())
    items, seen = [], set()
    for start, end in spoilers:
        lo, hi = text.rfind("\n", 0, start)+1, text.find("\n", end)
        hi = len(text) if hi < 0 else hi
        # Separators outside spoilers create independent labelled cells.
        row_start = lo
        for mark in re.finditer(r"[|;]", text[lo:hi]):
            pos = row_start+mark.start()
            if in_spoiler(pos, pos+1):
                continue
            if pos < start:
                lo = pos+1
            elif pos >= end:
                hi = min(hi, pos)
        row_labels = [label for label in labels if lo <= label[0] and label[1] <= hi]
        row_codes = [(a, b) for a, b in spoilers if lo <= a and b <= hi]
        chosen = None
        if row_labels:
            domains = {label[2] for label in row_labels}
            if sticker_domain is not None and domains != {sticker_domain}:
                continue
            if len(row_codes) == 1 and len(domains) > 1:
                continue  # Conflicting labels surrounding one code are ambiguous.
            prefix = row_labels[0][0] < row_codes[0][0]
            options = ([label for label in row_labels if label[1] <= start] if prefix
                       else [label for label in row_labels if label[0] >= end])
            if options:
                chosen = options[-1] if prefix else options[0]
        elif lo > 0:
            prev_end = lo-1
            prev_start = text.rfind("\n", 0, prev_end)+1
            previous = [label for label in labels if prev_start <= label[0] and label[1] <= prev_end]
            if previous and len({label[2] for label in previous}) == 1:
                residue = list(text[prev_start:prev_end])
                for a, b in [(a, b) for a, b, _ in previous]+[(a, b) for a, b, _ in emoji]:
                    for i in range(max(a, prev_start), min(b, prev_end)):
                        residue[i-prev_start] = " "
                rest = re.sub(r"NHẬN|NHAN|NHẬP|NHAP|CODE|NGAY", "", "".join(residue), flags=re.I)
                if not any(c.isalnum() for c in rest) and not any(prev_start <= a < prev_end for a, b in spoilers):
                    chosen = previous[-1]
        if chosen is None:
            if sticker_domain is not None and not any(
                lo <= label[0] and label[1] <= hi for label in labels
            ) and not any(
                barrier_start < hi and barrier_end > lo
                for barrier_start, barrier_end in barriers
            ):
                chosen = (0, len(text), sticker_domain)
            elif not row_labels and fallback_domain is not None:
                chosen = (0, len(text), fallback_domain)
            else:
                continue
        a, b, domain = chosen
        if domain not in urls or active and domain not in active:
            continue
        between_start, between_end = (b, start) if b <= start else (end, a)
        if any(between_start <= x and y <= between_end and not in_spoiler(x, y)
               for x, y in barriers):
            continue  # Unknown logo is not silently treated as the preceding brand.
        target = urls[domain]
        for line in text[start:end].splitlines():
            for candidate in extract_tokens_from_line(line):
                if not candidate:
                    continue
                code = candidate.strip()
                deferred = bool(re.search(r"<[^>\r\n]+>|%", code))
                if not deferred:
                    code = CodeValidator.clean_code(code)
                    code = normalize_code(code, domain)
                code_key = normalize_code(code, domain)
                if code and (domain, code_key) not in seen:
                    seen.add((domain, code_key))
                    items.append({"code": code, "channel_name": channel_name,
                                  "target_url": target, "domain": domain,
                                  "source": "kjc_spoiler", "source_kind": "spoiler"})
    return items
