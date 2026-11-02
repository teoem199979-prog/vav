
from __future__ import annotations
import re
from config import Config, is_domain_enabled
from code_identity import normalize_code
from code_validator import CodeValidator
from logger_setup import logger
from telethon.tl.types import MessageEntitySpoiler

_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

_WWW_RE = re.compile(r"www\.\S+", re.IGNORECASE)

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
    r"HTTP|WWW|\.COM|FACEBOOK|TELEGRAM|TIKTOK|\bYOUTUBE\b|ZALO|CSKH|BOT|CHECK\s+LINK|LINK",
    re.IGNORECASE,
)

# PATCH_MARKER_2026-10-25-v8-bugfix (#8: SPECIAL_CODE_CHARS_30 BAT BUOC phai qua re.escape truoc
# khi chen vao character class. Chuoi mac dinh da chua ^, ], \ va
# nhieu ky tu dac biet khac — bo re.escape thi regex break hoac doi
# nghia (] dong class som, ^ thanh phu dinh). Khong sua thanh phan
# nay neu khong co test di kem.)
_TOKEN_CHARS = rf"A-Za-z0-9{re.escape(getattr(Config, 'SPECIAL_CODE_CHARS_30', ''))}"
_TOKEN_RE = re.compile(
    rf"(?<![{_TOKEN_CHARS}])[{_TOKEN_CHARS}]"
    rf"{{{getattr(Config, 'CODE_MIN_LENGTH', 6)},{getattr(Config, 'CODE_MAX_LENGTH', 15) + 30}}}"
    rf"(?![{_TOKEN_CHARS}])"
)

_PLAIN_TEXT_ORDER_PHONE_CONTEXT_RE = re.compile(
    r"(?:mã\s*(?:đơn|don|dh)|đơn\s*hàng|don\s*hang|order|invoice|"
    r"transaction|tracking|mã\s*giao\s*dịch|ma\s*giao\s*dich|"
    r"sđt|sdt|điện\s*thoại|dien\s*thoai|phone|hotline|liên\s*hệ|lien\s*he)",
    re.IGNORECASE,
)
_DEFERRED_CODE_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:[A-Za-z0-9]{2,}(?:<[^>\r\n]+>|%+)(?:[A-Za-z0-9%]*)|"
    r"<[^>\r\n]+>)(?![A-Za-z0-9])"
)

# PATCH_MARKER_2026-10-28-extraction-order-spoiler-first

def _iter_spoiler_texts(text: str, entities):

    if not text or not entities:
        return
    try:
        raw = text.encode("utf-16-le")
    except Exception:
        return
    for entity in entities:
        if not isinstance(entity, MessageEntitySpoiler):
            continue
        try:
            start = max(0, int(entity.offset)) * 2
            end = start + max(0, int(entity.length)) * 2
            spoiler_text = raw[start:end].decode("utf-16-le", errors="ignore").strip()
        except (AttributeError, TypeError, ValueError):
            continue
        if spoiler_text:
            yield spoiler_text


def get_filter_group_name(target_url: str) -> str:
    name, _ = CodeValidator.get_filter_group(target_url)
    return name

def unique_keep_order(items, target_url: str = ""):
    seen, result = set(), []
    for it in items or ():
        raw = str(it or "").strip()
        deferred = bool(_DEFERRED_CODE_RE.fullmatch(raw))
        c = raw if deferred else CodeValidator.clean_code(raw)
        c = normalize_code(c, target_url) if not deferred else c
        if not c:
            continue
        u = normalize_code(c, target_url)
        if u not in seen:
            seen.add(u)
            result.append(c)
    return result

def remove_noise_from_text(text: str) -> str:
    t = text or ""
    t = _URL_RE.sub(" ", t)
    t = _WWW_RE.sub(" ", t)
    t = _DOMAIN_RE.sub(" ", t)
    t = _HASHTAG_RE.sub(" ", t)
    return t.replace("：", ":").replace("|", " ").replace("•", " ")

def line_has_code_marker(line: str) -> bool:
    return bool(_CODE_MARKER_RE.search(line or ""))

def line_is_noise(line: str) -> bool:
    u = line.strip()
    if not u:
        return True
    if _NOISE_RE.search(u):
        return True
    if _CURRENCY_RE.search(u):
        return True
    return False

def extract_tokens_from_line(line: str):
    mn = getattr(Config, "CODE_MIN_LENGTH", 6)
    mx = getattr(Config, "CODE_MAX_LENGTH", 15)
    candidates = _DEFERRED_CODE_RE.findall(line or "")
    candidates.extend(
        c for c in _TOKEN_RE.findall(line or "")
        if mn <= len(CodeValidator.clean_code(c)) <= mx
    )
    return list(dict.fromkeys(candidates))

def extract_spoiler_codes(event, target_url: str):
    codes = []
    message = getattr(event, "message", None)
    if message is None:
        return None
    entities = getattr(message, "entities", None) or []
    signature = tuple(
        (getattr(entity, "offset", None), getattr(entity, "length", None))
        for entity in entities
        if isinstance(entity, MessageEntitySpoiler)
    )
    if not signature:
        return None
    full = getattr(message, "message", None) or getattr(message, "text", None) or ""
    cache_key = (target_url, full, signature)
    cached = getattr(event, "_autobot_spoiler_cache", None)
    if cached is not None and cached[0] == cache_key:
        return list(cached[1])
    try:
        seen_codes = set()
        seen_candidates = set()
        for sp in _iter_spoiler_texts(full, entities):
            for line in (sp.splitlines() if "\n" in sp else [sp]):
                line = line.strip()
                if not line:
                    continue
                for tok in extract_tokens_from_line(line):
                    candidate = (
                        tok if _DEFERRED_CODE_RE.fullmatch(tok)
                        else CodeValidator.clean_code(tok)
                    )
                    candidate_key = normalize_code(candidate, target_url)
                    if not candidate_key or candidate_key in seen_candidates:
                        continue
                    seen_candidates.add(candidate_key)
                    if candidate_key not in seen_codes:
                        seen_codes.add(candidate_key)
                        codes.append(candidate)
                        logger.info("🔒 Spoiler candidate: %s", candidate)
    except Exception as e:
        logger.warning(f"⚠️ spoiler: {e}")
    result = unique_keep_order(codes, target_url)
    try:
        event._autobot_spoiler_cache = (cache_key, tuple(result))
    except (AttributeError, TypeError):
        pass
    return result

def extract_marker_near_codes(text: str, target_url: str):
    lines = [l.strip() for l in remove_noise_from_text(text).splitlines()]
    codes = []
    _, group_config = CodeValidator.get_filter_group(target_url)
    marker_scan_lines = max(
        0,
        int(group_config.get("marker_scan_lines", 3) or 0),
    )
    for i, line in enumerate(lines):
        if not line_has_code_marker(line):
            continue
        scan = [line] if line else []
        for off in range(1, marker_scan_lines + 1):
            if i + off < len(lines):
                scan.append(lines[i + off])
        for sl in scan:
            if line_is_noise(sl):
                continue
            for tok in extract_tokens_from_line(sl):
                codes.append(tok)
                logger.info("🎯 Marker candidate: %s", tok)
    return unique_keep_order(codes, target_url)

def _plain_text_token_is_safe(token: str, line: str) -> bool:

    if _DEFERRED_CODE_RE.fullmatch(token or ""):
        return True
    clean = CodeValidator.clean_code(token)
    if not clean or not any(ch.isalpha() for ch in clean):

        return False

    digit_count = sum(ch.isdigit() for ch in clean)
    if digit_count >= 7 or (len(clean) >= 10 and digit_count / len(clean) >= 0.7):
        return False

    context = line[max(0, line.find(token) - 40): line.find(token) + len(token) + 40]
    if _PLAIN_TEXT_ORDER_PHONE_CONTEXT_RE.search(context):
        return False

    return True

def extract_plain_text_codes(text: str, target_url: str):

    if not text:
        return []

    codes = []

    for raw_line in remove_noise_from_text(text).splitlines():
        line = raw_line.strip()
        if not line or line_has_code_marker(line) or line_is_noise(line):
            continue
        for token in extract_tokens_from_line(line):
            if not _plain_text_token_is_safe(token, line):
                continue
            codes.append(token)
            logger.info("🎯 Plain-text candidate: %s", token)
    return unique_keep_order(codes, target_url)

def extract_codes_from_message(
    event,
    raw_text: str,
    target_url: str,
    channel_name: str = "",
    include_text_after_spoiler: bool = False,
    spoiler_codes: list[str] | None = None,
):

    if not is_domain_enabled(target_url):
        logger.debug("[EXTRACT] disabled domain=%s", target_url)
        return []

    group = get_filter_group_name(target_url)
    logger.debug(f"[EXTRACT] group={group} | url={target_url}")

    if spoiler_codes is None:
        spoiler = extract_spoiler_codes(event, target_url)
        if spoiler is not None:
            logger.warning("🎯 [SPOILER] %s: %s", len(spoiler), spoiler)
            if not include_text_after_spoiler or not spoiler:
                return unique_keep_order(spoiler, target_url)
            collected = list(spoiler)
        else:
            collected = []
    else:
        spoiler = list(spoiler_codes)
        if spoiler and not include_text_after_spoiler:
            return unique_keep_order(spoiler, target_url)
        collected = spoiler

    mc = extract_marker_near_codes(raw_text, target_url)
    if mc:
        logger.warning(f"🎯 [MARKER] {len(mc)}: {mc}")
        collected.extend(mc)

    if not collected:
        pc = extract_plain_text_codes(raw_text, target_url)
        if pc:
            logger.warning(f"🎯 [PLAIN-TEXT] {len(pc)}: {pc}")
            collected.extend(pc)

    if collected:
        merged = unique_keep_order(collected, target_url)
        logger.warning(f"🎯 [EXTRACT-MERGED] {len(merged)}: {merged}")
        return merged

    return []
