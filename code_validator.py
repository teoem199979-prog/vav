

from __future__ import annotations

import ast
import math
import operator
import re
from functools import lru_cache as _lru_cache
from config import Config
from code_identity import normalize_code, normalize_code_domain
from logger_setup import logger


_SAFE_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.USub: operator.neg,
}


def _safe_eval_math(expr: str):

    try:
        tree = ast.parse(expr.strip(), mode="eval")
        return _eval_node(tree.body)
    except Exception:
        raise ValueError(f"Invalid expression: {expr}")


def _eval_node(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _SAFE_OPS:
        return _SAFE_OPS[type(node.op)](_eval_node(node.left), _eval_node(node.right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _SAFE_OPS:
        return _SAFE_OPS[type(node.op)](_eval_node(node.operand))
    raise ValueError("Unsupported expression")


_XX88_BIGWIN_RE = re.compile(r'^[A-Za-z0-9](\*[A-Za-z0-9]){4,}$')
_ALPHA_ONLY_RE = re.compile(r"[A-Z]+")
_VIET_NUMBER_DIGITS_RE = re.compile(r'^s[oố]\s*(\d+)$')
_VIET_NUMBER_WORD_RE = re.compile(r'^s[oố]\s+(.+)$')
_PLACEHOLDER_ANGLE_RE = re.compile(r'<([^>]+)>')
_PLACEHOLDER_MATH_RE = re.compile(r'^[\d+\-*/%\s()]+$')
_PLACEHOLDER_QUOTE_RE = re.compile(r'"([^"]+)"')
_VIET_QUOTE_RE = re.compile(r'"s[oố]\s*\w+"')
_REPEATED_CHAR_RE = re.compile(r"(.)\1{2,}")
_SPOILER_PLACEHOLDER_RE = re.compile(r"<[^>]*>|%+|\"[^\"]*\"")
_KT_PLACEHOLDER_RE = re.compile(r"KT\s*(<[^>]+>)", re.IGNORECASE)
_VALIDATOR_CACHE_VERSION = 2


class CodeValidator:
    SITE_ROUTING_RULES = {

        "mm88": ["MM88"],

    }

    COMMON_WORDS = [
        "CHUC", "MUNG", "HOM", "NAY", "NGAY", "THANG", "TANG", "LIXI", "NHAN", "THUONG",
        "DANG", "NHAP", "THAM", "GIA", "LINK", "GAME", "RUT", "NAP",
        "TIEN", "TAI", "KHOAN", "KHUYEN", "MAI", "DANGKY", "THANHCONG",
        "NHOM", "KENH", "ADMIN", "HOTRO", "CSKH", "ZALO", "TELE",
        "DANGNHAP", "MATKHAU", "LIENHE", "TRANGCHU", "NHACAI", "UYTIN",
        "BAOTRI", "NOHU", "BANCA", "THETHAO", "CASINO", "LODE", "XOSO",
        "KHONG", "DUOC", "HAY", "THOI", "GIAI", "TRI", "MUC", "VIP",
        "THEO", "DOI", "CHIA", "LIKE", "SHARE", "YOUTUBE",
        "MINIGAME", "O8THETHAO", "BONGDA", "TRUYCAP", "GIFTCODE", "EVENT",
        "FACEBOOK", "TELEGRAM", "TIKTOK", "WEBSITE", "OFFICIAL", "CHANNEL",
        "QUATANG", "BOT", "CHECK", "FREE", "ONLINE", "DAILY", "CLIP",
        "NHANH", "NHANHTAY", "ANH", "EM", "DUNG", "BO", "LO", "JACKPOT",
        "CANHBAO", "GIAMAO", "THONGBAO", "DANGNHAP", "BAOMAT", "KIEMTRA",
        "TAIDAY", "THONGTIN", "HOTLINE", "SUPPORT", "CHAT", "POST",
        "VIEW", "COMMENT", "PINNED", "SUBSCRIBE", "JOIN", "GROUP",
        "DANHSACH", "DIEUKIEN", "HUONGDAN", "KETQUA", "CHUCDANH", "PHAT",
        "XEMNGAY", "CLICK", "LOGIN", "PASSWORD", "TAIKHOAN", "KHUYENMAI",
        "TIENTHUONG", "SIEUTIENTHUONG", "SIEUPHAM",
        "DOTPHA", "GIAITHUONG", "GIAICUU", "CUOCTHUA",
        "NHANTHUONG", "MINHTHUONG", "PHANTHUONG",
        "NOHUBANCA", "BANCANO", "NOHUTAPDO",
        "SIEUKHUYEN", "SIEUNAP", "DOITHUONG",
        "REVIEWPHIM", "TINTUC", "TINTUCHANGAY",
        "NOHU47", "BANCA47",
        "VOUCHER", "BONUS", "GIFT", "REWARD", "PROMO", "PROMOTION",
        "WELCOME", "DEPOSIT", "WITHDRAW", "REGISTER", "SIGNUP", "MOBILE",
    ]

    VIETNAMESE_TEXT_WORDS = [
        "khong", "dung", "nhanh", "tang", "code", "free", "clip", "vui",
        "dang", "nhap", "dangky", "truycap", "chinh", "thuc", "kenh",
        "thong", "bao", "canh", "gia", "mao", "kiem", "tra", "duong",
        "link", "facebook", "tiktok", "telegram", "zalo", "website",
        "hom", "nay", "anh", "em", "nhan", "qua", "thuong", "jackpot",
        "may", "man", "don", "cho", "chat", "bot", "cskh", "hotro",
        "lienhe", "taiday", "dangnhap", "matkhau", "taikhoan",
    ]

    FAKE_CODE_PATTERNS = [
        r"^(TEST|DEMO|EXAMPLE|FAKE|SAMPLE)",
        r"^(ABC|DEF|GHI|JKL|MNO|PQR|STU|VWX|YZ)$",
        r"^(123|456|789|000|111|222|333|444|555|666|777|888|999)$",
        r"^(AAAA|BBBB|CCCC|DDDD|EEEE|FFFF|GGGG|HHHH|IIII|JJJJ)$",
    ]

    _FAKE_CODE_REGEXES = [re.compile(p) for p in FAKE_CODE_PATTERNS]

    GAME_BRAND_BLACKLIST = {
        "JILI", "PGSOFT", "PRAGMATIC", "GOLDEN", "EMPIRE", "JOKER",
        "CQ9", "AMB", "FACHAI", "SPADEGAMING", "MICROGAMING", "SCATTER",
    }

    SOFT_BLACKLIST = {"CODE", "GAME", "FREE", "VIP", "NAP", "RUT"}
    HARD_BLACKLIST = {
        "HTTP", "HTTPS", "WWW", "FACEBOOK", "TELEGRAM", "TIKTOK", "ZALO",
        "CHECK", "CLIP", "DAILY", "TRUYCAP", "BANCA", "NOHU", "ONLINE",
        "GIFTCODE", "MINIGAME", "THETHAO", "O8THETHAO", "BONGDA", "TROLL",
        "SUPPORT", "HOTLINE", "CSKH",
    }

    OCR_JUNK_KEYWORDS = [
        "TIENTHUONG", "NOHUBAN", "BANCANO", "SIEUTHUO",
        "DOTPHA", "GIAICUU", "CUOCTHUA", "REVIEWPHIM",
        "TINTUCMO", "NOHUTAP", "KHOGIF",
        "IENTHU", "IEUTHU", "AKTIEN", "SIKTIEN",
        "MEGALIVE", "MEGA LIVE", "BUNGNA",
    ]

    @staticmethod
    def clean_code(
        code,
        to_upper: bool = False,
        *,
        domain: str = "",
        preserve_placeholders: bool = False,
    ):

        if not code:
            return ""
        s = str(code).strip()

        if _XX88_BIGWIN_RE.match(s):
            s = s.replace('*', '')
            return normalize_code(s, domain, force_uppercase=to_upper)

        if preserve_placeholders:
            parts = []
            offset = 0
            for match in _SPOILER_PLACEHOLDER_RE.finditer(s):
                parts.append(re.sub(r"[^A-Za-z0-9]", "", s[offset:match.start()]))
                parts.append(match.group(0))
                offset = match.end()
            parts.append(re.sub(r"[^A-Za-z0-9]", "", s[offset:]))
            return normalize_code("".join(parts), domain, force_uppercase=to_upper)

        out = []
        append = out.append
        for ch in s:
            if ('a' <= ch <= 'z') or ('A' <= ch <= 'Z') or ('0' <= ch <= '9'):
                append(ch)
        return normalize_code("".join(out), domain, force_uppercase=to_upper)

    @staticmethod
    def calculate_entropy(code):
        if not code:
            return 0.0

        char_freq = {}
        for char in code:
            char_freq[char] = char_freq.get(char, 0) + 1

        entropy = 0.0
        code_len = len(code)

        for freq in char_freq.values():
            p = freq / code_len
            if p > 0:
                entropy -= p * math.log2(p)

        return entropy

    @classmethod
    def get_filter_group(cls, target_url="", filter_group_name=None):
        groups = getattr(Config, "CODE_FILTER_GROUPS", {}) or {}

        if filter_group_name and filter_group_name in groups:
            return filter_group_name, groups[filter_group_name]

        target_lower = (target_url or "").lower()
        for group_name, group_config in groups.items():
            if group_name == "default":
                continue
            keywords = group_config.get("url_keywords", [])
            if any(str(keyword).lower() in target_lower for keyword in keywords):
                return group_name, group_config

        return "default", groups.get("default", {})

    @staticmethod
    def get_special_chars(group_config=None):
        group_config = group_config or {}
        return str(group_config.get("special_chars") or getattr(Config, "SPECIAL_CODE_CHARS_30", ""))

    @classmethod
    def count_special_chars(cls, raw_code, group_config=None):
        special_chars = set(cls.get_special_chars(group_config))
        return sum(1 for char in str(raw_code or "") if char in special_chars)

    @staticmethod
    def is_sequential_code(code):
        if not code:
            return True

        upper = code.upper()

        if len(set(upper)) <= 2 and len(upper) >= 6:
            return True

        if len(code) >= 4:
            pattern_1 = code[:1]
            if pattern_1 and pattern_1 * len(code) == code:
                return True

            pattern_2 = code[:2]
            if len(code) % 2 == 0 and pattern_2 * (len(code) // 2) == code:
                return True

            pattern_3 = code[:3]
            if len(code) % 3 == 0 and pattern_3 * (len(code) // 3) == code:
                return True

        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        if _ALPHA_ONLY_RE.fullmatch(upper) and len(upper) >= 4:
            if upper in alphabet or upper in alphabet[::-1]:
                return True

        digits = "0123456789"
        if code.isdigit() and len(code) >= 4:
            if code in digits or code in digits[::-1]:
                return True

        return False

    @staticmethod
    def looks_like_domain_or_link(code):
        upper = code.upper()

        if upper.startswith(("HTTP", "HTTPS", "WWW", "TME", "TELEGRAM", "FACEBOOK", "TIKTOK")):
            return True

        if upper.endswith(("COM", "NET", "ORG", "VN", "APP", "INFO")) and len(upper) <= 14:
            return True

        if any(fragment in upper for fragment in ("DOTCOM", "CHAMCOM", "COMVN", "NETVN")):
            return True

        return False

    @classmethod
    def detect_site_identity(cls, clean_code):
        clean_upper = clean_code.upper()

        for site_key, prefixes in cls.SITE_ROUTING_RULES.items():
            if any(clean_upper.startswith(prefix) for prefix in prefixes):
                return site_key

        return None

    @classmethod
    def has_code_shape(cls, code, group_config=None):
        if not code:
            return False

        group_config = group_config or {}
        length = len(code)

        min_len = int(group_config.get("min_clean_length", getattr(Config, "CODE_MIN_LENGTH", 6)) or 6)
        max_len = int(group_config.get("max_clean_length", getattr(Config, "CODE_MAX_LENGTH", 15)) or 15)

        if length < min_len:
            return False

        if length > max_len:
            return False

        if bool(group_config.get("require_uppercase", False)) and code != code.upper():
            return False

        has_lower = any(c.islower() for c in code)
        has_upper = any(c.isupper() for c in code)
        has_digit = any(c.isdigit() for c in code)
        has_letter = any(c.isalpha() for c in code)
        entropy = cls.calculate_entropy(code)
        min_entropy = float(group_config.get("min_entropy", 2.3))
        uppercase_min_entropy = float(group_config.get("uppercase_min_entropy", 2.9))
        allow_numeric = bool(group_config.get("allow_numeric", True))
        allow_random_mix = bool(group_config.get("allow_random_mix", True))

        if cls.detect_site_identity(code):
            return entropy >= min_entropy

        if code.isdigit():
            return allow_numeric and 8 <= length <= 12 and entropy >= 2.6

        if not has_letter:
            return False

        if has_upper and has_lower:
            return allow_random_mix and entropy >= min_entropy

        if has_letter and has_digit:
            return entropy >= min_entropy

        if code.isupper() and length >= min_len and entropy >= uppercase_min_entropy:
            return True

        return False

    _SEGMENT_WORDS_CACHE: frozenset | None = None

    @classmethod
    def is_composed_of_common_words(cls, code):

        up = code.upper()
        if not up.isalpha():
            return False
        if cls._SEGMENT_WORDS_CACHE is None:
            cls._SEGMENT_WORDS_CACHE = frozenset(w for w in cls.COMMON_WORDS if len(w) >= 3)
        words = cls._SEGMENT_WORDS_CACHE
        n = len(up)
        best = [None] * (n + 1)
        best[0] = 0
        for i in range(1, n + 1):
            for j in range(max(0, i - 15), i):
                if best[j] is not None and up[j:i] in words:
                    cand = best[j] + 1
                    if best[i] is None or cand < best[i]:
                        best[i] = cand
        return best[n] is not None and best[n] >= 2

    @classmethod
    def is_text_word(cls, code):
        lower = code.lower()
        upper = code.upper()

        if lower in cls.VIETNAMESE_TEXT_WORDS:
            return True

        if upper in cls.COMMON_WORDS:
            return True

        if upper in cls.GAME_BRAND_BLACKLIST:
            return True

        if code.isalpha() and code[:1].isupper() and code[1:].islower() and len(code) >= 4:
            return True

        if code.islower() and not any(c.isdigit() for c in code):
            return True

        if code.isupper() and not any(c.isdigit() for c in code):
            if upper in cls.COMMON_WORDS:
                return True
            if len(code) <= 5:
                return True
            if len(code) <= 7 and cls.calculate_entropy(code) < 2.4:
                return True

        return False

    _config_blacklist_upper_cache: frozenset | None = None

    @classmethod
    def _get_config_blacklist_upper(cls) -> frozenset:
        if cls._config_blacklist_upper_cache is None:
            cls._config_blacklist_upper_cache = frozenset(
                str(item).upper() for item in getattr(Config, "CODE_BLACKLIST", []) if str(item).strip()
            )
        return cls._config_blacklist_upper_cache

    @classmethod
    def contains_blacklisted_fragment(cls, code, group_config=None):
        group_config = group_config or {}
        code_upper = code.upper()
        config_blacklist = cls._get_config_blacklist_upper()
        group_soft_blacklist = {str(item).upper() for item in group_config.get("soft_blacklist", []) if str(item).strip()}

        soft_blacklist = cls.SOFT_BLACKLIST | group_soft_blacklist
        hard_blacklist = cls.HARD_BLACKLIST | (config_blacklist - soft_blacklist)

        for word in hard_blacklist:
            if not word:
                continue
            if word == code_upper:
                return True
            if len(word) >= 5 and word in code_upper and len(code_upper) <= len(word) + 4:
                return True

        has_digit = any(c.isdigit() for c in code)
        has_lower = any(c.islower() for c in code)
        has_upper = any(c.isupper() for c in code)

        for word in soft_blacklist:
            if not word:
                continue
            if word == code_upper:
                return True
            if word in code_upper and len(code_upper) <= len(word) + 2 and not (has_digit or (has_lower and has_upper)):
                return True

        return False

    @classmethod
    def is_site_allowed_for_group(cls, site_identity, group_config):
        if not site_identity:
            return True

        allowed_sites = group_config.get("allowed_sites", []) if group_config else []
        if not allowed_sites:
            return True

        return site_identity in {str(site).lower() for site in allowed_sites}

    @classmethod
    def is_likely_fake(cls, code, group_config=None):
        if not code:
            return True
        code_upper = code.upper()
        group_config = group_config or {}

        for fake_regex in cls._FAKE_CODE_REGEXES:
            if fake_regex.match(code_upper):
                return True

        for word in cls.COMMON_WORDS:
            if word not in code_upper:
                continue
            has_digit_in_code = any(c.isdigit() for c in code_upper)

            if len(word) >= 6:
                return True
            if len(code_upper) <= len(word) + 2 and not has_digit_in_code:
                return True

        code_upper_nospace = code.upper().replace(" ", "")
        for junk in cls.OCR_JUNK_KEYWORDS:
            if junk in code_upper_nospace:
                return True

        entropy = cls.calculate_entropy(code)
        min_entropy = float(group_config.get("min_entropy", 2.3))

        return entropy < min_entropy

    _VIET_DIGIT_MAP = {
        "không": "0", "khong": "0",
        "một": "1", "mot": "1",
        "hai": "2",
        "ba": "3",
        "bốn": "4", "bon": "4",
        "năm": "5", "nam": "5",
        "sáu": "6", "sau": "6",
        "bảy": "7", "bay": "7",
        "tám": "8", "tam": "8",
        "chín": "9", "chin": "9",
        "mười": "10", "muoi": "10",
    }

    @classmethod
    def _resolve_viet_number(cls, text: str) -> str:

        text_lower = text.strip().lower()

        m = _VIET_NUMBER_DIGITS_RE.match(text_lower)
        if m:
            return m.group(1)

        m = _VIET_NUMBER_WORD_RE.match(text_lower)
        if m:
            text_lower = m.group(1).strip()

        if text_lower in cls._VIET_DIGIT_MAP:
            return cls._VIET_DIGIT_MAP[text_lower]

        words = text_lower.split()
        if words and all(word in cls._VIET_DIGIT_MAP for word in words):
            return "".join(cls._VIET_DIGIT_MAP[word] for word in words)

        return text

    @classmethod
    def _expand_placeholder_code(cls, code: str) -> str:

        result = code

        def replace_angle(match):
            placeholder = match.group(1).strip()
            resolved = cls._resolve_viet_number(placeholder)
            if resolved != placeholder and resolved.isdigit():
                return resolved
            if _PLACEHOLDER_MATH_RE.fullmatch(placeholder):
                try:
                    return str(int(_safe_eval_math(placeholder)))
                except (ArithmeticError, SyntaxError, TypeError, ValueError):
                    return match.group(0)
            return match.group(0)

        result = _KT_PLACEHOLDER_RE.sub(lambda match: match.group(1), result)
        result = _PLACEHOLDER_ANGLE_RE.sub(replace_angle, result)

        def replace_quote(match):
            inner = match.group(1).strip()
            resolved = cls._resolve_viet_number(inner)
            if resolved != inner and resolved.isdigit():
                return resolved
            return match.group(0)

        return _PLACEHOLDER_QUOTE_RE.sub(replace_quote, result)

    @classmethod
    def validate_code(cls, code, target_url="", filter_group_name=None, source="normal"):
        return validate_code_cached(code, target_url, filter_group_name, source)

    @classmethod
    def validate_code_cached(cls, code, target_url="", filter_group_name=None, source="normal"):
        return validate_code_cached(code, target_url, filter_group_name, source)

    @classmethod
    def _validate_code_uncached(cls, code, target_url="", filter_group_name=None, source="normal"):
        raw_code = str(code or "").strip()

        group_name, group_config = cls.get_filter_group(target_url, filter_group_name)
        source_aliases = {"normal": "text", "image_ocr": "ocr"}
        source = source_aliases.get(source, source)
        if source == "spoiler":
            raw_code = cls.clean_code(
                raw_code,
                domain=target_url,
                preserve_placeholders=True,
            )
            logger.info(f"🔧 [MMOO] Placeholder detected: {raw_code}")
            expanded = cls._expand_placeholder_code(raw_code)
            if expanded != raw_code:
                logger.info(f"✅ [MMOO] Expanded: {raw_code} → {expanded}")
            raw_code = expanded
            if (
                "%" in raw_code
                or "KT" in raw_code.upper()
                or "<" in raw_code
                or ">" in raw_code
                or '"' in raw_code
            ):
                clean_code = cls.clean_code(raw_code, domain=target_url)
                return {
                    "valid": False, "confidence": 0.0,
                    "reason": "Unresolved spoiler placeholder",
                    "is_fake": False, "entropy": 0.0,
                    "recommendation": "SKIP", "clean_code": clean_code,
                    "raw_code": raw_code, "filter_group": group_name,
                    "special_count": cls.count_special_chars(raw_code, group_config),
                    "source": source,
                }

        force_uppercase = bool(group_config.get("force_uppercase", False))
        clean_code = cls.clean_code(raw_code, to_upper=force_uppercase, domain=target_url)
        special_count = cls.count_special_chars(raw_code, group_config)

        result = {
            "valid": False,
            "confidence": 0.0,
            "reason": "",
            "is_fake": False,
            "entropy": 0.0,
            "recommendation": "SKIP",
            "clean_code": clean_code,
            "raw_code": raw_code,
            "filter_group": group_name,
            "special_count": special_count,
            "source": source,
        }

        if not clean_code:
            result["reason"] = "Empty code"
            return result

        if not cls.has_code_shape(clean_code, group_config):
            result["reason"] = "Invalid code shape"
            return result

        if cls.is_likely_fake(clean_code, group_config):
            result["is_fake"] = True
            result["reason"] = f"🚫 Nhận diện là chữ quảng cáo / link / text rác ({group_name})"
            return result

        if cls.contains_blacklisted_fragment(clean_code, group_config):
            result["reason"] = "Code contains a blacklisted fragment"
            return result

        site_identity = cls.detect_site_identity(clean_code)
        if not cls.is_site_allowed_for_group(site_identity, group_config):
            result["reason"] = f"🛡️ Code [{site_identity.upper()}] không thuộc nhóm lọc [{group_name}]"
            return result

        if cls.is_composed_of_common_words(clean_code):
            result["reason"] = "Code is composed of common words"
            return result

        if cls.is_text_word(clean_code):
            result["reason"] = "Code is a text word"
            return result

        if cls.is_sequential_code(clean_code):
            result["reason"] = "Code is sequential"
            return result

        if cls.looks_like_domain_or_link(clean_code):
            result["reason"] = "Code looks like a domain or link"
            return result

        if source in ("ocr", "marker"):
            clean_upper_ocr = clean_code.upper()
            for word in cls.COMMON_WORDS:
                if len(word) >= 4 and word in clean_upper_ocr:
                    result["is_fake"] = True
                    tag = "OCR" if source == "ocr" else "MARKER"
                    result["reason"] = f"🚫 [{tag}] Nghi rác — chứa từ '{word}': {clean_code}"
                    return result

            if _REPEATED_CHAR_RE.search(clean_code):
                result["is_fake"] = True
                tag = "OCR" if source == "ocr" else "MARKER"
                result["reason"] = (
                    f"🚫 [{tag}] Nghi rác — có ký tự lặp liên tiếp ≥3 lần: {clean_code}"
                )
                return result

        min_len = int(group_config.get("min_clean_length", getattr(Config, "CODE_MIN_LENGTH", 6)) or 6)
        max_len = int(group_config.get("max_clean_length", getattr(Config, "CODE_MAX_LENGTH", 15)) or 15)

        if len(clean_code) < min_len or len(clean_code) > max_len:
            result["reason"] = f"❌ Độ dài không hợp lệ: {len(clean_code)}"
            return result

        min_special_chars = 0

        if source not in ("spoiler", "marker", "ocr"):
            min_special_chars = int(group_config.get("min_special_chars", 0) or 0)

        if min_special_chars > 0 and special_count < min_special_chars:
            result["reason"] = f"🚫 Không đủ dấu đặc biệt: {special_count}/{min_special_chars} ({group_name})"
            return result

        if bool(group_config.get("require_uppercase", False)) and clean_code != clean_code.upper():
            result["reason"] = f"🚫 Mã không viết hoa đúng chuẩn ({group_name})"
            return result

        target_lower = normalize_code_domain(target_url)
        site_identity = cls.detect_site_identity(clean_code)

        if site_identity and target_lower and site_identity not in target_lower:
            result["reason"] = (
                f"🛡️ CHỐNG NHẬP SAI: Code [{site_identity.upper()}] "
                f"không thuộc trang [{target_url}]"
            )
            return result

        entropy = cls.calculate_entropy(clean_code)
        result["entropy"] = round(entropy, 2)

        min_entropy = float(group_config.get("min_entropy", 2.3))
        has_lower = any(c.islower() for c in clean_code)
        has_upper = any(c.isupper() for c in clean_code)
        has_digit = any(c.isdigit() for c in clean_code)

        if site_identity:
            result["valid"] = True
            result["confidence"] = 1.0
            result["reason"] = f"🌟 Code có định danh {site_identity.upper()} hợp lệ ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        if clean_code.isdigit() and bool(group_config.get("allow_numeric", True)) and entropy >= 2.6:
            result["valid"] = True
            result["confidence"] = 0.9
            result["reason"] = f"✅ Code hợp lệ dạng toàn số ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        if has_lower and has_upper and entropy >= min_entropy:
            result["valid"] = True
            result["confidence"] = 0.95
            result["reason"] = f"✅ Code hợp lệ dạng mix chữ hoa/thường ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        if has_digit and entropy >= min_entropy:
            result["valid"] = True
            result["confidence"] = 0.92
            result["reason"] = f"✅ Code hợp lệ dạng có số ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        if entropy >= float(group_config.get("uppercase_min_entropy", 2.9)):
            result["valid"] = True
            result["confidence"] = 0.85
            result["reason"] = f"✅ Code hợp lệ độ ngẫu nhiên cao ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        result["reason"] = f"🚫 Không đủ đặc điểm code thật ({group_name})"
        return result


# PATCH_MARKER_2026-10-28-validation-normalize-unified
# PATCH_MARKER_2026-10-26-v9-core-ocr-sqlite: cache values, never caller-owned dicts.
@_lru_cache(maxsize=8192)
def _validate_code_cached(
    cache_version: int,
    raw_code: str,
    target_url: str,
    filter_group_name,
    source: str,
) -> tuple:
    result = CodeValidator._validate_code_uncached(
        raw_code, target_url, filter_group_name, source
    )
    return tuple(result.items())


def validate_code_cached(raw_code: str, target_url: str = "", filter_group_name=None, source: str = "normal") -> dict:
    args = (
        _VALIDATOR_CACHE_VERSION,
        str(raw_code or ""),
        str(target_url or ""),
        filter_group_name,
        source,
    )
    try:
        hash(args)
    except TypeError:
        return CodeValidator._validate_code_uncached(*args[1:])
    # Rules are startup configuration; clear this cache if runtime rule reload is added.
    return dict(_validate_code_cached(*args))
