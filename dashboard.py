import threading
from collections import deque
from typing import Optional
from code_identity import normalize_code


_lock = threading.Lock()

_rows: deque = deque(maxlen=100)
_stats = {
    "success": 0,
    "failed": 0,
    "total": 0,
    "last_telegram_rtt_ms": None,
    "last_concurrent": 0,
    "last_concurrent_elapsed_ms": None,
    "server_rtts_ms": deque(maxlen=200),
    "timing_records": 0,
    "download_active": 0,
    "download_completed": 0,
    "download_dedup": 0,
    "ocr_scans": 0,
    "ocr_codes": 0,
    "ocr_no_code": 0,
    "ocr_errors": 0,
    "last_download_ms": None,
    "last_download_mb_s": None,
    "last_download_size_mb": None,
    "last_download_attempts": None,
}
_row_id = 0


def _fmt_ms(ms) -> str:

    if ms is None:
        return "-"
    try:
        return f"{float(ms):.0f}ms"
    except Exception:
        return "-"

def _format_latency(latency: Optional[dict]) -> str:

    if not latency:
        return "-"

    def _num(key):
        try:
            v = latency.get(key)
            return None if v is None else float(v)
        except (TypeError, ValueError):
            return None

    parts = []
    tab = _num("tab_wait_ms")
    if tab is not None and tab >= 100:
        parts.append(f"tab{tab:.0f}")
    gap = _num("pre_api_ms")
    if gap is not None and gap >= 100:
        parts.append(f"gap{gap:.0f}")
    api = _num("api_ms")
    if api is not None:
        parts.append(f"api{api:.0f}")
    else:
        res = _num("result_wait_ms")
        if res is not None:
            parts.append(f"res{res:.0f}")
    tot = _num("total_submit_ms")
    if tot is not None:
        parts.append(f"tot{tot:.0f}")
    return " ".join(parts) or "-"


def update_dashboard(
    domain: str,
    account: str,
    code: str,
    status: str,
    rtt_ms: Optional[float] = None,
    raw_response: str = "",
    telegram_rtt_ms: Optional[float] = None,
    latency_ms: Optional[dict] = None,
):

    global _row_id
    code = normalize_code(code, domain)
    # Keep counters and recent results for /status without rendering a UI.

    clean_raw = " ".join((raw_response or "").replace("\r", " ").replace("\n", " ").split())

    for noise in ("No popup Close", "popup Close", "Close"):
        if len(clean_raw) > len(noise) and noise in clean_raw:
            clean_raw = clean_raw.replace(noise, "").strip()

    raw = " ".join(clean_raw.split()) if clean_raw else "-"
    if len(raw) > 160:
        raw = raw[:160] + "..."

    def _bucket(value: str) -> str:
        upper = (value or "").upper()
        if "THÀNH CÔNG" in upper or "SUCCESS" in upper:
            return "success"
        if "THẤT BẠI" in upper or "FAILED" in upper:
            return "failed"
        return "other"

    with _lock:
        existing = next(
            (
                row for row in _rows
                if row["domain"] == domain
                and row["account"] == account
                and row["code"] == code
            ),
            None,
        )
        if existing is None:
            _row_id += 1
            _rows.append(
                {
                    "id": _row_id,
                    "domain": domain,
                    "account": account,
                    "code": code,
                    "status": status,
                    "rtt": _fmt_ms(rtt_ms),
                    "raw": raw,
                    "latency": _format_latency(latency_ms),
                }
            )
            _stats["total"] += 1
            old_bucket = "other"
        else:
            old_bucket = _bucket(existing.get("status", ""))
            existing.update(
                status=status,
                rtt=_fmt_ms(rtt_ms),
                raw=raw,
                latency=_format_latency(latency_ms),
            )

        new_bucket = _bucket(status)
        if old_bucket in ("success", "failed"):
            _stats[old_bucket] = max(0, _stats[old_bucket] - 1)
        if new_bucket in ("success", "failed"):
            _stats[new_bucket] += 1

        # server_rtts_ms chỉ nhận api_ms thật (xem update_latency), không nhận
        # tổng thời gian submit vì nó gồm cả thời gian chờ hết timeout + cleanup.
        if telegram_rtt_ms is not None:
            _stats["last_telegram_rtt_ms"] = telegram_rtt_ms


def update_latency(domain: str, account: str, code: str, latency_ms: Optional[dict]) -> None:
    code = normalize_code(code, domain)
    text = _format_latency(latency_ms)
    api_ms = None
    wait_ms = None
    try:
        if latency_ms and latency_ms.get("api_ms") is not None:
            api_ms = float(latency_ms["api_ms"])
        elif latency_ms and latency_ms.get("result_wait_ms") is not None:
            wait_ms = float(latency_ms["result_wait_ms"])
    except (TypeError, ValueError):
        api_ms = wait_ms = None
    with _lock:
        for row in reversed(_rows):
            if row["domain"] == domain and row["account"] == account and row["code"] == code:
                row["latency"] = text
                # RTT API thật = thời gian từ request tới phản hồi của website.
                # Nếu site chưa trả lời trong thời gian chờ thì hiện ">Nms".
                if api_ms is not None:
                    row["rtt"] = _fmt_ms(api_ms)
                elif wait_ms is not None:
                    row["rtt"] = ">" + _fmt_ms(wait_ms)
                break
        if api_ms is not None:
            _stats["server_rtts_ms"].append(api_ms)


def report_ocr_result(result: dict):
    with _lock:
        _stats["ocr_scans"] += 1
        if result.get("success") and result.get("codes"):
            _stats["ocr_codes"] += len(result["codes"])
        elif result.get("retryable", False):
            _stats["ocr_errors"] += 1
        else:
            _stats["ocr_no_code"] += 1


def report_download_started():
    with _lock:
        _stats["download_active"] += 1


def report_download_finished():
    with _lock:
        _stats["download_active"] = max(0, _stats["download_active"] - 1)


def report_timing_record(record: dict):

    with _lock:
        _stats["timing_records"] += 1
        if record.get("media"):
            _stats["download_completed"] += 1
            if record.get("download_dedup_hit"):
                _stats["download_dedup"] += 1
            elapsed = record.get("download_elapsed_ms")
            size = record.get("file_size_bytes")
            speed = record.get("download_bytes_per_sec")
            _stats["last_download_ms"] = float(elapsed) if elapsed is not None else None
            _stats["last_download_mb_s"] = float(speed) / 1_000_000 if speed is not None else None
            _stats["last_download_size_mb"] = float(size) / 1_000_000 if size is not None else None
            _stats["last_download_attempts"] = record.get("download_attempts")


def get_dashboard_snapshot() -> dict:

    with _lock:
        return {
            "success": _stats["success"],
            "failed": _stats["failed"],
            "total": _stats["total"],
            "download_active": _stats["download_active"],
            "download_completed": _stats["download_completed"],
            "rows_tracked": len(_rows),
        }
