
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass

try:
    import psutil
except Exception:
    psutil = None

from logger_setup import logger

_health_monitor = None
_perf_monitor = None
_admission_cpu = 0.0
_admission_available_mb = float("inf")
_admission_sample_at = 0.0
_admission_lock = threading.Lock()


def ocr_admission_paused() -> bool:
    """Use current CPU and available memory; stale health samples cannot stall OCR."""
    from config import Config
    if not getattr(Config, "PAUSE_OCR_ON_HIGH_CPU", True):
        return False
    global _admission_cpu, _admission_available_mb, _admission_sample_at
    threshold = min(85.0, max(1.0, float(getattr(Config, "OCR_CPU_THRESHOLD", 85))))
    snapshot = _health_monitor.snapshot() if _health_monitor is not None else None
    if psutil is None:
        return bool(snapshot is not None and (snapshot["pause_ocr"] or snapshot["cpu_percent"] > threshold))
    # Health snapshots refresh every 30s. Use the current one-second sample
    # for unconfirmed CPU spikes so an old reading cannot stall new OCR jobs.
    with _admission_lock:
        now = time.monotonic()
        if now - _admission_sample_at >= 1.0:
            _admission_cpu = float(psutil.cpu_percent(interval=None))
            _admission_available_mb = psutil.virtual_memory().available / (1024 * 1024)
            _admission_sample_at = now
        return (_admission_cpu > threshold or
                _admission_available_mb < float(getattr(Config, "OCR_RAM_MIN_AVAILABLE_MB", 768)))


class HealthMonitor:
    def __init__(
        self,
        cpu_threshold: int = 85,
        interval: float = 120.0,
        recovery_margin: float = 10.0,
        ram_threshold: int = 90,
        ram_warn_threshold: int = 85,
        breach_confirm_count: int = 2,
        ram_min_available_mb: int = 768,
    ):
        self.cpu_threshold = max(1, min(100, int(cpu_threshold)))

        self.interval = max(5.0, float(interval))

        self.breach_confirm_count = max(1, int(breach_confirm_count))
        self._consecutive_breaches = 0

        self.recovery_threshold = max(1, self.cpu_threshold - max(0, recovery_margin))
        self.ram_threshold = max(1, min(100, int(ram_threshold)))
        self.ram_warn_threshold = max(1, min(100, int(ram_warn_threshold)))
        self.ram_min_available_mb = max(256, int(ram_min_available_mb))
        self.last_available_mb = float("inf")
        self.ram_recovery_threshold = max(1, self.ram_threshold - max(0, recovery_margin))
        self._ram_warned_recently = False
        self.pause_ocr = False
        self.pause_reason = ""
        self.last_cpu = 0.0
        self.last_memory = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

        self.on_high_memory_callback = None
        self._last_high_memory_cleanup = 0.0
        self._high_memory_cleanup_cooldown = 60.0

    def set_high_memory_callback(self, callback):

        self.on_high_memory_callback = callback

    def _sample(self):
        if psutil is None:
            return 0.0, 0.0
        memory = psutil.virtual_memory()
        self.last_available_mb = float(memory.available) / (1024 * 1024)
        return float(psutil.cpu_percent(interval=None)), float(memory.percent)

    def _loop(self):
        if psutil is not None:
            psutil.cpu_percent(interval=None)
        while not self._stop.wait(self.interval):
            try:
                cpu, mem = self._sample()
                with self._lock:
                    self.last_cpu, self.last_memory = cpu, mem

                    cpu_over_raw = cpu >= self.cpu_threshold
                    ram_over_raw = (mem >= self.ram_threshold and
                                    self.last_available_mb < self.ram_min_available_mb)
                    cpu_recovered = cpu <= self.recovery_threshold
                    ram_recovered = (mem <= self.ram_recovery_threshold or
                                     self.last_available_mb >= self.ram_min_available_mb * 1.5)

                    if cpu_over_raw or ram_over_raw:
                        self._consecutive_breaches += 1
                    else:
                        self._consecutive_breaches = 0
                    confirmed = self._consecutive_breaches >= self.breach_confirm_count
                    cpu_over = cpu_over_raw and confirmed
                    ram_over = ram_over_raw and confirmed

                    if ram_over and self.on_high_memory_callback is not None:
                        now_ts = time.time()
                        if now_ts - self._last_high_memory_cleanup >= self._high_memory_cleanup_cooldown:
                            self._last_high_memory_cleanup = now_ts
                            try:
                                logger.warning("🧹 [Health] RAM cao → chạy dọn dẹp khẩn cấp...")
                                self.on_high_memory_callback()
                            except Exception as exc:
                                logger.debug(f"⚠️ high_memory_callback error: {exc}")

                    if not self.pause_ocr and (cpu_over or ram_over):
                        self.pause_ocr = True
                        self.pause_reason = "CPU" if cpu_over else "RAM"
                        if cpu_over and ram_over:
                            self.pause_reason = "CPU+RAM"
                        logger.warning(
                            "⚠️ [Health] OCR paused (lý do=%s | CPU %.1f%%, RAM %.1f%%)",
                            self.pause_reason, cpu, mem,
                        )
                    elif self.pause_ocr and cpu_recovered and ram_recovered:
                        self.pause_ocr = False
                        self.pause_reason = ""
                        logger.warning(
                            "✅ [Health] OCR resumed (CPU %.1f%%, RAM %.1f%%)", cpu, mem
                        )

                    if mem >= self.ram_warn_threshold and not ram_over:
                        if not self._ram_warned_recently:
                            logger.warning(
                                "⚠️ RAM cao: %.1f%% — cân nhắc giảm TAB_POOL_SIZE / "
                                "MAX_TAB_PER_DOMAIN_CAP / MAX_CONCURRENT_* trong .env "
                                "nếu tình trạng này lặp lại thường xuyên",
                                mem,
                            )
                            self._ram_warned_recently = True
                    elif mem < self.ram_warn_threshold - 3:

                        self._ram_warned_recently = False
            except Exception as exc:
                logger.debug("Health monitor error: %s", exc)

    def start(self):
        if self._thread and self._thread.is_alive():
            return self
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="health-monitor", daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "cpu_percent": self.last_cpu,
                "memory_percent": self.last_memory,
                "available_memory_mb": self.last_available_mb,
                "pause_ocr": self.pause_ocr,
                "pause_reason": self.pause_reason,
            }


@dataclass
class TaskMetric:
    name: str
    elapsed: float
    success: bool
    timestamp: float


class PerformanceMonitor:
    def __init__(self, max_samples: int = 500):
        self.samples = deque(maxlen=max_samples)
        self._lock = threading.Lock()

    def record_task(self, name: str, elapsed: float, success: bool):
        with self._lock:
            self.samples.append(TaskMetric(name, float(elapsed), bool(success), time.time()))

    def snapshot(self) -> dict:
        with self._lock:
            rows = list(self.samples)
        if not rows:
            return {"count": 0, "success_rate": 0.0, "avg_elapsed": 0.0}
        return {
            "count": len(rows),
            "success_rate": sum(r.success for r in rows) / len(rows),
            "avg_elapsed": sum(r.elapsed for r in rows) / len(rows),
        }


def init_monitoring(*, initialize_performance: bool = True):

    global _health_monitor, _perf_monitor
    if _health_monitor is None:
        from config import Config
        _health_monitor = HealthMonitor(
            cpu_threshold=getattr(Config, "OCR_CPU_THRESHOLD", 85),
            interval=getattr(Config, "HEALTH_CHECK_INTERVAL", 120.0),
            ram_threshold=getattr(Config, "OCR_RAM_PAUSE_THRESHOLD", 90),
            ram_warn_threshold=getattr(Config, "OCR_RAM_WARN_THRESHOLD", 85),

            breach_confirm_count=getattr(Config, "HEALTH_BREACH_CONFIRM_COUNT", 2),
            ram_min_available_mb=getattr(Config, "OCR_RAM_MIN_AVAILABLE_MB", 768),
        )
        if getattr(Config, "ENABLE_MONITORING", True):
            _health_monitor.start()
    if initialize_performance:
        init_performance_monitor()
    return _health_monitor, _perf_monitor


def init_performance_monitor() -> PerformanceMonitor:
    global _perf_monitor
    if _perf_monitor is None:
        _perf_monitor = PerformanceMonitor()
    return _perf_monitor


def stop_monitoring() -> None:

    if _health_monitor is not None:
        _health_monitor.stop()
