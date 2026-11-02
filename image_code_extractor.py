from __future__ import annotations

import asyncio
import gc
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import numpy as np
from PIL import Image

from config import Config
from logger_setup import logger

try:
    import cv2

    # PATCH_MARKER_2026-10-26-v9-core-ocr-sqlite: avoid native CPU oversubscription.
    cv2.setNumThreads(1)
    _CV2_AVAILABLE = True
except ImportError:
    cv2 = None
    _CV2_AVAILABLE = False


VIDEO_FORMATS = [".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v"]

# PATCH_MARKER_2026-10-28-ocr-panel-streaming

_OCR_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(1, int(getattr(Config, "MAX_CONCURRENT_OCR", 2))),
    thread_name_prefix="ocr-worker",
)
_OCR_EXECUTOR_STOPPED = False


def shutdown_ocr_executor(wait: bool = True, cancel_futures: bool = True) -> None:

    global _OCR_EXECUTOR_STOPPED
    if _OCR_EXECUTOR_STOPPED:
        return
    _OCR_EXECUTOR_STOPPED = True
    _OCR_EXECUTOR.shutdown(wait=wait, cancel_futures=cancel_futures)


_rapid_engine = None
_rapid_engine_lock = threading.Lock()
# RapidOCR's full detector stores a per-image preprocessor and mutable call
# options on the shared instance. Recognition of already cropped regions is
# read-only and continues to run in parallel outside this lock.
_rapid_detector_lock = threading.Lock()
_OCR_SUPPORTS_NO_CLS = bool(getattr(Config, "OCR_SKIP_CLS", True))


def _disable_no_cls() -> None:
    global _OCR_SUPPORTS_NO_CLS
    _OCR_SUPPORTS_NO_CLS = False


def _get_rapid_engine():

    global _rapid_engine
    if _rapid_engine is None:
        with _rapid_engine_lock:
            if _rapid_engine is None:
                from rapidocr import RapidOCR

                det_side = int(getattr(Config, "OCR_DET_LIMIT_SIDE", 640) or 0)
                thread_params = {
                    "EngineConfig.onnxruntime.intra_op_num_threads": 2,
                    "EngineConfig.onnxruntime.inter_op_num_threads": 1,
                }
                params = dict(thread_params)
                if det_side > 0:
                    params.update({
                        "Det.limit_side_len": det_side,
                        "Det.limit_type": "max",
                    })
                _rapid_engine = None
                try:
                    _rapid_engine = RapidOCR(params=params)
                    if det_side > 0:
                        logger.info(
                            "⚙️ RapidOCR Det.limit_type=max limit_side_len=%s", det_side
                        )
                except Exception as exc:
                    logger.warning(
                        "⚠️ RapidOCR không nhận params tối ưu (%s) — dùng mặc định", exc
                    )
                    if det_side > 0:
                        try:
                            _rapid_engine = RapidOCR(params=thread_params)
                        except Exception:
                            _rapid_engine = None
                if _rapid_engine is None:
                    _rapid_engine = RapidOCR()
                logger.info("✅ RapidOCR engine đã load (ONNXRuntime, CPU)")
    return _rapid_engine


def _resize_for_ocr(pil_img: Image.Image) -> Image.Image:

    max_side = max(320, int(getattr(Config, "OCR_MAX_IMAGE_SIDE", 1280)))
    width, height = pil_img.size
    current_side = max(width, height)
    if current_side <= max_side:
        return pil_img

    scale = max_side / current_side
    resized = pil_img.resize(
        (max(1, int(width * scale)), max(1, int(height * scale))),
        Image.Resampling.BILINEAR,
    )
    logger.debug(
        "🖼️ [OCR] resize preprocessing %sx%s → %sx%s",
        width,
        height,
        resized.width,
        resized.height,
    )
    return resized


class ImageCodeExtractor:

    def __init__(self):
        self.supported_formats = [".png", ".jpg", ".jpeg", ".bmp", ".gif", ".tiff", ".webp"]
        self._streaming_recognizer = None
        self._streaming_checked = False
        self._streaming_lock = threading.Lock()
        try:
            _get_rapid_engine()
            logger.info("✅ ImageCodeExtractor ready (RapidOCR/ONNXRuntime)")
        except Exception as e:
            logger.error(
                f"❌ Không load được RapidOCR! ({e})\n"
                f"   Chạy: pip install rapidocr onnxruntime --break-system-packages"
            )
            raise

    def _preprocess_opencv(self, pil_img: Image.Image) -> np.ndarray:
        """Tiền xử lý ảnh trực tiếp bằng OpenCV: khử nhiễu, tách watermark/nền và tăng cường chữ mờ."""
        if not _CV2_AVAILABLE:
            return np.asarray(pil_img.convert("RGB"))[:, :, ::-1].copy()
        
        # Chuyển PIL Image sang OpenCV BGR format
        opencv_img = cv2.cvtColor(np.asarray(pil_img.convert("RGB")), cv2.COLOR_RGB2BGR)
        
        # Chuyển sang ảnh xám
        gray = cv2.cvtColor(opencv_img, cv2.COLOR_BGR2GRAY)
        
        # Khử nhiễu hạt
        denoised = cv2.fastNlMeansDenoising(gray, h=30)
        
        # Sử dụng phép biến đổi Black-Hat để loại bỏ watermark/nền hoa văn rườm rà
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15))
        blackhat = cv2.morphologyEx(denoised, cv2.MORPH_BLACKHAT, kernel)
        enhanced = cv2.add(denoised, blackhat)
        
        # Nhị phân hóa Otsu giúp chữ mờ hiển thị rõ nét tuyệt đối so với nền
        _, thresh = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # Chuyển ngược lại thành 3 kênh BGR để tương thích hoàn toàn với đầu vào RapidOCR
        return cv2.cvtColor(thresh, cv2.COLOR_GRAY2BGR)

    def _run_rapid_ocr(self, pil_img: Image.Image, *, raise_on_error: bool = False,
                       return_readings: bool = False):

        try:
            engine = _get_rapid_engine()
            started = time.perf_counter()
            
            # Áp dụng tiền xử lý OpenCV tối ưu trực tiếp cho ảnh đầu vào
            arr = self._preprocess_opencv(pil_img)
            
            with _rapid_detector_lock:
                result = None
                if _OCR_SUPPORTS_NO_CLS:
                    try:
                        result = engine(arr, use_cls=False)
                    except TypeError:
                        _disable_no_cls()
                if result is None:
                    result = engine(arr)
            if bool(getattr(Config, "OCR_DEBUG_TIMING", False)):
                logger.info(
                    "⏱️ [OCR-INFER] %.1fms input=%sx%s",
                    (time.perf_counter() - started) * 1000.0,
                    pil_img.width,
                    pil_img.height,
                )
            txts = list(getattr(result, "txts", None) or [])
            scores = list(getattr(result, "scores", None) or [])
            boxes = getattr(result, "boxes", None)
            threshold = float(getattr(Config, "OCR_CONFIDENCE_THRESHOLD", 0.70))
            kept = []
            for idx, text in enumerate(txts):
                if not text or not str(text).strip():
                    continue

                if scores and idx < len(scores):
                    try:
                        if float(scores[idx]) < threshold:
                            continue
                    except (TypeError, ValueError):
                        pass
                if return_readings:
                    if boxes is None or idx >= len(boxes):
                        continue
                    points = np.asarray(boxes[idx], dtype=float)
                    kept.append({"text": str(text).strip(),
                                 "confidence": float(scores[idx]) if idx < len(scores) else 0.0,
                                 "center": (float(points[:, 0].mean()) / pil_img.width,
                                             float(points[:, 1].mean()) / pil_img.height),
                                  "bounds": (float(points[:, 0].min()) / pil_img.width,
                                             float(points[:, 1].min()) / pil_img.height,
                                             float(points[:, 0].max()) / pil_img.width,
                                             float(points[:, 1].max()) / pil_img.height)})
                else:
                    kept.append(str(text).strip())
            return kept if return_readings else "\n".join(kept)
        except Exception as e:
            logger.debug(f"⚠️ [OCR] RapidOCR inference lỗi: {e}")
            if raise_on_error:
                raise
            return [] if return_readings else ""

    def confirm_code_readings(self, image_path, readings: list[dict]) -> list[dict]:
        """Re-read only detected code boxes without another full-image detector."""
        if not readings:
            return []
        if isinstance(image_path, Image.Image):
            image = image_path.convert("RGB")
        else:
            with Image.open(image_path) as source:
                image = source.convert("RGB")
        crops = []
        try:
            for reading in readings:
                x0, y0, x1, y1 = reading["bounds"]
                pad = max(2, round((y1 - y0) * image.height * 0.15))
                crops.append(image.crop((max(0, int(x0 * image.width) - pad),
                                         max(0, int(y0 * image.height) - pad),
                                         min(image.width, int(x1 * image.width) + pad),
                                         min(image.height, int(y1 * image.height) + pad))))
            # Use the same bounded recognizer as panel detection. Confirmation
            # used to bypass its batch limit and silently shift partial results
            # onto the wrong source reading.
            confirmations = self.recognize_code_regions(
                [np.asarray(crop)[:, :, ::-1].copy() for crop in crops])
            return [{**reading, "confirmation_text": confirmations[i][0],
                     "confirmation_confidence": confirmations[i][1]}
                    for i, reading in enumerate(readings) if i < len(confirmations)]
        finally:
            for crop in crops:
                crop.close()
            image.close()

    def read_image_regions(self, image: Image.Image) -> list[dict]:
        """Detect text in memory; coordinates refer to the supplied image."""
        resized = _resize_for_ocr(image)
        try:
            return self._run_rapid_ocr(resized, raise_on_error=True, return_readings=True)
        finally:
            if resized is not image:
                resized.close()

    def get_streaming_recognizer(self):
        """Reuse the loaded recognizer session; unsupported engines use legacy OCR."""
        # PATCH_MARKER_2026-10-26-streaming-ctc: no extra native session/pool.
        with self._streaming_lock:
            if not self._streaming_checked:
                try:
                    from streaming_code_reader import CtcAlnumRecognizer
                    self._streaming_recognizer = CtcAlnumRecognizer.from_rapidocr(_get_rapid_engine())
                    logger.info("[OCR-STREAM] CTC adapter ready (shared ONNX session)")
                except (AttributeError, IndexError, TypeError, ValueError) as exc:
                    logger.warning("[OCR-STREAM] CTC adapter unavailable; using existing OCR: %s", exc)
                self._streaming_checked = True
            return self._streaming_recognizer

    def recognize_code_regions(self, bgr_images: list) -> list[tuple[str, float]]:
        """Batch existing regions, without invoking or mutating the detector."""
        if not bgr_images:
            return []
        engine = _get_rapid_engine()
        recognize = getattr(engine, "recognize_txt", None)
        results = []
        batch_size = max(1, min(16, int(getattr(Config, "OCR_RECOGNITION_BATCH_SIZE", 8))))
        for start in range(0, len(bgr_images), batch_size):
            batch = bgr_images[start:start + batch_size]
            if callable(recognize):
                result = recognize(batch)
                texts = list(getattr(result, "txts", None) or [])
                scores = list(getattr(result, "scores", None) or [])
                if len(texts) != len(batch) or len(scores) != len(batch):
                    raise RuntimeError("OCR recognizer returned an incomplete batch")
                results.extend((str(text).strip(), float(score)) for text, score in zip(texts, scores))
            else:
                for bgr in batch:
                    image = Image.fromarray(bgr[:, :, ::-1])
                    try:
                        readings = self._run_rapid_ocr(image, raise_on_error=True, return_readings=True)
                        results.append((readings[0]["text"], readings[0]["confidence"])
                                       if len(readings) == 1 else ("", 0.))
                    finally:
                        image.close()
        return results

    def confirm_video_readings(self, video_path, batches: list[dict], *, track_motion=False) -> list[dict]:
        """Confirm exact text on a nearby decoded frame, retaining one frame at a time."""
        if not batches or not _CV2_AVAILABLE:
            return []
        cap = cv2.VideoCapture(str(video_path))
        bgr = None
        try:
            if not cap.isOpened():
                raise RuntimeError("Cannot open video for OCR confirmation")
            fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
            count = float(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0)
            if 0 < count < 2:
                return []
            confirmed = []
            grouped = {}
            for batch in batches:
                frame = int(batch["source_frame"]) if "source_frame" in batch else round(float(batch["second"]) * fps)
                offset = max(1, round(fps * 0.15))
                other_frame = frame + offset if count <= 0 or frame + offset < count else frame - offset
                if other_frame < 0 or other_frame == frame:
                    continue
                grouped.setdefault(other_frame, []).extend(
                    (frame, reading) for reading in batch["readings"])
            # Many cells share a source frame. Decode its neighbor only once
            # and recognize their small crops together instead of repeatedly
            # seeking and converting the same full-resolution frame.
            for other_frame, items in grouped.items():
                cap.set(cv2.CAP_PROP_POS_FRAMES, other_frame)
                ok, bgr = cap.read()
                actual_frame = round(float(cap.get(cv2.CAP_PROP_POS_FRAMES) or 0.0)) - 1
                if not ok or actual_frame < 0:
                    continue
                height, width = bgr.shape[:2]
                if width > 640:
                    scale = 640.0 / width
                    bgr = cv2.resize(
                        bgr,
                        (640, max(1, round(height * scale))),
                        interpolation=cv2.INTER_AREA,
                    )
                readings = [reading for frame, reading in items if actual_frame != frame]
                if not readings:
                    bgr = None
                    continue
                image = Image.fromarray(cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB))
                try:
                    result = self.confirm_code_readings(image, readings)
                    if track_motion:
                        # A moving word need not occupy the identical box on
                        # the next frame. Re-detect near that box, accepting
                        # only the same exact case-sensitive string.
                        threshold = float(getattr(Config, "OCR_CONFIDENCE_THRESHOLD", 0.70))
                        for reading in result:
                            if (reading.get("confirmation_text") == reading["code"]
                                    and reading.get("confirmation_confidence", 0) >= threshold):
                                continue
                            x0, y0, x1, y1 = reading["bounds"]
                            dx = max(x1 - x0, 0.04)
                            dy = max(2 * (y1 - y0), 0.04)
                            search = image.crop((max(0, int((x0 - dx) * image.width)),
                                                 max(0, int((y0 - dy) * image.height)),
                                                 min(image.width, int((x1 + dx) * image.width)),
                                                 min(image.height, int((y1 + dy) * image.height))))
                            try:
                                matches = [r for r in self._run_rapid_ocr(
                                    search, raise_on_error=True, return_readings=True)
                                    if r["text"] == reading["code"] and r["confidence"] >= threshold]
                                if matches:
                                    reading["confirmation_text"] = reading["code"]
                                    reading["confirmation_confidence"] = max(r["confidence"] for r in matches)
                            finally:
                                search.close()
                    confirmed.extend({**reading, "confirmation_frame": actual_frame}
                                     for reading in result)
                finally:
                    image.close()
                    bgr = None
            return confirmed
        finally:
            cap.release()
            del bgr
            gc.collect()


_image_extractor = None


def init_image_extractor() -> ImageCodeExtractor:

    global _image_extractor
    if _image_extractor is None:
        try:
            _image_extractor = ImageCodeExtractor()
        except Exception as e:
            logger.error(f"❌ Cannot init image extractor: {e}")
            return None
    return _image_extractor


def get_image_extractor() -> ImageCodeExtractor:

    global _image_extractor
    if _image_extractor is None:
        _image_extractor = init_image_extractor()
    return _image_extractor


def warmup_image_extractor() -> bool:

    try:
        extractor = get_image_extractor()
        if extractor is None:
            return False
        warmup = Image.new("RGB", (64, 64), "white")
        try:
            extractor._run_rapid_ocr(warmup)
        finally:
            warmup.close()
        return True
    except Exception as exc:
        logger.warning("⚠️ OCR warm-up inference thất bại: %s", exc)
        return False
