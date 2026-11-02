"""Position-independent, bounded OCR for code posters and animated videos.

Find panels by their pixels, keep the clearest independent observations of
each appearance, then batch recognition of crops from bounded-width frames.
The neural text detector is a fallback for artwork without detectable panels.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import asyncio
import gc
import logging
import math
import re
import time

import cv2
# PATCH_MARKER_2026-10-26-v9-core-ocr-sqlite: share one-thread OpenCV policy.
cv2.setNumThreads(1)
import numpy as np
from PIL import Image, ImageOps

from config import Config
from code_identity import normalize_code, normalize_code_domain
from code_validator import CodeValidator
from scheduling import random_codes

logger = logging.getLogger("autobot." + __name__)

# OCR inference is memory intensive; only one task may hold the OCR slot.
OCR_SEMAPHORE = asyncio.Semaphore(1)


# PATCH_MARKER_2026-10-28-ocr-panel-streaming
POLICY_VERSION = "dynamic-regions-v12.3-mid-end-red-grid"
_NOISE = re.compile(
    r"SCATTER|OLYMPUS|ANUBIS|MAHJONG|AZTEC|OFAKIND|DCHOI.*PHTTI|"
    r"QUAY(?:MINPH|MIENPHI)|WINMULTIPLIER|BETVND|PROFITVND|BALANCEVND|"
    r"BET(?:SIZE|LEVEL)\d*|ROUND\d+|\d+WAYS", re.I)
_LABELS = frozenset(("THUMAY", "TOPWIN", "MEGALIVE", "GIFTCODE", "BONUS",
                     "PROMO", "PROMOTION", "KHUYENMAI", "WINNER",
                     "YOUTUBE", "FACEBOOK", "TELEGRAM", "TIKTOK"))
_XX8_X_Y, _XX8_X_X = np.indices((28, 20))
_XX8_X_DIAGONAL = ((np.abs(_XX8_X_X/19.-_XX8_X_Y/27.) < .25)
                  | (np.abs(_XX8_X_X/19.+_XX8_X_Y/27.-1.) < .25))


@dataclass(frozen=True)
class OCRProfile:
    domain: str
    lengths: tuple[int, ...]
    foreground: str
    max_codes: int

    @classmethod
    def for_site(cls, url, cfg):
        domain = normalize_code_domain(url)
        defaults = {
            "xx88code.com": ((int(getattr(Config, "XX88_OCR_CODE_LENGTH", 6)),), "red", 10),
            "getcodexx8bonus.pages.dev": ((10,), "white", 20),
            "llwincode.com": (tuple(range(6, 16)), "auto", 4),
        }
        lengths, foreground, limit = defaults.get(domain, ((), "auto", 4))
        configured_lengths = tuple(cfg.get("ocr_code_lengths") or lengths)
        if domain == "getcodexx8bonus.pages.dev":
            configured_lengths = (10,)
        return cls(domain, configured_lengths, foreground,
                   max(1, int(cfg.get("max_ocr_codes_per_batch", limit))))

    def candidate(self, text: str) -> bool:
        # Never repair case, join fragments or guess confusable characters.
        text = str(text or "").strip()
        if not text.isascii() or not text.isalnum():
            return False
        if self.lengths and len(text) not in self.lengths:
            return False
        if self.domain == "llwincode.com" and text != text.upper():
            return False
        if self.domain == "getcodexx8bonus.pages.dev" and not text.startswith("XX"):
            return False
        if text.upper() in _LABELS:
            return False
        code = normalize_code(text, self.domain)
        if not CodeValidator.validate_code(code, target_url=self.domain, source="image_ocr").get("valid"):
            return False
        return True


def _candidate_code(profile: OCRProfile, text: str) -> str | None:
    if not profile.candidate(text):
        return None
    return normalize_code(str(text).strip(), profile.domain)


@dataclass
class Observation:
    bounds: tuple[float, float, float, float]
    frame: int
    image: np.ndarray
    mask: np.ndarray | None
    quality: float
    prefix_anchored: bool = False
    signature: np.ndarray | None = field(default=None, init=False, repr=False, compare=False)
    edges: tuple[np.ndarray, np.ndarray] | None = field(default=None, init=False, repr=False, compare=False)


@dataclass
class Appearance:
    bounds: tuple[float, float, float, float]
    last_frame: int
    observations: list[Observation] = field(default_factory=list)
    stable: bool = False
    pending: Observation | None = field(default=None, repr=False)

    def add(self, obs):
        self.bounds, self.last_frame = obs.bounds, obs.frame
        self.observations.append(obs)
        # Only small crops are retained. Keep three alternatives to survive a
        # transition where one sampled frame still has a partly drawn word.
        self.observations.sort(key=lambda o: o.quality, reverse=True)
        del self.observations[3:]

    def observe(self, obs):
        """Split a persistent panel only after a replacement survives two samples."""
        if self.stable:
            if any(_same_artwork(old, obs, tolerate_edges=True) for old in self.observations):
                self.pending = None
                self.add(obs)
            elif (self.pending is not None and self.pending.frame != obs.frame
                  and _same_artwork(self.pending, obs, tolerate_edges=True)):
                replacement = Appearance(self.bounds, obs.frame, stable=True)
                replacement.add(self.pending)
                replacement.add(obs)
                self.pending = None
                return replacement
            else:
                # Do not let reveal effects or isolated noisy frames displace
                # the independently observed stable code in this panel.
                self.pending = obs
                self.last_frame = obs.frame
            return self
        matching = [old for old in self.observations
                    if old.frame != obs.frame and _same_artwork(old, obs, tolerate_edges=True)]
        if matching:
            self.stable = True
            self.observations = matching
        self.add(obs)
        return self


def _iou(a, b):
    overlap = max(0., min(a[2], b[2]) - max(a[0], b[0])) * max(0., min(a[3], b[3]) - max(a[1], b[1]))
    total = (a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - overlap
    return overlap / total if total > 0 else 0.


def _llwin_grid_cells(bgr):
    """Find repeated aligned blue, white or red cells; ignore isolated labels."""
    height, width = bgr.shape[:2]
    hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
    masks = (cv2.inRange(hsv, (95, 105, 60), (135, 255, 255)),
             cv2.inRange(hsv, (0, 0, 205), (179, 45, 255)),
             cv2.bitwise_or(cv2.inRange(hsv, (0, 105, 60), (10, 255, 255)),
                            cv2.inRange(hsv, (170, 105, 60), (179, 255, 255))))
    boxes = []
    for mask in masks:
        contours, _ = cv2.findContours(mask, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            if not (.07 < w / width < .35 and .018 < h / height < .10
                    and 1.3 < w / h < 8 and cv2.contourArea(contour) / (w * h) > .55):
                continue
            box = (x / width, y / height, (x+w) / width, (y+h) / height)
            if not any(_iou(box, old) > .8 for old in boxes):
                boxes.append(box)
    result = []
    for box in boxes:
        bw, bh = box[2]-box[0], box[3]-box[1]
        same_size = [b for b in boxes if abs((b[2]-b[0])-bw) < bw*.12
                     and abs((b[3]-b[1])-bh) < bh*.18]
        row = [b for b in same_size if abs(b[1]-box[1]) < bh*.25]
        other_rows = [b for b in same_size if bh*.7 < abs(b[1]-box[1]) < bh*8
                      and abs(b[0]-box[0]) < bw*.25]
        if len(row) >= 2 and other_rows:
            result.append(box)
    return result


def _llwin_in_grid(reading, cells):
    x0, y0, x1, y1 = reading["bounds"]
    cx, cy = (x0+x1)/2, (y0+y1)/2
    return any(a <= cx <= c and b <= cy <= d for a, b, c, d in cells)


def _strict_grid_cells(bgr):
    """Locate repeated code-cell grids anywhere; isolated labels/clocks fail closed."""
    scale = min(1., 960 / max(bgr.shape[:2]))
    small = cv2.resize(bgr, None, fx=scale, fy=scale) if scale < 1 else bgr
    cells = []
    for box in _llwin_grid_cells(small):
        if not any(_iou(box, old) > .65 for old in cells):
            cells.append(box)
    cells = cells[:128]
    # Ornate cells may expose both the outer badge frame and an inner text
    # panel. OCR only the inner panel, otherwise "CODE" joins the real code.
    def encloses(outer, inner):
        ow, oh = outer[2]-outer[0], outer[3]-outer[1]
        iw, ih = inner[2]-inner[0], inner[3]-inner[1]
        overlap = max(0., min(outer[2], inner[2])-max(outer[0], inner[0])) * max(
            0., min(outer[3], inner[3])-max(outer[1], inner[1]))
        return .30 < iw*ih/(ow*oh) < .85 and overlap/(iw*ih) >= .95
    cells = [box for box in cells if not any(encloses(box, inner) for inner in cells
                                            if inner != box)]
    neighbors = [set() for _ in cells]
    for i, a in enumerate(cells):
        aw, ah = a[2]-a[0], a[3]-a[1]
        for j in range(i):
            b = cells[j]
            bw, bh = b[2]-b[0], b[3]-b[1]
            if abs(aw-bw) > max(aw, bw)*.18 or abs(ah-bh) > max(ah, bh)*.20:
                continue
            dx = abs((a[0]+a[2]-b[0]-b[2])/2)
            dy = abs((a[1]+a[3]-b[1]-b[3])/2)
            row = dy < max(ah, bh)*.35 and dx < max(aw, bw)*1.8
            column = dx < max(aw, bw)*.35 and dy < max(ah, bh)*2.0
            if row or column:
                neighbors[i].add(j)
                neighbors[j].add(i)
    kept, visited = [], set()
    for first in range(len(cells)):
        if first in visited:
            continue
        component, pending = set(), [first]
        while pending:
            index = pending.pop()
            if index in component:
                continue
            component.add(index)
            pending.extend(neighbors[index]-component)
        visited.update(component)
        if len(component) >= 6:
            kept.extend(cells[index] for index in sorted(component))
    return kept


def _anchor_frame_indices(frame_count, fps):
    if frame_count < 1 or not math.isfinite(fps) or fps <= 0:
        raise ValueError("Cannot determine video duration for anchor OCR")
    duration = frame_count / fps
    return list(dict.fromkeys(
        min(frame_count-1, max(0, round(seconds*fps)))
        for seconds in (max(0., duration/2-1.), max(0., duration-1.))
    ))


def _anchor_candidate(profile, reading):
    text, score = reading
    raw = str(text or "").strip()
    try:
        score = float(score)
    except (TypeError, ValueError):
        return None
    if (not math.isfinite(score) or score > 1.
            or score < max(.90, float(getattr(Config, "OCR_CONFIDENCE_THRESHOLD", .70)))
            or not raw.isascii() or not raw.isalnum()
            or not any(char.isalpha() for char in raw)
            or _NOISE.search(raw)
            or re.fullmatch(r"(?:\d{1,2}H\d{0,2}){1,2}", raw, re.I)
            or raw.upper() in {"USDT", "VOUCHER", "APPLLWIN", "LLWIN", "XX88", "DOWNLOAD"}):
        return None
    return _candidate_code(profile, raw)


def _anchor_cell_readings(bgr, cells, extractor, metrics):
    height, width = bgr.shape[:2]
    boxes, crops = [], []
    for box in cells:
        x0, y0, x1, y1 = (round(box[0]*width), round(box[1]*height),
                         round(box[2]*width), round(box[3]*height))
        px, py = max(2, round((x1-x0)*.035)), max(2, round((y1-y0)*.12))
        crop = bgr[y0+py:y1-py, x0+px:x1-px]
        if crop.size:
            boxes.append(box)
            crops.append(crop.copy())
    if not crops:
        return []
    
    # OCR chỉ chạy 1 lần duy nhất thay vì 2 lần để tăng gấp đôi tốc độ và chống timeout
    readings = extractor.recognize_code_regions(crops)
    if len(readings) != len(boxes):
        raise RuntimeError("Anchor OCR returned an incomplete cell batch")
    metrics["recognized_regions"] += len(crops)
    
    checked = []
    for box, (text, score) in zip(boxes, readings):
        raw_text = str(text).strip()
        try:
            sc = float(score)
        except (TypeError, ValueError):
            sc = 0.0
            
        if math.isfinite(sc) and sc >= 0.90 and raw_text.isascii() and raw_text.isalnum() and 6 <= len(raw_text) <= 15:
            checked.append((box, (raw_text, sc)))
        else:
            checked.append((box, ("", 0.)))
            
    return checked


def _extract_anchor_video(path, extractor, profile, cfg, on_code, metrics, started):
    """Two requested snapshots, each verified against its own adjacent frame."""
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        cap.release()
        raise RuntimeError("Cannot decode video")
    found = {}
    try:
        fps = float(cap.get(cv2.CAP_PROP_FPS))
        count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        indices = _anchor_frame_indices(count, fps)
        metrics.update(sampling="midpoint-minus-1,end-minus-1", video_frames=count,
                       duration_seconds=round(count/fps, 4), anchor_details=[],
                       confirmation_offset_seconds=.10, min_confidence=.90)

        def read_at(index):
            cap.set(cv2.CAP_PROP_POS_FRAMES, index)
            ok, image = cap.read()
            if not ok:
                raise RuntimeError(f"Cannot decode requested video frame {index}")
            actual = round(cap.get(cv2.CAP_PROP_POS_FRAMES))-1
            if actual != index:
                raise RuntimeError(f"Video seek mismatch: requested {index}, decoded {actual}")
            metrics["sampled_frames"] += 1
            return image

        for index in indices:
            main = read_at(index)
            cells = _strict_grid_cells(main)
            detail = {"frame": index, "seconds": round(index/fps, 4),
                      "grid_cells": len(cells), "confirmed_codes": 0}
            metrics["anchor_details"].append(detail)
            if not cells or count < 2:
                detail["rejected"] = "no_trusted_grid" if not cells else "no_independent_frame"
                continue
            offset = max(1, round(fps*.10))
            other_index = min(count-1, index+offset)
            if other_index == index:
                other_index = max(0, index-offset)
            other = read_at(other_index)
            other_cells = _strict_grid_cells(other)
            detail.update(confirmation_frame=other_index,
                          confirmation_seconds=round(other_index/fps, 4),
                          confirmation_grid_cells=len(other_cells))
            if not other_cells:
                detail["rejected"] = "confirmation_grid_missing"
                continue
            main_readings = _anchor_cell_readings(main, cells, extractor, metrics)
            other_readings = _anchor_cell_readings(other, other_cells, extractor, metrics)
            for box, reading in main_readings:
                code = _anchor_candidate(profile, reading)
                if not code:
                    continue
                matching = [(other_box, vote) for other_box, vote in other_readings
                            if _iou(box, other_box) >= .60]
                if (len(matching) != 1 or _anchor_candidate(profile, matching[0][1]) != code
                        or str(reading[0]).strip() != str(matching[0][1][0]).strip()):
                    continue
                confidence = min(float(reading[1]), float(matching[0][1][1]))
                old = found.get(code)
                if old is None or confidence > old["confidence"]:
                    found[code] = {"code": code, "raw": code, "confidence": confidence}
                detail["confirmed_codes"] += 1
    finally:
        cap.release()
    codes = random_codes(found.values(), 1 if cfg.get("ocr_single_code_mode") else profile.max_codes)
    emitted = []
    if on_code is not None and getattr(Config, "OCR_VIDEO_EARLY_EMIT_ENABLED", True):
        for item in codes:
            try:
                on_code(dict(item))
                emitted.append(item["code"])
            except Exception:
                metrics["early_emit_errors"] = metrics.get("early_emit_errors", 0) + 1
    metrics.update(appearances=len(found), early_emitted=len(emitted),
                   elapsed_ms=round((time.perf_counter()-started)*1000, 1))
    return {"success": bool(codes), "codes": codes,
            "message": f"{len(codes)} codes" if codes else "no verified anchor code",
            "text": "\n".join(item["code"] for item in codes), "retryable": False,
            "failure_kind": None if codes else "NO_CODE", "ocr_metrics": metrics,
            "early_emitted": emitted}


def find_code_panels(bgr, profile):
    """Find light/colored rectangular panels anywhere, in either orientation."""
    if profile.domain == "llwincode.com":
        return _llwin_grid_cells(bgr)
    height, width = bgr.shape[:2]
    side = (max(320, int(getattr(Config, "XX8_OCR_PANEL_SIDE", 640)))
            if profile.domain == "getcodexx8bonus.pages.dev" else 960)
    scale = min(1., side / max(width, height))
    small = cv2.resize(bgr, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA) if scale < 1 else bgr
    hsv = cv2.cvtColor(small, cv2.COLOR_BGR2HSV)
    masks = [cv2.inRange(hsv, (0, 0, 205), (179, 35, 255))]
    if profile.foreground == "white":
        masks.append(cv2.inRange(hsv, (140, 90, 100), (179, 255, 255)))
        if profile.domain == "getcodexx8bonus.pages.dev":
            # New XX8 artwork has white cells outlined in red. A bright glow
            # can split their white interior, but the outer border stays closed.
            masks.append(cv2.bitwise_or(
                cv2.inRange(hsv, (0, 95, 70), (12, 255, 255)),
                cv2.inRange(hsv, (170, 95, 70), (179, 255, 255))))
    if profile.foreground == "dark":
        masks.append(cv2.inRange(hsv, (30, 35, 130), (95, 210, 255)))
    boxes = []
    if profile.domain == "xx88code.com":
        # Repeated blue code cells have mostly blue interiors. Do not classify
        # the blue clock banners or blue outlines around white cells as codes.
        for box in _llwin_grid_cells(small):
            x0, y0, x1, y1 = (round(box[0]*width), round(box[1]*height),
                              round(box[2]*width), round(box[3]*height))
            px, py = max(2, round((x1-x0)*.035)), max(2, round((y1-y0)*.15))
            interior = bgr[y0+py:y1-py, x0+px:x1-px]
            if not interior.size:
                continue
            color = cv2.cvtColor(interior, cv2.COLOR_BGR2HSV)
            blue = cv2.inRange(color, (95, 70, 40), (140, 255, 255))
            yellow = cv2.inRange(color, (15, 80, 100), (40, 255, 255))
            if (cv2.countNonZero(blue) > blue.size * .70
                    and cv2.countNonZero(yellow) < yellow.size * .04):
                boxes.append(box)
    sh, sw = small.shape[:2]
    for mask in masks:
        # Closing can bridge neighboring white cells through a pale border.
        # Contours of the original mask keep each code cell separate.
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            if (w < 36 * scale or h < max(8, 12 * scale) or not 1.8 < w / h < 20
                    or w > sw * .94 or h > sh * .80
                    or cv2.contourArea(contour) / (w * h) < .52):
                continue
            box = (x / sw, y / sh, (x+w) / sw, (y+h) / sh)
            if not any(_iou(box, old) > .7 for old in boxes):
                boxes.append(box)
    return sorted(boxes, key=lambda b: (round(b[1], 2), b[0]))[:64]


def _xx8_anchor_components(mask, components):
    """Locate the two X glyphs by pixels, without running another OCR model.

    Tight XX kerning can join both letters into one connected component.
    Use their diagonal strokes as an anchor, then retain the whole text row.
    The recognizer still checks the exact prefix/case; this never invents text.
    """
    def is_x(glyph):
        height, width = glyph.shape
        if height < 5 or not .25 <= width / height <= 1.4:
            return False
        small = cv2.resize(glyph, (20, 28), interpolation=cv2.INTER_AREA) > 80
        # Fixed small arrays, independent of source image dimensions.
        return np.count_nonzero(small & _XX8_X_DIAGONAL) / max(1, np.count_nonzero(small)) >= .85

    ordered = sorted(components, key=lambda component: component[0])
    anchors = []
    for index, component in enumerate(ordered):
        x, y, width, height = (int(v) for v in component[:4])
        glyph = mask[y:y+height, x:x+width]
        # XX can also touch the third character (e.g. XXA). Check the prefix
        # width independently; never split/truncate the text sent to OCR.
        for letter_width in range(max(2, round(height*.55)), min(width//2, round(height*1.1))+1):
            if is_x(glyph[:, :letter_width]) and is_x(glyph[:, letter_width:2*letter_width]):
                anchors.append(component)
                break
        if index + 1 < len(ordered) and is_x(glyph):
            other = ordered[index+1]
            ox, oy, ow, oh = (int(v) for v in other[:4])
            if (0 <= ox-x-width <= height*.65 and abs(oy-y) <= height*.3
                    and .7 <= oh/height <= 1.3
                    and is_x(mask[oy:oy+oh, ox:ox+ow])):
                anchors.append(component)
    for anchor in anchors:
        ax, ay, aw, ah = (int(v) for v in anchor[:4])
        row = [component for component in ordered
               if abs(int(component[1]+component[3])-ay-ah) <= ah*.5
               and int(component[3]) >= ah*.4]
        # An XX in the middle of a longer word is not the start of a code.
        if any(int(component[0]) < ax-ah*.25 for component in row):
            continue
        kept, right = [], ax
        for component in row:
            if int(component[0]) < ax:
                continue
            if int(component[0]) - right > ah:
                break
            kept.append(component)
            right = int(component[0]+component[2])
        if kept:
            return kept
    return []


def _xx8_text_mask(crop, hsv):
    """XX8 uses both white letters on pink and red/dark letters on white."""
    white = cv2.inRange(hsv, (0, 0, 175), (179, 105, 255))
    height, width = white.shape
    py, px = max(1, height//8), max(1, width//10)
    interior = white[py:height-py, px:width-px]
    if interior.size and cv2.countNonZero(interior) / interior.size > .55:
        red = cv2.bitwise_or(cv2.inRange(hsv, (0, 85, 45), (15, 255, 255)),
                            cv2.inRange(hsv, (160, 85, 45), (179, 255, 255)))
        dark = cv2.inRange(hsv, (0, 0, 0), (179, 255, 135))
        return cv2.bitwise_or(red, dark)
    return white


def _glyph_crop(bgr, box, profile, frame):
    height, width = bgr.shape[:2]
    x0, y0, x1, y1 = (round(box[0]*width), round(box[1]*height),
                      round(box[2]*width), round(box[3]*height))
    # Exclude the panel border while retaining words close to its sides.
    px, py = max(2, round((x1-x0)*.035)), max(2, round((y1-y0)*.15))
    x0, x1, y0, y1 = x0+px, x1-px, y0+py, y1-py
    crop = bgr[y0:y1, x0:x1]
    if crop.size == 0:
        return None
    hsv = cv2.cvtColor(crop, cv2.COLOR_BGR2HSV)
    if profile.domain == "getcodexx8bonus.pages.dev":
        mask = _xx8_text_mask(crop, hsv)
    elif (profile.domain in ("xx88code.com", "llwincode.com")
          and cv2.countNonZero(cv2.inRange(hsv, (95, 70, 40), (140, 255, 255)))
          > hsv.shape[0] * hsv.shape[1] * (.70 if profile.domain == "xx88code.com" else .5)):
        # Blue grid artwork carries white text; preserve red/dark text handling
        # for other panels instead of running the full-image detector for all.
        mask = cv2.inRange(hsv, (0, 0, 175), (179, 105, 255))
    elif profile.foreground == "white":
        mask = cv2.inRange(hsv, (0, 0, 175), (179, 105, 255))
    elif profile.foreground == "red":
        mask = cv2.bitwise_or(cv2.inRange(hsv, (0, 95, 70), (12, 255, 255)),
                             cv2.inRange(hsv, (166, 95, 70), (179, 255, 255)))
    elif profile.foreground == "color":
        mask = cv2.inRange(hsv, (0, 85, 55), (179, 255, 255))
    else:
        mask = cv2.inRange(hsv, (0, 0, 0), (179, 255, 115))
    original_mask = mask.copy() if profile.foreground in ("color", "dark") else None
    if profile.foreground in ("color", "dark"):
        # Remove long, thin strike-through strokes by geometry; preserve the
        # original crop for a separate reading as well.
        lines = cv2.morphologyEx(mask, cv2.MORPH_OPEN,
                                 np.ones((1, max(30, crop.shape[1]//3)), np.uint8))
        mask = cv2.subtract(mask, lines)
    if original_mask is not None:
        points = cv2.findNonZero(original_mask)
        if points is None or cv2.countNonZero(mask) < 40:
            return None
        gx0, gy0, gw, gh = cv2.boundingRect(points)
        if gw < gh*1.8:
            return None
        pad = max(2, round(gh*.12))
        gx1, gy1 = min(crop.shape[1], gx0+gw+pad), min(crop.shape[0], gy0+gh+pad)
        gx0, gy0 = max(0,gx0-pad), max(0,gy0-pad)
        return Observation(((x0+gx0)/width, (y0+gy0)/height,
                            (x0+gx1)/width, (y0+gy1)/height), frame,
                           crop[gy0:gy1,gx0:gx1].copy(), mask[gy0:gy1,gx0:gx1].copy(),
                           float(cv2.countNonZero(mask)))
    _, _, stats, _ = cv2.connectedComponentsWithStats(mask)
    components = [s for s in stats[1:] if s[cv2.CC_STAT_AREA] >= 4
                  and s[cv2.CC_STAT_HEIGHT] >= max(5, crop.shape[0]*.18)
                  and s[cv2.CC_STAT_WIDTH] < crop.shape[1]*.75]
    if not components:
        return None
    max_h = max(s[cv2.CC_STAT_HEIGHT] for s in components)
    components = [s for s in components if s[cv2.CC_STAT_HEIGHT] >= max_h*.48]
    anchored = False
    if profile.domain == "getcodexx8bonus.pages.dev":
        anchor_components = _xx8_anchor_components(mask, components)
        if anchor_components:
            components, anchored = anchor_components, True
        # A pixel template is only a hint: narrow/new fonts and glow may not
        # match it. Keep plausible ink for exact-prefix OCR instead of dropping
        # every region when the artwork changes.
    if len(components) < 4:
        return None
    gx0 = min(s[0] for s in components)
    gy0 = min(s[1] for s in components)
    gx1 = max(s[0]+s[2] for s in components)
    gy1 = max(s[1]+s[3] for s in components)
    if gx1-gx0 < (gy1-gy0)*1.8:
        return None
    pad = max(2, round((gy1-gy0)*.12))
    gx0, gy0 = max(0, gx0-pad), max(0, gy0-pad)
    gx1, gy1 = min(crop.shape[1], gx1+pad), min(crop.shape[0], gy1+pad)
    glyph = crop[gy0:gy1, gx0:gx1].copy()
    foreground = mask[gy0:gy1, gx0:gx1].copy()
    quality = float(cv2.countNonZero(foreground))
    bounds = ((x0+gx0)/width, (y0+gy0)/height, (x0+gx1)/width, (y0+gy1)/height)
    return Observation(bounds, frame, glyph, foreground, quality, prefix_anchored=anchored)


def _same_artwork(a, b, *, tolerate_edges=False):
    if _iou(a.bounds, b.bounds) < .5 or abs(a.image.shape[1]/a.image.shape[0] - b.image.shape[1]/b.image.shape[0]) > .4:
        return False
    for obs in (a, b):
        if obs.signature is None:
            obs.signature = cv2.resize(obs.mask, (96, 24), interpolation=cv2.INTER_AREA).astype(np.int16)
    # Keep this conservative: two real codes can differ by just one glyph.
    if float(np.mean(np.abs(a.signature-b.signature))) < 2.:
        return True
    if not tolerate_edges:
        return False
    # Compression may shift an edge by one signature pixel. This tolerance is
    # only for tracking; final repeated-artwork deduplication stays exact.
    for obs in (a, b):
        if obs.edges is None:
            mask = obs.signature >= 128
            obs.edges = (mask, cv2.dilate(mask.astype(np.uint8), np.ones((3, 3), np.uint8)) != 0)
    ma, da = a.edges
    mb, db = b.edges
    return not np.any(ma & ~db) and not np.any(mb & ~da)


def _early_stop_samples(profile):
    """Số khung mẫu "yên" liên tiếp để dừng quét video sớm; 0 = tắt (mặc định).

    Chỉ áp cho domain trong OCR_VIDEO_EARLY_STOP_DOMAINS và chỉ sau khi đã thấy ít nhất
    một vùng mã ổn định (hai khung độc lập khớp). Chọn giá trị dựa trên metrics
    max_quiet_gap ghi trong dòng [OCR-DYNAMIC]: phải lớn hơn khoảng lặng dài nhất
    mà sau đó vẫn còn mã mới.
    """
    domains = tuple(getattr(Config, "OCR_VIDEO_EARLY_STOP_DOMAINS", ()) or ())
    if profile.domain not in domains:
        return 0
    try:
        return max(0, int(getattr(Config, "OCR_VIDEO_EARLY_STOP_SAMPLES", 0) or 0))
    except (TypeError, ValueError):
        return 0


def _early_emit_allowed(profile, cfg):
    """Có được phát mã ngay khi một vùng mã ổn định (hai khung độc lập khớp) hay không.

    Chỉ áp cho domain trong OCR_VIDEO_EARLY_EMIT_DOMAINS (mặc định xx88code.com) và chỉ khi
    người gọi truyền on_code. XX8 có đường đọc khung thứ ba riêng nên giữ nguyên.
    """
    if not getattr(Config, "OCR_VIDEO_EARLY_EMIT_ENABLED", True):
        return False
    if profile.domain == "getcodexx8bonus.pages.dev":
        return False
    domains = tuple(getattr(Config, "OCR_VIDEO_EARLY_EMIT_DOMAINS", ()) or ())
    return profile.domain in domains


def _early_emit_confidence(profile):
    if profile.domain in (
        "xx88code.com",
        "getcodexx8bonus.pages.dev",
        "llwincode.com",
    ):
        return 0.70
    return 0.85


def _coarse_factor(profile):
    """Hệ số giãn bước lấy mẫu khi khung hiện tại không có vùng mã; 1 = tắt (mặc định).

    Khung không có vùng mã thì bước tới là step*factor (thưa); thấy vùng mã thì về lại step (dày).
    Đổi thang đo của max_quiet_gap, nên đo lại sau khi bật.
    """
    domains = tuple(getattr(Config, "OCR_VIDEO_COARSE_DOMAINS", ()) or ())
    if profile.domain not in domains:
        return 1
    try:
        return max(1, min(4, int(getattr(Config, "OCR_VIDEO_COARSE_FACTOR", 1) or 1)))
    except (TypeError, ValueError):
        return 1


def _video_sample_limits(path, cap, metrics):
    fps = float(cap.get(cv2.CAP_PROP_FPS) or 25.)
    if not np.isfinite(fps) or fps <= 0:
        fps = 25.
    total_frames = max(0, int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0))
    domain_limits = {
        "xx88code.com": (
            getattr(Config, "XX88_VIDEO_MAX_DURATION_SEC", 12.0),
            getattr(Config, "XX88_VIDEO_MAX_SAMPLES", 20),
        ),
        "getcodexx8bonus.pages.dev": (
            getattr(Config, "XX8_VIDEO_MAX_DURATION_SEC", 12.0),
            getattr(Config, "XX8_VIDEO_MAX_SAMPLES", 20),
        ),
        "llwincode.com": (
            getattr(Config, "LLWIN_VIDEO_MAX_DURATION_SEC", 12.0),
            getattr(Config, "LLWIN_VIDEO_MAX_SAMPLES", 20),
        ),
    }
    duration_limit, sample_limit = domain_limits.get(
        str(metrics.get("domain", "") or "").lower(),
        (
            getattr(Config, "OCR_VIDEO_MAX_DURATION_SEC", 7.0),
            getattr(Config, "OCR_VIDEO_MAX_SAMPLES", 10),
        ),
    )
    duration = max(0.1, float(duration_limit))
    frame_limit = max(1, math.ceil(fps * duration))
    frame_count = min(total_frames, frame_limit) if total_frames else frame_limit
    sample_limit = max(1, int(sample_limit))
    try:
        if (Path(path).stat().st_size > 20 * 1024 * 1024
                and str(metrics.get("domain", "") or "").lower()
                not in ("xx88code.com", "getcodexx8bonus.pages.dev", "llwincode.com")):
            sample_limit = min(sample_limit, 5)
    except OSError:
        pass
    metrics.update(video_frames=frame_count, duration_seconds=round(frame_count / fps, 3),
                   sample_limit=sample_limit, duration_limit_seconds=duration)
    return fps, frame_count, sample_limit


def _resize_frame_for_ocr(frame):
    height, width = frame.shape[:2]
    if width <= 640:
        return frame
    scale = 640.0 / width
    return cv2.resize(
        frame,
        (640, max(1, round(height * scale))),
        interpolation=cv2.INTER_AREA,
    )


def _collect_video(path, profile, metrics, emit=None):
    """Quét video trong giới hạn thời lượng và số mẫu theo domain.
    Early emission only receives stable tracks for consecutive-sample checks.

    PATCH_MARKER_2026-10-25-v4-poll-ocr: các khung mẫu được xử lý song song
    theo lô (tìm panel + cắt glyph) khi coarse=1 và OCR_VIDEO_PANEL_WORKERS>1.
    Bước lấy mẫu lúc đó cố định (=step) nên vị trí các khung trong lô không
    phụ thuộc kết quả khung trước; thứ tự lắp track vẫn giữ nguyên thứ tự
    khung (executor.map giữ thứ tự), nên kết quả track giống hệt chạy tuần
    tự. Khi coarse>1 hoặc workers=1 thì chạy đúng vòng lặp tuần tự cũ.
    """
    from concurrent.futures import ThreadPoolExecutor

    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        cap.release()
        raise RuntimeError("Cannot decode video")
    tracks, fallback = [], []
    frame = None
    batch = []
    results = []
    try:
        fps, count, limit = _video_sample_limits(path, cap, metrics)
        step = max(1, round(fps*float(getattr(Config, "OCR_VIDEO_SAMPLE_SECONDS", .40))))
        if count > 0:
            step = max(step, (count+limit-1)//limit)
        metrics["sample_step_frames"] = step
        fallback_marks = {max(0, round((count-1)*fraction / step)*step)
                          for fraction in (.08, .25, .42, .60, .78, .94)} if count > 0 else set()
        frame_no = 0
        early_samples = _early_stop_samples(profile)
        coarse = _coarse_factor(profile)
        workers = max(1, min(8, int(getattr(Config, "OCR_VIDEO_PANEL_WORKERS", 4) or 1)))
        parallel = workers > 1 and coarse == 1
        state = {"idle": 0, "max_quiet": 0, "progress_at": 0, "seen_stable": False}

        def process_frame(item):
            """Tìm panel + cắt glyph cho một khung. Chỉ đọc frame của mình,
            không chạm metrics/track nên chạy song song an toàn."""
            fno, frame = item
            frame = _resize_frame_for_ocr(frame)
            panels = find_code_panels(frame, profile)
            pairs = []
            for panel in panels:
                pairs.append((panel, _glyph_crop(frame, panel, profile, fno)))
            return panels, pairs

        def assemble(fno, frame, panels, pairs):
            """Lắp các quan sát của một khung vào track, đúng thứ tự khung.
            Trả (gap, stop): gap cho khung kế tiếp, stop=True nếu dừng sớm."""
            progressed = unsettled = False
            emittable = []
            if profile.domain == "getcodexx8bonus.pages.dev":
                metrics["xx8_panels_checked"] = metrics.get("xx8_panels_checked", 0) + len(panels)
            for panel, obs in pairs:
                if obs is None:
                    if profile.domain == "getcodexx8bonus.pages.dev":
                        metrics["xx8_panels_skipped"] = metrics.get("xx8_panels_skipped", 0) + 1
                    continue
                if profile.domain == "getcodexx8bonus.pages.dev":
                    name = "xx8_prefix_anchored" if obs.prefix_anchored else "xx8_ink_fallback"
                    metrics[name] = metrics.get(name, 0) + 1
                # Panel matching uses the full panel, not the growing word.
                track = next((t for t in reversed(tracks)
                              if fno-t.last_frame <= step*(coarse+1)
                              and _iou(t.bounds, panel) > .45), None)
                new_track = track is None
                if new_track:
                    track = Appearance(panel, fno)
                    tracks.append(track)
                    progressed = True
                was_stable = track.stable
                newly_stable = False
                replacement = track.observe(obs)
                if replacement is not track:
                    tracks.append(replacement)
                    track = replacement
                    progressed = newly_stable = True
                elif track.stable and not was_stable:
                    progressed = newly_stable = True
                if track.stable:
                    state["seen_stable"] = True
                if not track.stable or track.pending is not None:
                    unsettled = True
                track.bounds = panel
                if emit is not None and track.stable:
                    emittable.append(track)
            for track in random_codes(emittable):
                emit(track)
            if (fno in fallback_marks or count <= 0 and not panels) and len(fallback) < 6:
                # Downsize only the fallback frame; code crops keep all pixels.
                side = max(frame.shape[:2])
                thumb = cv2.resize(frame, None, fx=min(1.,1280/side), fy=min(1.,1280/side))
                if not fallback or fno-fallback[-1][0] >= fps*.8:
                    fallback.append((fno, thumb))
            track_limit = max(64, profile.max_codes*6)
            if len(tracks) > track_limit:
                tracks.sort(key=lambda t: t.observations[0].quality, reverse=True)
                del tracks[track_limit:]
                tracks.sort(key=lambda t: t.last_frame)
            if progressed or unsettled:
                if state["idle"] and state["seen_stable"]:
                    # Khoảng lặng này đã bị phá bởi mã mới: ghi lại để chọn ngưỡng dừng sớm.
                    state["max_quiet"] = max(state["max_quiet"], state["idle"])
                state["idle"] = 0
                state["progress_at"] = metrics["sampled_frames"]
            else:
                state["idle"] += 1
            if early_samples and state["seen_stable"] and state["idle"] >= early_samples:
                metrics["early_stopped_at_sample"] = metrics["sampled_frames"]
                return step, True
            # Thưa-rồi-dày: khung không có vùng mã và không có vùng đang hiện dở thì nhảy xa hơn.
            gap = step*coarse if coarse > 1 and not panels and not unsettled else step
            if gap > step:
                metrics["coarse_skipped_frames"] = metrics.get("coarse_skipped_frames", 0) + gap - step
            return gap, False

        def skip_frames(gap):
            for _ in range(gap-1):
                if not cap.grab():
                    break

        if parallel:
            pool = ThreadPoolExecutor(max_workers=workers)
            try:
                while metrics["sampled_frames"] < limit:
                    batch = []
                    while len(batch) < workers and metrics["sampled_frames"] < limit:
                        ok, frame = cap.read()
                        if not ok:
                            break
                        metrics["sampled_frames"] += 1
                        frame = _resize_frame_for_ocr(frame)
                        batch.append((frame_no, frame))
                        skip_frames(step)
                        frame_no += step
                    if not batch:
                        break
                    results = list(pool.map(process_frame, batch))
                    stop = False
                    for (fno, frame), (panels, pairs) in zip(batch, results):
                        _gap, stop = assemble(fno, frame, panels, pairs)
                        if stop:
                            break
                    if stop:
                        break
            finally:
                pool.shutdown(wait=True)
        else:
            while metrics["sampled_frames"] < limit:
                ok, frame = cap.read()
                if not ok:
                    break
                metrics["sampled_frames"] += 1
                frame = _resize_frame_for_ocr(frame)
                panels, pairs = process_frame((frame_no, frame))
                gap, stop = assemble(frame_no, frame, panels, pairs)
                if stop:
                    break
                skip_frames(gap)
                frame_no += gap
        metrics["last_progress_sample"] = state["progress_at"]
        if coarse > 1:
            metrics["coarse_factor"] = coarse
        metrics["max_quiet_gap"] = state["max_quiet"]
        metrics["video_frames"] = min(count, frame_no + 1)
        metrics["duration_seconds"] = round(metrics["video_frames"]/fps, 3)
    finally:
        cap.release()
        del frame
        batch.clear()
        results.clear()
        gc.collect()
    # Repeated animation cycles need no repeated neural recognition. Compare
    # glyph pixels and position, retaining the sharpest independent pair.
    unique = []
    for track in tracks:
        if not track.observations:
            continue
        match = next((t for t in unique if _same_artwork(t.observations[0], track.observations[0])), None)
        if match is None:
            unique.append(track)
        else:
            for obs in track.observations:
                match.add(obs)
    metrics["appearances"] = len(tracks)
    return unique[:max(32, profile.max_codes*4)], fallback


def _recognize(extractor, observations, metrics, *, enhanced=False):
    images = []
    for obs in observations:
        if enhanced and obs.mask is not None:
            image = cv2.cvtColor(255-obs.mask, cv2.COLOR_GRAY2BGR)
        else:
            image = obs.image
        # A little neutral padding improves recognition at glyph boundaries.
        value = 255 if enhanced else tuple(int(v) for v in image[0,0])
        image = cv2.copyMakeBorder(image, 3, 3, 4, 4, cv2.BORDER_CONSTANT, value=value)
        images.append(image)
    metrics["recognized_regions"] += len(images)
    return extractor.recognize_code_regions(images)


def _recognize_cached(extractor, observations, metrics, cache):
    """Như _recognize nhưng bỏ qua crop đã đọc lúc phát sớm. cache rỗng => đúng một lần gọi như cũ.

    cache lưu cả đối tượng obs (không chỉ id) để id không bị tái sử dụng sau khi obs bị thu hồi.
    """
    todo = [o for o in observations if id(o) not in cache]
    if todo:
        for obs, reading in zip(todo, _recognize(extractor, todo, metrics)):
            cache[id(obs)] = (obs, reading)
    return [cache[id(o)][1] for o in observations]


def _select_readings(profile, tracks, readings, *, temporal):
    threshold = float(getattr(Config, "OCR_CONFIDENCE_THRESHOLD", .70))
    accepted, unresolved = [], []
    offset = 0
    for track in tracks:
        items = []
        for obs in track.observations:
            text, score = readings[offset]
            offset += 1
            code = _candidate_code(profile, text) if score >= threshold else None
            if code:
                items.append((code, float(score), obs.frame))
        by_code = {}
        for code, score, frame in items:
            by_code.setdefault(code, []).append((score, frame))
        agreed = [(code, votes) for code, votes in by_code.items()
                  if len({frame for _,frame in votes}) >= (2 if temporal else 1)]
        if len(agreed) == 1:
            code, votes = agreed[0]
            accepted.append({"code": code, "raw": code,
                             "confidence": min(score for score,_ in votes)})
        else:
            unresolved.append(track)
    return accepted, unresolved


def _confirm_sparse_appearances(path, extractor, profile, tracks, observations, readings, metrics):
    """Short appearances may only intersect one sample; decode a neighbor to verify."""
    threshold = float(getattr(Config, "OCR_CONFIDENCE_THRESHOLD", .70))
    by_observation = {id(obs): result for obs, result in zip(observations, readings)}
    batches = []
    for track in tracks:
        candidates = []
        for obs in track.observations:
            text, score = by_observation.get(id(obs), ("", 0.))
            code = _candidate_code(profile, text) if score >= threshold else None
            if code:
                candidates.append((obs, code, score))
        if not candidates:
            continue
        obs, code, score = max(candidates, key=lambda item: (len(item[1]), item[2]))
        x0, y0, x1, y1 = obs.bounds
        batches.append({"source_frame": obs.frame, "readings": [{
            "code":code, "text":code, "confidence":score, "bounds":obs.bounds,
            "center":((x0+x1)/2, (y0+y1)/2), "frame":obs.frame}]})
    if not batches:
        return []
    metrics["temporal_confirmation_regions"] = sum(len(b["readings"]) for b in batches)
    confirmed = extractor.confirm_video_readings(path, batches, track_motion=True)
    return [{"code":r["code"], "raw":r["code"],
             "confidence":min(r["confidence"],r["confirmation_confidence"])}
            for r in confirmed if _candidate_code(profile, r.get("confirmation_text")) == r["code"]
            and r.get("confirmation_confidence", 0.) >= threshold
            and r.get("confirmation_frame") is not None and r["confirmation_frame"] != r["frame"]]


def _extract_streaming_video(path, extractor, profile, cfg, on_code, metrics, started):
    """XX8 CTC streaming in the existing worker and callback contract.

    Return None only when the loaded engine has no compatible CTC adapter.
    An uncertain reading is never retried through an easier submission gate.
    """
    # PATCH_MARKER_2026-10-26-streaming-ctc
    factory = getattr(type(extractor), "get_streaming_recognizer", None)
    rec = factory(extractor) if callable(factory) else None
    if rec is None:
        metrics["streaming_unavailable"] = True
        return None
    from streaming_code_reader import StreamingCodeReader

    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        cap.release()
        raise RuntimeError("Cannot decode video")
    items, emitted, fallback = {}, [], []
    frame = None
    limit_codes = 1 if cfg.get("ocr_single_code_mode") else profile.max_codes
    callback = on_code if getattr(Config, "OCR_VIDEO_EARLY_EMIT_ENABLED", True) else None

    def accept(event):
        code = _candidate_code(profile, event.code)
        early_threshold = _early_emit_confidence(profile)
        if not event.final or event.ambiguous:
            if code and (event.confidence < early_threshold or not event.consecutive):
                logger.info("OCR_REJECT_LOW_CONFIDENCE code=%s", code)
            metrics["streaming_uncertain"] = metrics.get("streaming_uncertain", 0) + 1
            return
        if code and (event.confidence < early_threshold or not event.consecutive):
            logger.info("OCR_REJECT_LOW_CONFIDENCE code=%s", code)
            return
        if (not code or event.confidence < early_threshold or not event.consecutive
                or code in items or len(items) >= limit_codes):
            return
        item = {"code": code, "raw": code, "confidence": event.confidence}
        items[code] = item
        if callback is not None:
            try:
                callback(dict(item))
            except Exception:
                metrics["early_emit_errors"] = metrics.get("early_emit_errors", 0) + 1
            else:
                emitted.append(code)
                metrics["early_emitted"] = len(emitted)
                metrics.setdefault("first_emit_sample", metrics["sampled_frames"])
                metrics.setdefault("first_emit_ms", round((time.perf_counter()-started)*1000, 1))

    try:
        fps, count, limit = _video_sample_limits(path, cap, metrics)
        step = max(1, round(fps*float(getattr(Config, "OCR_STREAMING_SAMPLE_SECONDS", .30))))
        if count:
            step = max(step, (count+limit-1)//limit)
        marks = {max(0, round((count-1)*fraction/step)*step)
                 for fraction in (.08, .25, .42, .60, .78, .94)} if count else set()
        reader = StreamingCodeReader(
            rec, profile, find_panels=find_code_panels, glyph_crop=_glyph_crop,
            same_artwork=lambda a, b: _same_artwork(a, b, tolerate_edges=True),
            margin=float(getattr(Config, "OCR_STREAMING_MARGIN", .90)),
            min_mass=float(getattr(Config, "OCR_CONFIDENCE_THRESHOLD", .70)),
            window=int(getattr(Config, "OCR_STREAMING_WINDOW", 12)),
            gap_reset=max(1.2, 2*step/fps+.01), max_cells=max(64, profile.max_codes*4),
            sample_interval=step/fps,
            # PATCH_MARKER_2026-11-01-xx8-parallel: XX8 giu nguyen phat
            # som (nhap ngay song song voi OCR); chi giam tai detector
            # bang cach chay 1 khung trong 2 khung, khong tat callback.
            detect_every=2 if profile.domain == "getcodexx8bonus.pages.dev" else 1)
        frame_no = 0
        while metrics["sampled_frames"] < limit:
            ok, frame = cap.read()
            if not ok:
                break
            metrics["sampled_frames"] += 1
            frame = _resize_frame_for_ocr(frame)
            for event in random_codes(reader.feed(frame, frame_no/fps)):
                accept(event)
            if (frame_no in marks or not count) and len(fallback) < 6:
                scale = min(1., 1280/max(frame.shape[:2]))
                fallback.append((frame_no, cv2.resize(frame, None, fx=scale, fy=scale)))
            for _ in range(step-1):
                if not cap.grab():
                    break
            frame_no += step
        for event in random_codes(reader.flush()):
            accept(event)
        metrics.update(streaming=True, video_frames=min(count, frame_no + 1),
                       duration_seconds=round(min(count, frame_no + 1)/fps, 3),
                       appearances=len(reader.cells), recognized_regions=reader.stats["recognitions"])
        metrics["streaming_stats"] = {k: round(v, 2) if isinstance(v, float) else v
                                      for k, v in reader.stats.items()}
    finally:
        cap.release()
        del frame
        gc.collect()
    # Artwork without glyph panels can still use the existing two-frame detector
    # confirmation. Pending CTC evidence is not converted into guessed variants.
    if not items and reader.stats["recognitions"] == 0:
        for item in _fallback_codes(path, extractor, profile, fallback, True, metrics):
            code = _candidate_code(profile, item["code"])
            if code and code not in items and len(items) < limit_codes:
                items[code] = item
    codes = random_codes(items.values())
    metrics["elapsed_ms"] = round((time.perf_counter()-started)*1000, 1)
    result = {"success": bool(codes), "codes": codes, "message": f"{len(codes)} codes" if codes else "no verified code",
              "text": "\n".join(item["code"] for item in codes), "retryable": False,
              "failure_kind": None if codes else "NO_CODE", "ocr_metrics": metrics,
              "early_emitted": emitted}
    del reader, fallback
    gc.collect()
    return result


def extract_site_codes(path, extractor, target_url, cfg, on_code=None):
    """Run completely offline in one worker; return only independently verified codes.

    on_code (tuỳ chọn): early video callbacks require the domain-specific
    confidence threshold and the same code in two consecutive samples. Hàm chạy trong
    luồng worker OCR, nên người gọi phải tự chuyển về event loop (loop.call_soon_threadsafe).
    XX8 video uses bounded CTC fusion and only emits confirmed events. Other sites retain
    the existing recognition policy. Kết quả cuối vẫn gồm mọi mã; "early_emitted" liệt kê
    các mã đã phát để người gọi bỏ qua trùng.
    """
    started = time.perf_counter()
    profile = OCRProfile.for_site(target_url, cfg)
    metrics = {"policy": POLICY_VERSION, "domain": profile.domain,
               "sampled_frames": 0, "recognized_regions": 0,
               "detector_calls": 0, "appearances": 0}
    video = Path(path).suffix.lower() in (".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v")
    if video and profile.domain in ("llwincode.com", "xx88code.com"):
        return _extract_anchor_video(path, extractor, profile, cfg, on_code, metrics, started)
    if (video and profile.domain == "getcodexx8bonus.pages.dev"
            and getattr(Config, "OCR_STREAMING_ENABLED", True)):
        result = _extract_streaming_video(path, extractor, profile, cfg, on_code, metrics, started)
        if result is not None:
            return result
    early = {"cache": {}, "items": {}}
    emit = None
    emit_code = None
    if video and on_code is not None and _early_emit_allowed(profile, cfg):
        emit_cap = 1 if cfg.get("ocr_single_code_mode") else profile.max_codes
        early_threshold = _early_emit_confidence(profile)

        def emit_code(item):
            """Ghi nhận một mã phát sớm (đã qua candidate + ngưỡng). Trả True
            nếu mã này mới được phát; người gọi dùng để tránh gửi trùng và
            để EarlyDispatch.settle loại nó khỏi lô cuối."""
            code = item.get("code") if isinstance(item, dict) else None
            if not code or code in early["items"] or len(early["items"]) >= emit_cap:
                return False
            early["items"][code] = item
            metrics["early_emitted"] = metrics.get("early_emitted", 0) + 1
            try:
                on_code(dict(item))
            except Exception:
                metrics["early_emit_errors"] = metrics.get("early_emit_errors", 0) + 1
                return False
            return True

        def emit(track):
            observations = list(track.observations[-2:])
            if len(observations) < 2:
                return
            if len(early["items"]) >= emit_cap:
                return
            try:
                readings = _recognize_cached(extractor, observations, metrics, early["cache"])
                recognized = [(_candidate_code(profile, text), float(score))
                              for text, score in readings]
                code = recognized[0][0]
                sample_step = max(1, int(metrics.get("sample_step_frames", 1)))
                consecutive_samples = (
                    observations[-1].frame - observations[-2].frame == sample_step
                )
                if (not code or recognized[1][0] != code
                        or min(recognized[0][1], recognized[1][1]) < early_threshold
                        or not consecutive_samples):
                    if code or recognized[1][0]:
                        logger.info("OCR_REJECT_LOW_CONFIDENCE code=%s",
                                    code or recognized[1][0])
                    return
                item = {"code": code, "raw": code,
                        "confidence": min(recognized[0][1], recognized[1][1])}
                if emit_code(item):
                    metrics.setdefault("first_emit_sample", metrics["sampled_frames"])
                    metrics.setdefault("first_emit_ms", round((time.perf_counter()-started)*1000, 1))
            except Exception:
                # Phát sớm chỉ là đường tắt: lỗi ở đây không được làm hỏng lượt quét đầy đủ.
                metrics["early_emit_errors"] = metrics.get("early_emit_errors", 0) + 1

    if video:
        tracks, fallback = _collect_video(path, profile, metrics, emit=emit)
    else:
        with Image.open(path) as source:
            bgr = cv2.cvtColor(np.asarray(ImageOps.exif_transpose(source).convert("RGB")), cv2.COLOR_RGB2BGR)
        bgr = _resize_frame_for_ocr(bgr)
        observations = [_glyph_crop(bgr, box, profile, 0) for box in find_code_panels(bgr, profile)]
        tracks = [Appearance(obs.bounds, 0, [obs]) for obs in observations if obs is not None]
        fallback = [(0, bgr)]
        metrics["sampled_frames"] = 1
    metrics["region_selection_ms"] = round((time.perf_counter()-started)*1000, 1)
    # Two independent frames are enough when they agree. Read the third stored
    # alternative only for unresolved XX8 cells, keeping its recovery benefit.
    xx8_temporal = video and profile.domain == "getcodexx8bonus.pages.dev"
    primary_tracks = ([Appearance(t.bounds, t.last_frame, t.observations[:2]) for t in tracks]
                      if xx8_temporal else tracks)
    observations = [o for track in primary_tracks for o in track.observations]
    codes = []
    if observations:
        readings = _recognize_cached(extractor, observations, metrics, early["cache"])
        codes, unresolved = _select_readings(profile, primary_tracks, readings, temporal=video)
        if xx8_temporal and unresolved:
            originals = {id(primary): original for primary, original in zip(primary_tracks, tracks)}
            expanded = [originals[id(track)] for track in unresolved]
            known = {id(obs): reading for obs, reading in zip(observations, readings)}
            extra_obs = [obs for track in expanded for obs in track.observations if id(obs) not in known]
            extra_readings = _recognize(extractor, extra_obs, metrics) if extra_obs else []
            known.update((id(obs), reading) for obs, reading in zip(extra_obs, extra_readings))
            metrics["xx8_third_frame_regions"] = len(extra_obs)
            expanded_readings = [known[id(obs)] for track in expanded for obs in track.observations]
            recovered, unresolved = _select_readings(profile, expanded, expanded_readings, temporal=True)
            codes.extend(recovered)
        # Photos require a second preprocessing reading; videos use it only
        # for boxes whose original independent frames did not agree.
        retry_tracks = unresolved if video else tracks
        retry_obs = [o for track in retry_tracks for o in track.observations]
        if retry_obs:
            alternate = _recognize(extractor, retry_obs, metrics, enhanced=True)
            extra, remaining = _select_readings(profile, retry_tracks, alternate, temporal=video)
            if video:
                codes.extend(extra)
                codes.extend(_confirm_sparse_appearances(
                    path, extractor, profile, remaining, retry_obs, alternate, metrics))
            else:
                exact = {item["code"]:item for item in extra}
                codes = [{**item, "confidence": min(item["confidence"], exact[item["code"]]["confidence"])}
                         for item in codes if item["code"] in exact]
    if not codes:
        # Fallback early emission requires the same confidence and consecutive
        # sample checks as panel-based emission.
        codes = _fallback_codes(path, extractor, profile, fallback, video, metrics,
                                on_code_item=emit_code)
    unique = {}
    for item in codes:
        old = unique.get(item["code"])
        if old is None or item["confidence"] > old["confidence"]:
            unique[item["code"]] = item
    # Conflicting readings of one cell never become a list of guessed variants.
    codes = random_codes(unique.values(), profile.max_codes)
    if cfg.get("ocr_single_code_mode"):
        codes = random_codes(codes, 1)
    metrics["elapsed_ms"] = round((time.perf_counter()-started)*1000, 1)
    return {"success": bool(codes), "codes": codes, "message": f"{len(codes)} codes" if codes else "no verified code",
            "text": "\n".join(item["code"] for item in codes), "retryable": False,
            "failure_kind": None if codes else "NO_CODE", "ocr_metrics": metrics,
            "early_emitted": list(early["items"])}


def _fallback_codes(path, extractor, profile, frames, video, metrics, on_code_item=None):
    """One full-image detection per selected frame, followed by box recognition.

    PATCH_MARKER_2026-10-25-v4-poll-ocr: khi on_code_item được truyền (video
    có early-emit), mỗi khung fallback vừa đọc ra mã là phát ngay mã đó,
    không chờ quét hết các khung rồi mới xác nhận. Detection toàn ảnh vẫn
    tuần tự vì detector dùng chung không an toàn luồng — phần song song của
    bản vá nằm ở _collect_video, còn ở đây lợi chính là phát sớm.
    """
    found = []
    threshold = float(getattr(Config, "OCR_CONFIDENCE_THRESHOLD", .70))
    early_threshold = _early_emit_confidence(profile)
    previous_early_candidates = {}
    batches = []
    for frame_no, bgr in frames:
        cells = _llwin_grid_cells(bgr) if profile.domain == "llwincode.com" else None
        if cells is not None and not cells:
            metrics["llwin_no_grid_frames"] = metrics.get("llwin_no_grid_frames", 0) + 1
            continue
        cropped_grid = cells is not None and video
        if cropped_grid:
            height, width = bgr.shape[:2]
            x0 = max(0, int(min(cell[0] for cell in cells) * width))
            y0 = max(0, int(min(cell[1] for cell in cells) * height))
            x1 = min(width, math.ceil(max(cell[2] for cell in cells) * width))
            y1 = min(height, math.ceil(max(cell[3] for cell in cells) * height))
            crop = bgr[y0:y1, x0:x1]
            if not crop.size:
                continue
            image = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
            metrics["llwin_grid_detector_frames"] = metrics.get("llwin_grid_detector_frames", 0) + 1
        else:
            image = Image.fromarray(cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB))
        try:
            metrics["detector_calls"] += 1
            readings = extractor.read_image_regions(image)
            candidates = []
            for reading in readings:
                if cropped_grid:
                    a, b, c, d = reading["bounds"]
                    reading = {**reading, "bounds": ((x0+a*(x1-x0))/width, (y0+b*(y1-y0))/height,
                                                     (x0+c*(x1-x0))/width, (y0+d*(y1-y0))/height)}
                if cells is not None and not _llwin_in_grid(reading, cells):
                    continue
                code = _candidate_code(profile, reading["text"])
                if code and reading["confidence"] >= threshold:
                    candidates.append({**reading, "code":code, "frame":frame_no})
            if video and on_code_item is not None:
                for candidate in random_codes(candidates):
                    code = candidate["code"]
                    confidence = float(candidate["confidence"])
                    if confidence < early_threshold:
                        logger.info("OCR_REJECT_LOW_CONFIDENCE code=%s", code)
                        continue
                    previous = previous_early_candidates.get(code, [])
                    sample_step = max(1, int(metrics.get("sample_step_frames", 1)))
                    matches = [item for item in previous
                               if frame_no - item["frame"] == sample_step
                               and _iou(item["bounds"], candidate["bounds"]) >= .45]
                    if not matches:
                        continue
                    previous_confidence = max(float(item["confidence"]) for item in matches)
                    if on_code_item({
                            "code": code, "raw": code,
                            "confidence": min(confidence, previous_confidence)}):
                        metrics["fallback_emitted"] = metrics.get("fallback_emitted", 0) + 1
                previous_early_candidates = {}
                for item in candidates:
                    if float(item["confidence"]) >= early_threshold:
                        previous_early_candidates.setdefault(item["code"], []).append(item)
            if video:
                if candidates:
                    batches.append({"source_frame": frame_no, "readings": candidates})
            else:
                confirmations = extractor.confirm_code_readings(image, candidates)
                for reading in confirmations:
                    if (_candidate_code(profile, reading["confirmation_text"]) == reading["code"]
                            and reading["confirmation_confidence"] >= threshold):
                        found.append({"code":reading["code"], "raw":reading["code"],
                                      "confidence":min(reading["confidence"],reading["confirmation_confidence"])})
        finally:
            image.close()
    if batches:
        # Detector readings already come from independently decoded frames.
        # Reuse that evidence before seeking/re-reading every repeated cell.
        # Keep position agreement so two unrelated labels cannot confirm a code.
        evidence = {}
        agreed = {}
        for batch in batches:
            for reading in batch["readings"]:
                code = reading["code"]
                previous = evidence.setdefault(code, [])
                pairs = [other for other in previous
                         if other["frame"] != reading["frame"]
                         and _iou(other["bounds"], reading["bounds"]) >= .45]
                if pairs:
                    score = max(min(other["confidence"], reading["confidence"])
                                for other in pairs)
                    agreed[code] = max(agreed.get(code, 0.), score)
                previous.append(reading)
        found.extend({"code": code, "raw": code, "confidence": score}
                     for code, score in agreed.items())
        metrics["fallback_temporal_agreements"] = len(agreed)
        batches = [{**batch, "readings": [reading for reading in batch["readings"]
                                          if reading["code"] not in agreed]}
                   for batch in batches]
        batches = [batch for batch in batches if batch["readings"]]
        metrics["fallback_confirmation_regions"] = sum(len(b["readings"]) for b in batches)
    # For LLWIN/XX88, a second OCR pass over a neighboring crop can agree on
    # two incompatible readings of the same cell. Require two separately
    # detected frames at the same position instead of guessing a variant.
    if batches and profile.domain not in {"llwincode.com", "xx88code.com"}:
        for reading in extractor.confirm_video_readings(path, batches, track_motion=True):
            if (_candidate_code(profile, reading["confirmation_text"]) == reading["code"]
                    and reading["confirmation_confidence"] >= threshold
                    and reading.get("confirmation_frame") is not None
                    and reading["confirmation_frame"] != reading["frame"]):
                found.append({"code":reading["code"], "raw":reading["code"],
                              "confidence":min(reading["confidence"],reading["confirmation_confidence"])})
    return found
