"""Bounded, position-independent CTC evidence for animated code panels.

PATCH_MARKER_2026-10-26-streaming-ctc
Inference stays in the OCR worker. Events marked final=False are diagnostics,
never submission candidates. A fused word must have occurred intact in a frame.
"""
# PATCH_MARKER_2026-10-28-ocr-panel-streaming
from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
import importlib.util
import math
from pathlib import Path
import time

import cv2
import numpy as np

ALNUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"


@dataclass
class Reading:
    text: str
    dist: np.ndarray
    t: float = 0.0
    mass: np.ndarray | None = None


class CtcAlnumRecognizer:
    """Decode PP-OCR probabilities while retaining absolute alphabet evidence.

    The bot supplies its existing ONNX session; standalone callers may supply
    a model path. No second model or inference thread pool is created by the bot.
    """

    def __init__(self, model_path=None, threads=2, *, session=None, vocab=None):
        if session is None:
            import onnxruntime as ort
            if model_path is None:
                found = []
                for package in ("rapidocr", "rapidocr_onnxruntime"):
                    spec = importlib.util.find_spec(package)
                    if spec is not None and spec.origin:
                        found = sorted((Path(spec.origin).parent / "models").glob("*rec*.onnx"))
                        if found:
                            break
                if len(found) != 1:
                    raise ValueError("Supply a recognizer model path (no unique local model)")
                model_path = found[0]
            opts = ort.SessionOptions()
            opts.intra_op_num_threads = max(1, int(threads))
            opts.inter_op_num_threads = 1
            session = ort.InferenceSession(str(model_path), opts, providers=["CPUExecutionProvider"])
        self.session = session
        if vocab is None:
            chars = session.get_modelmeta().custom_metadata_map.get("character")
            if not chars:
                raise ValueError("CTC model has no embedded character dictionary")
            vocab = ["blank"] + chars.splitlines() + [" "]
        self.vocab = tuple(vocab)
        self.allowed = np.asarray([self.vocab.index(c) for c in ALNUM], dtype=np.int64)
        inputs, outputs = session.get_inputs(), session.get_outputs()
        if len(inputs) != 1 or len(inputs[0].shape) != 4 or inputs[0].type != "tensor(float)":
            raise ValueError("Expected one float32 NCHW recognizer input")
        shape = inputs[0].shape
        if shape[1] != 3 or not isinstance(shape[2], int) or shape[2] <= 0:
            raise ValueError("Unsupported recognizer channels/height")
        self.input_name, self.height = inputs[0].name, shape[2]
        self.width = shape[3] if isinstance(shape[3], int) else None
        out = outputs[0].shape
        if len(out) != 3 or isinstance(out[2], int) and out[2] != len(self.vocab):
            raise ValueError("CTC output and character dictionary differ")

    @classmethod
    def from_rapidocr(cls, engine):
        rec = engine.text_rec
        if rec is None:
            # Recent RapidOCR loads recognition lazily. An empty warm-up image
            # only loads detection; use its loader and share the same session.
            rec = engine._load_rec_model()
        return cls(session=rec.session.session, vocab=rec.postprocess_op.character)

    def _prep(self, bgr):
        if bgr.ndim != 3 or bgr.shape[2] != 3 or not bgr.size:
            raise ValueError("Expected a nonempty BGR crop")
        bgr = cv2.copyMakeBorder(bgr, 3, 3, 4, 4, cv2.BORDER_CONSTANT,
                                 value=tuple(int(v) for v in bgr[0, 0]))
        width = max(8, math.ceil(self.height * bgr.shape[1] / bgr.shape[0]))
        if width > 2048:
            raise ValueError("Recognizer crop is too wide")
        resized = cv2.resize(bgr, (min(width, self.width or width), self.height))
        x = (resized.astype(np.float32) / 127.5 - 1).transpose(2, 0, 1)
        if self.width is not None:
            padded = np.zeros((3, self.height, self.width), np.float32)
            padded[:, :, :x.shape[2]] = x
            x = padded
        return x[None]

    def read(self, bgr):
        probs = np.asarray(self.session.run(None, {self.input_name: self._prep(bgr)})[0])
        if probs.ndim != 3 or probs.shape[0] != 1 or probs.shape[2] != len(self.vocab):
            raise ValueError("Unexpected CTC output shape")
        probs = probs[0]
        # These PP-OCR models return probabilities, not arbitrary logits.
        if (not np.isfinite(probs).all() or np.any(probs < 0)
                or not np.allclose(probs.sum(axis=1), 1., atol=.03)):
            raise ValueError("Expected normalized CTC probabilities")
        sub = np.concatenate((probs[:, :1], probs[:, self.allowed]), axis=1)
        best = sub.argmax(axis=1)
        chars, dists, masses = [], [], []
        start = 0
        while start < len(best):
            end = start + 1
            while end < len(best) and best[end] == best[start]:
                end += 1
            k = best[start]
            if k:
                row = sub[start:end, 1:].mean(axis=0)
                mass = float(row.sum())
                chars.append(ALNUM[k-1])
                masses.append(mass)
                dists.append(row / max(mass, 1e-12))
            start = end
        return Reading("".join(chars), np.asarray(dists).reshape((-1, len(ALNUM))),
                       mass=np.asarray(masses))


def fuse(readings):
    if not readings:
        raise ValueError("No CTC readings")
    length = len(readings[0].text)
    if not length or any(r.dist.shape != (length, len(ALNUM)) for r in readings):
        raise ValueError("CTC readings are not position aligned")
    logp = np.zeros((length, len(ALNUM)), np.float64)
    for reading in readings:
        if not np.isfinite(reading.dist).all() or np.any(reading.dist < 0):
            raise ValueError("Invalid CTC distribution")
        logp += np.log(np.maximum(reading.dist, 1e-6))
    post = np.exp(logp - logp.max(axis=1, keepdims=True))
    post /= post.sum(axis=1, keepdims=True)
    code = "".join(ALNUM[i] for i in post.argmax(axis=1))
    conf = post.max(axis=1)
    alts = [[ALNUM[i] for i in row.argsort()[::-1][:2] if row[i] >= .15]
            if c < .9 else [] for row, c in zip(post, conf)]
    return code, conf, alts


@dataclass
class CodeEvent:
    code: str
    t: float
    readings: int
    confidence: float
    ambiguous: list = field(default_factory=list)
    revised: bool = False
    final: bool = True
    consecutive: bool = False


@dataclass
class _Cell:
    box: tuple
    readings: deque
    last_seen: float
    last_read: float = -1e9
    observation: object = None
    strong_text: str | None = None
    strikes: int = 0
    blocked: bool = False
    emitted: str | None = None
    provisional: str | None = None
    previous: str | None = None


def _iou(a, b):
    overlap = max(0., min(a[2], b[2])-max(a[0], b[0])) * max(0., min(a[3], b[3])-max(a[1], b[1]))
    total = (a[2]-a[0])*(a[3]-a[1]) + (b[2]-b[0])*(b[3]-b[1]) - overlap
    return overlap / total if total > 0 else 0.


def _same_pixels(a, b):
    aa = cv2.resize(a.image, (128, 32)).astype(np.float32)
    bb = cv2.resize(b.image, (128, 32)).astype(np.float32)
    return float(np.mean(np.abs(aa-bb))) < 2.


class StreamingCodeReader:
    def __init__(self, recognizer, profile, *, find_panels, glyph_crop,
                 lengths=None, prefix=None, min_readings=2, margin=.90,
                 patience=5, gap_reset=1.2, max_strikes=3, window=12,
                 same_artwork=None, min_mass=.70, max_cells=80,
                 sample_interval=None, detect_every=1):
        self.rec, self.profile = recognizer, profile
        self.find_panels, self.glyph_crop = find_panels, glyph_crop
        self.lengths = tuple(profile.lengths if lengths is None else lengths)
        self.prefix = ("XX" if profile.domain == "getcodexx8bonus.pages.dev" else "") if prefix is None else prefix
        self.min_readings = max(2, int(min_readings))
        self.margin, self.patience = float(margin), max(2, int(patience))
        self.gap_reset, self.max_strikes = max(.1, float(gap_reset)), max(1, int(max_strikes))
        self.window, self.max_cells = max(self.min_readings, int(window)), max(1, int(max_cells))
        self.same_artwork, self.min_mass = same_artwork or _same_pixels, float(min_mass)
        self.sample_interval = max(0., float(sample_interval or 0.))
        # PATCH_MARKER_2026-11-01-xx8-parallel: detect_every>1 thi chi chay
        # detector o 1 khung trong detect_every khung, cac khung giua tai
        # dung hop panel cua khung truoc (luoi video XX8 la tinh). O moi
        # hien cham nhat 1 nhip lay mau moi duoc phat hien, nhung van can
        # 2 lan doc lien tiep moi phat ma nen khong mat ma. Mac dinh 1.
        self._detect_every = max(1, int(detect_every))
        self._cached_boxes = None
        self.cells = []
        self.stats = dict(frames=0, recognitions=0, skipped_blocked=0, skipped_settled=0,
                          valid_readings=0, resets=0, rec_ms=0., total_ms=0.,
                          panel_cache_hits=0)
        self._last_t = -1e9

    def _reset(self, cell):
        cell.previous = cell.emitted or cell.previous
        cell.readings.clear()
        cell.emitted = cell.provisional = cell.strong_text = None
        cell.strikes, cell.blocked = 0, False
        self.stats["resets"] += 1

    def feed(self, bgr, t):
        t = float(t)
        if not math.isfinite(t) or t <= self._last_t:
            return []  # Repeated crops of one decoded frame are not independent.
        self._last_t = t
        started = time.perf_counter()
        self.stats["frames"] += 1
        self.cells[:] = [c for c in self.cells if t-c.last_seen <= self.gap_reset]
        used, boxes, events = set(), [], []
        if (self._detect_every > 1 and self._cached_boxes is not None
                and (self.stats["frames"] % self._detect_every) != 1):
            self.stats["panel_cache_hits"] += 1
            raw_boxes = list(self._cached_boxes)
        else:
            raw_boxes = list(self.find_panels(bgr, self.profile))
            self._cached_boxes = list(raw_boxes)
        for box in raw_boxes:
            if any(_iou(box, old) > .7 for old in boxes):
                continue
            boxes.append(box)
            cell = next((c for c in self.cells if id(c) not in used and _iou(c.box, box) > .45), None)
            if cell is None:
                if len(self.cells) >= self.max_cells:
                    continue
                cell = _Cell(box, deque(maxlen=self.window), t)
                self.cells.append(cell)
            used.add(id(cell))
            cell.box, cell.last_seen = box, t
            obs = self.glyph_crop(bgr, box, self.profile, self.stats["frames"])
            if obs is None:
                continue
            if cell.observation is not None and not self.same_artwork(cell.observation, obs):
                self._reset(cell)
                cell.last_read = -1e9
            cell.observation = obs
            # Even unchanged banners and finalized cells are periodically probed.
            # New pixels bypass this cooldown immediately, before the blocked check.
            if (cell.blocked or cell.emitted) and t-cell.last_read < self.gap_reset:
                key = "skipped_blocked" if cell.blocked else "skipped_settled"
                self.stats[key] += 1
                continue
            begin = time.perf_counter()
            reading = self.rec.read(obs.image)
            self.stats["rec_ms"] += (time.perf_counter()-begin)*1000
            self.stats["recognitions"] += 1
            cell.last_read, reading.t = t, t
            mass = np.ones(len(reading.text)) if reading.mass is None else np.asarray(reading.mass)
            valid = (reading.text.startswith(self.prefix) and len(reading.text) in self.lengths
                     and reading.dist.shape == (len(reading.text), len(ALNUM))
                     and np.isfinite(reading.dist).all() and np.all(reading.dist >= 0)
                     and np.allclose(reading.dist.sum(axis=1), 1., atol=.01)
                     and mass.shape == (len(reading.text),) and np.isfinite(mass).all()
                     and np.all(mass >= self.min_mass) and self.profile.candidate(reading.text))
            if not valid:
                cell.strikes += 1
                cell.blocked = cell.strikes >= self.max_strikes and not cell.readings and not cell.emitted
                continue
            strong = float(reading.dist.max(axis=1).min()) >= self.margin
            changed_final = (cell.emitted is not None and reading.text != cell.emitted
                             and float(reading.dist.max(axis=1).min()) >= self.min_mass)
            if changed_final or strong and cell.strong_text is not None and reading.text != cell.strong_text:
                # A changed, clear word starts fresh even if a pixel comparison
                # missed a one-character artwork replacement.
                self._reset(cell)
            if strong:
                cell.strong_text = reading.text
            cell.strikes, cell.blocked = 0, False
            cell.readings.append(reading)
            self.stats["valid_readings"] += 1
            ev = self._maybe_emit(cell, t)
            if ev is not None:
                events.append(ev)
        self.stats["total_ms"] += (time.perf_counter()-started)*1000
        return events

    def _maybe_emit(self, cell, t, *, flush=False):
        if cell.emitted or len(cell.readings) < self.min_readings:
            return None
        ranked = Counter(len(r.text) for r in cell.readings).most_common()
        if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
            return None
        same = [r for r in cell.readings if len(r.text) == ranked[0][0]]
        if len({r.t for r in same}) < self.min_readings:
            return None
        code, conf, alts = fuse(same)
        # Multiplying unrelated words can invent a confident hybrid. Neither
        # normal emission nor EOF may manufacture an unobserved whole word.
        if code not in {r.text for r in same} or not self.profile.candidate(code):
            return None
        indices = [ALNUM.index(c) for c in code]
        supports = [r for r in same if np.all(r.dist[np.arange(len(code)), indices] >= .15)]
        if len(supports) < self.min_readings:
            return None
        masses = [np.ones(len(code)) if r.mass is None else r.mass for r in supports]
        low = min(1., float(conf.min()), float(np.mean(masses, axis=0).min()))
        confident = low >= self.margin and not flush
        if not confident and not (flush or len(same) >= self.patience):
            return None
        if not confident and cell.provisional == code:
            return None
        recent = list(cell.readings)[-2:]
        consecutive = (
            len(recent) == 2
            and recent[0].text == code
            and recent[1].text == code
            and self.sample_interval > 0
            and recent[1].t - recent[0].t <= self.sample_interval * 1.5
        )
        event = CodeEvent(code, t, len(same), low,
                          [(i, a) for i, a in enumerate(alts) if a and conf[i] < self.margin],
                          revised=cell.previous is not None and cell.previous != code,
                          final=confident, consecutive=consecutive)
        if confident:
            cell.emitted = code
        else:
            cell.provisional = code
        return event

    def flush(self):
        return [ev for c in self.cells if (ev := self._maybe_emit(c, self._last_t, flush=True)) is not None]
