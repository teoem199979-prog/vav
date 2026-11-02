"""LLwin slider handling for async Playwright pages.

OpenCV detection runs off the event loop; page operations stay on it.
Only a trusted positive gap permits a drag, and cancellation releases the mouse.
Per-submit debug directories keep parallel attempts independent.
The foreground helper and window lock are also used by browser_engine.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import time
import weakref
from pathlib import Path

logger = logging.getLogger("autobot.llwin_slider")
_OS_MOUSE_LOCK = asyncio.Lock()
_LLWIN_SOLVE_LOCK = asyncio.Lock()
_LLWIN_SOLVE_ATTEMPTS = weakref.WeakSet()

GOCAPTCHA = {
    "wrapper": ".gocaptcha-module_wrapper__Kpdey",
    "body": ".gocaptcha-module_body__KJKNu",
    "picture": ".gocaptcha-module_picture__LRwbY",
    "tile_class": ".index-module_tile__8pkQD",
    "drag_block": ".gocaptcha-module_dragBlock__bFlwx",
    "drag_bar": ".gocaptcha-module_dragSlideBar__noauW",
    "icon_block": ".gocaptcha-module_iconBlock__mVB8B",
}


def _match_edge_template(edges, piece_box, pad: int):
    """Match one padded edge template; None when it has no edges / no peak."""
    import cv2
    import numpy as np

    px, py, pw, ph = [int(v) for v in piece_box]
    h, w = edges.shape[:2]
    ex = max(0, px - pad)
    ey = max(0, py - pad)
    ex2 = min(w, px + pw + pad)
    ey2 = min(h, py + ph + pad)
    templ = edges[ey:ey2, ex:ex2]
    if templ.size == 0:
        return None
    density = float(templ.mean()) / 255.0
    if density == 0:
        return None
    res = cv2.matchTemplate(edges, templ, cv2.TM_CCOEFF_NORMED)
    res2 = res.copy()
    x0 = max(0, ex - 20)
    y0 = max(0, ey - 20)
    x1 = min(res2.shape[1], ex2 + 20)
    y1 = min(res2.shape[0], ey2 + 20)
    res2[y0:y1, x0:x1] = -1.0
    flat = np.argsort(res2.ravel())[::-1]
    best = None
    second = None
    for idx in flat[:200]:
        y, x = np.unravel_index(idx, res2.shape)
        score = float(res2[y, x])
        if abs(y - ey) <= 15:
            gap_x = int(x) + (px - ex)
            gap_y = int(y) + (py - ey)
            if best is None:
                best = (gap_x, gap_y, score)
            elif second is None and abs(gap_x - best[0]) > 10:
                second = (gap_x, gap_y, score)
                break
    if best is None:
        return None
    gap_x, gap_y, score = best
    return {
        "piece_x": px, "piece_y": py, "piece_w": pw, "piece_h": ph,
        "gap_x": gap_x, "gap_y": gap_y,
        "dx": int(gap_x - px), "dy": int(gap_y - py),
        "score": score,
        "second_score": second[2] if second else None,
        "image_w": int(w), "image_h": int(h),
        "edge_density": density,
    }


def _find_hole_contour(edges, piece_box):
    """Find the missing-piece hole directly in the picture.

    The hole exists even when the tile crop has no edges at all (flat
    tile, blurred frame): its outline is a closed contour in the
    picture, about the size of the tile, on the same row, to the
    right of the tile. Grayscale template matching is NOT used here
    on purpose: the hole is empty, so the tile's pixels cannot match
    anything at the gap location.
    """
    import cv2

    px, py, pw, ph = [int(v) for v in piece_box]
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL,
                                   cv2.CHAIN_APPROX_SIMPLE)
    candidates = []
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        if x < px + pw // 2:
            continue
        if abs((y + h / 2) - (py + ph / 2)) > 20:
            continue
        if not (0.55 <= w / pw <= 1.45 and 0.55 <= h / ph <= 1.45):
            continue
        area = cv2.contourArea(cnt)
        if not (0.4 * pw * ph <= area <= 1.6 * pw * ph):
            continue
        if w * h == 0 or area / (w * h) < 0.4:
            continue
        size_match = 1.0 - min(1.0, (abs(w - pw) / pw + abs(h - ph) / ph) / 2)
        y_match = 1.0 - abs((y + h / 2) - (py + ph / 2)) / 20.0
        candidates.append((0.6 * size_match + 0.4 * y_match, x, y, w, h))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    score, x, y, w, h = candidates[0]
    return {
        "piece_x": px, "piece_y": py, "piece_w": pw, "piece_h": ph,
        "gap_x": int(x), "gap_y": int(y),
        "dx": int(x - px), "dy": int(y - py),
        "score": float(score),
        "second_score": None,
        "image_w": int(edges.shape[1]), "image_h": int(edges.shape[0]),
        "edge_density": 0.0,
        "hole_candidates": len(candidates),
    }


def _find_gap_from_png(png_bytes: bytes, piece_box: tuple[int, int, int, int]) -> dict:
    """Edge-template gap detection, sync (runs in a thread).

    Ladder, in order, on the SAME frame -- a single blurred frame no
    longer kills the whole solve:
      1. edge template, Canny 50/150 (original behaviour);
      2. edge template, Canny 30/90 (blurred / low-contrast picture);
      3. same, with a 3px padded template (tile box inset, outline cut off);
      4. Canny 20/60, padded (very blurred frame);
      5. hole contour in the picture (flat tile: template has no edges).
    Raises only when every step fails, with the diagnostics the next
    log line needs instead of the bare "Template vien rong".
    """
    import cv2
    import numpy as np

    data = np.frombuffer(png_bytes, dtype=np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("Khong giai ma duoc anh captcha")
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    px, py, pw, ph = [int(v) for v in piece_box]
    if px < 0 or py < 0 or px + pw > img.shape[1] or py + ph > img.shape[0]:
        raise ValueError(f"piece_box {piece_box} vuot khoi anh {img.shape[::-1]}")
    best = None
    best_density = 0.0
    for low, high, pad in ((50, 150, 0), (30, 90, 0), (30, 90, 3), (20, 60, 3)):
        edges = cv2.Canny(cv2.GaussianBlur(gray, (3, 3), 0), low, high)
        cand = _match_edge_template(edges, piece_box, pad)
        if cand is not None:
            best_density = max(best_density, cand["edge_density"])
            if cand["dx"] > 0 and (best is None or cand["score"] > best["score"]):
                cand = dict(cand)
                cand["method"] = f"edge-canny{low}-{high}-pad{pad}"
                best = cand
    if best is not None and best["score"] >= 0.20:
        return best
    edges_low = cv2.Canny(cv2.GaussianBlur(gray, (3, 3), 0), 30, 90)
    hole = _find_hole_contour(edges_low, piece_box)
    if hole is not None and hole["dx"] > 0 and hole["score"] >= 0.20:
        hole = dict(hole)
        hole["method"] = "hole-contour"
        hole["edge_density"] = best_density
        return hole
    best_score = best["score"] if best else None
    raise ValueError(
        "Khong tim thay gap: template_edge_density="
        f"{best_density:.4f} best_edge_score={best_score} "
        f"hole_candidates={hole['hole_candidates'] if hole else 0} "
        "(anh qua mo / box sai / tile khong co vien)"
    )


async def _tile_locator(page):
    for sel in (GOCAPTCHA["tile_class"],
                ".gocaptcha-module_body__KJKNu div:has(img[src^='data:image/png'])"):
        loc = page.locator(sel).first
        try:
            if await loc.count() > 0 and await loc.is_visible():
                return loc
        except Exception:  # noqa: BLE001 - thu selector tiep theo
            continue
    return page.locator(GOCAPTCHA["tile_class"]).first


async def _detect_gap_with_retries(page, body_loc, tile_loc, *, debug_dir=None,
                                   max_frames: int = 5, interval_s: float = 0.3):
    """Screenshot the slider until the picture is sharp enough to trust.

    The old code took ONE screenshot the instant the wrapper appeared:
    a blurred first frame (edge template empty) failed the whole solve
    in under a second. This helper re-screenshots up to max_frames times
    and only returns a gap with score >= 0.20 and dx > 0 -- a gap that
    can be dragged, never a zero-distance down/up that the site would
    see as a click on the handle. On failure it raises with diagnostics
    and touches no mouse at all.
    """
    import cv2
    import numpy as np

    last_exc = None
    best = None
    for frame in range(max_frames):
        body_box = await body_loc.bounding_box()
        tile_box = await tile_loc.bounding_box()
        if not (body_box and tile_box):
            raise RuntimeError("Missing LLWIN slider bounding boxes")
        png = await body_loc.screenshot()
        if debug_dir is not None:
            dbg = Path(debug_dir)
            dbg.mkdir(parents=True, exist_ok=True)
            (dbg / "gocap_retry_last.png").write_bytes(png)
        image = cv2.imdecode(np.frombuffer(png, dtype=np.uint8), cv2.IMREAD_COLOR)
        if image is None or body_box["width"] <= 0:
            raise RuntimeError("Could not decode LLWIN slider image")
        image_scale = image.shape[1] / body_box["width"]
        piece = (
            round((tile_box["x"] - body_box["x"]) * image_scale),
            round((tile_box["y"] - body_box["y"]) * image_scale),
            round(tile_box["width"] * image_scale),
            round(tile_box["height"] * image_scale),
        )
        try:
            det = await asyncio.to_thread(_find_gap_from_png, png, piece)
        except Exception as exc:  # noqa: BLE001 - retry on the next frame
            last_exc = exc
            det = None
        if det is not None and det["score"] >= 0.20 and det["dx"] > 0:
            logger.warning(
                "LLWIN slider gap: method=%s score=%.2f dx=%s frame=%s/%s",
                det.get("method"), det["score"], det["dx"], frame + 1, max_frames)
            return det, png, body_box, tile_box, image_scale, frame
        if det is not None and (best is None or det["score"] > best[0]["score"]):
            best = (det, png, body_box, tile_box, image_scale)
        if frame + 1 < max_frames:
            await asyncio.sleep(interval_s)
    if best is not None:
        det = best[0]
        raise RuntimeError(
            f"LLWIN slider gap khong tin cay: method={det.get('method')} "
            f"score={det['score']:.2f} dx={det['dx']} - khong cham chuot")
    raise RuntimeError(f"LLWIN slider gap khong tim duoc: {last_exc}")


async def _move_eased(page, x0: float, y0: float, from_off: float, to_off: float,
                      delay_scale: float = 0.5) -> float:
    dist = to_off - from_off
    if abs(dist) < 0.5:
        return to_off
    # Tiny corrections should not cost eight CDP round trips.
    steps = max(2, min(28, math.ceil(abs(dist) / 5)))
    for i in range(1, steps + 1):
        t = i / steps
        e = 1 - pow(1 - t, 3)
        await page.mouse.move(x0 + from_off + dist * e,
                              y0 + random.uniform(-1.0, 1.0), steps=1)
        await asyncio.sleep(random.uniform(0.006, 0.014) * delay_scale)
    return to_off


async def is_llwin_slider_visible(page) -> bool:
    """True only if the GoCaptcha wrapper is actually on screen."""
    try:
        loc = page.locator(GOCAPTCHA["wrapper"]).first
        return await loc.count() > 0 and await loc.is_visible()
    except Exception:  # noqa: BLE001
        return False


def _request_window_foreground(user32, hwnd, timeout: float = .30) -> bool:
    """Allow an accepted cross-thread activation to finish before checking it."""
    if user32.GetForegroundWindow() == hwnd:
        return True
    if not user32.SetForegroundWindow(hwnd):
        return user32.GetForegroundWindow() == hwnd
    deadline = time.monotonic() + timeout
    while user32.GetForegroundWindow() != hwnd:
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(.01, remaining))
    return True


async def _solve_llwin_slider_locked(page, *, max_attempts: int = 2,
                                     debug_dir: str | Path | None = None,
                                     on_manual_required=None) -> bool:
    """Solve one open GoCaptcha slider on an async Playwright page.

    Returns True when the wrapper disappears after a drag (the
    solver's success signal, same as the standalone version). In manual
    mode, wait for the user to solve the challenge without mouse automation.
    Raises on missing boxes / unreadable images so the caller can
    log the failure instead of silently treating it as a code result.
    Automatic mode uses Playwright directly. If it fails, the caller can
    retain the page in the PENDING_VERIFICATION watcher.
    """
    from config import Config

    if bool(getattr(Config, "LLWIN_MANUAL_SLIDER_MODE", True)):
        await page.bring_to_front()
        if on_manual_required is not None:
            await on_manual_required()
        print("WAITING FOR MANUAL SLIDER...", flush=True)
        deadline = asyncio.get_running_loop().time() + 60.0
        while asyncio.get_running_loop().time() < deadline:
            if not await is_llwin_slider_visible(page):
                return True
            await asyncio.sleep(0.5)
        return False

    logger.info("LLWIN slider mode=playwright-direct")
    return await _solve_llwin_slider_playwright(page, max_attempts=max_attempts, debug_dir=debug_dir)


def _llwin_slider_was_attempted(page) -> bool:
    return page in _LLWIN_SOLVE_ATTEMPTS


def _clear_llwin_slider_attempt(page) -> None:
    _LLWIN_SOLVE_ATTEMPTS.discard(page)


async def solve_llwin_slider(page, *, max_attempts: int = 2,
                             debug_dir: str | Path | None = None,
                             on_manual_required=None) -> bool:
    """Serialize slider attempts and retain verification state across callers."""
    queued_at = time.perf_counter()
    async with _LLWIN_SOLVE_LOCK:
        logger.info('[LLWIN-SLIDER-TIMING] phase=lock_wait elapsed_ms=%.1f',
                    (time.perf_counter() - queued_at) * 1000)
        _LLWIN_SOLVE_ATTEMPTS.add(page)
        return await _solve_llwin_slider_locked(
            page, max_attempts=max_attempts, debug_dir=debug_dir,
            on_manual_required=on_manual_required,
        )


async def _solve_llwin_slider_playwright(page, *, max_attempts: int = 2,
                                        debug_dir: str | Path | None = None) -> bool:
    """Use Playwright's page mouse and the existing gap correction."""
    ratio = 1.0
    dbg = Path(debug_dir) if debug_dir else None
    if dbg is not None:
        dbg.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, max_attempts + 1):
        prepare_started = time.perf_counter()
        await page.wait_for_selector(GOCAPTCHA["wrapper"], timeout=10000)
        await page.wait_for_selector(GOCAPTCHA["body"], timeout=10000)
        body_loc = page.locator(GOCAPTCHA["body"])
        tile_loc = await _tile_locator(page)
        drag_loc = page.locator(GOCAPTCHA["drag_block"])
        drag_box = await drag_loc.bounding_box()
        if not drag_box:
            raise RuntimeError("Khong lay duoc bounding_box GoCaptcha "
                               "(wrapper/body/tile/drag_block sai?)")
        await page.wait_for_timeout(120)
        det, png_bytes, body_box, tile_box, dpr, _frame = (
            await _detect_gap_with_retries(
                page, body_loc, tile_loc, debug_dir=debug_dir, max_frames=4,
            )
        )
        logger.info('[LLWIN-SLIDER-TIMING] phase=prepare attempt=%s elapsed_ms=%.1f frames=%s',
                    attempt, (time.perf_counter() - prepare_started) * 1000, _frame + 1)
        # Re-read the handle AFTER the gap frames: a stale handle box is
        # how a drag turns into a down/up on the wrong interface element.
        drag_box = await drag_loc.bounding_box() or drag_box
        if dbg is not None:
            (dbg / f"gocap_live_{attempt}.png").write_bytes(png_bytes)
        import cv2 as _cv2
        import numpy as _np
        if dbg is not None:
            import cv2 as _cv2w
            vis = _cv2w.imdecode(_np.frombuffer(png_bytes, dtype=_np.uint8),
                                 _cv2w.IMREAD_COLOR)
            _cv2w.rectangle(vis, (det["piece_x"], det["piece_y"]),
                            (det["piece_x"] + det["piece_w"],
                             det["piece_y"] + det["piece_h"]), (0, 0, 255), 2)
            _cv2w.rectangle(vis, (det["gap_x"], det["gap_y"]),
                            (det["gap_x"] + det["piece_w"],
                             det["gap_y"] + det["piece_h"]), (0, 255, 0), 2)
            _cv2w.imwrite(str(dbg / f"gocap_live_{attempt}_debug.png"), vis)
        if det["score"] < 0.20 or det["dx"] <= 0:
            # No mouse.down here: a zero-distance down/up is a click on
            # the handle, not a drag, and it can invalidate the captcha.
            logger.warning("LLWIN slider gap not reliable; keeping puzzle for manual verification")
            return False
        target_tile_x = body_box["x"] + det["gap_x"] / dpr
        dx_vp = det["dx"] / dpr
        cx = drag_box["x"] + drag_box["width"] / 2
        cy = drag_box["y"] + drag_box["height"] / 2
        tile0_x = tile_box["x"]
        await page.mouse.move(cx, cy, steps=3)
        delay_scale = 0.96
        pressed = False
        drag_started = time.perf_counter()
        try:
            pressed = True  # Release even if the down acknowledgement is lost.
            await page.mouse.down()
            await asyncio.sleep(0.05)
            cur = await _move_eased(page, cx, cy, 0, dx_vp / ratio, delay_scale)
            await page.wait_for_timeout(90)
            tile_cur = await tile_loc.bounding_box()
            if tile_cur:
                tile_delta = tile_cur["x"] - tile0_x
                if abs(cur) > 5 and abs(tile_delta) > 1:
                    ratio = tile_delta / cur
                for _ in range(2):
                    tile_cur = await tile_loc.bounding_box()
                    if not tile_cur:
                        break
                    residual = target_tile_x - tile_cur["x"]
                    if abs(residual) <= 2:
                        break
                    cur = await _move_eased(page, cx, cy, cur,
                                            cur + residual / ratio, delay_scale)
                    await page.wait_for_timeout(90)
            cur = await _move_eased(page, cx, cy, cur, cur + 3, delay_scale)
            await page.wait_for_timeout(30)
            cur = await _move_eased(page, cx, cy, cur, cur - 3, delay_scale)
        finally:
            if pressed:
                await page.mouse.up()
            logger.info('[LLWIN-SLIDER-TIMING] phase=drag attempt=%s elapsed_ms=%.1f',
                        attempt, (time.perf_counter() - drag_started) * 1000)
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        result_started = time.perf_counter()
        try:
            await page.locator(GOCAPTCHA["wrapper"]).wait_for(state='hidden', timeout=1500)
        except PlaywrightTimeoutError:
            logger.info('[LLWIN-SLIDER-TIMING] phase=verification attempt=%s elapsed_ms=%.1f hidden=False',
                        attempt, (time.perf_counter() - result_started) * 1000)
            logger.warning("LLWIN slider remains visible; keeping puzzle for manual verification")
            return False
        else:
            logger.info('[LLWIN-SLIDER-TIMING] phase=verification attempt=%s elapsed_ms=%.1f hidden=True',
                        attempt, (time.perf_counter() - result_started) * 1000)
            return True
    return False
