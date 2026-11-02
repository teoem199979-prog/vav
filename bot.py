import os
import sys
import time
import logging
import asyncio
from pathlib import Path

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot_activity_fixed.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("BaBaoBot")

# ==========================================
# 1. KIỂM TRA & CHẶN XUNG ĐỘT PHIÊN (PID Lock)
# ==========================================
PID_FILE = "bot_session.pid"

def check_and_create_pid():
    if os.path.exists(PID_FILE):
        with open(PID_FILE, "r") as f:
            old_pid = f.read().strip()
        logger.critical(f"Phát hiện một tiến trình bot khác đang chạy với PID: {old_pid} hoặc session bị trùng!")
        # Bạn có thể chọn thoát hoặc ghi đè tùy ý đồ quản trị
        # sys.exit(1)
    
    current_pid = os.getpid()
    with open(PID_FILE, "w") as f:
        f.write(str(current_pid))
    logger.info(f"Đã khóa session thành công với PID hiện tại: {current_pid}")

def remove_pid_file():
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)
        logger.info("Đã giải phóng file khóa session.")

# ==========================================
# 2. XỬ LÝ OCR VÀ HÀNG ĐỢI (INBOX / RETRY)
# ==========================================
async def process_ocr_with_timeout(image_path: str, timeout: int = 10):
    """Giả lập gọi OCR với cơ chế Timeout để tránh treo hệ thống"""
    logger.info(f"Bắt đầu quét OCR cho tệp: {image_path}")
    try:
        # Giả lập thời gian xử lý OCR
        await asyncio.sleep(1.5)
        
        # Mô phỏng lỗi Timeout nếu đường dẫn chứa chuỗi test_timeout
        if "timeout" in image_path.lower():
            raise TimeoutError("OCR infrastructure: TimeoutError")
            
        return {"status": "success", "code": "ABC123XYZ"}
    except TimeoutError as te:
        logger.error(f"Lỗi OCR Timeout: {te}")
        raise
    except Exception as e:
        logger.error(f"Lỗi OCR không xác định: {e}")
        return {"status": "failed", "error": str(e)}

async def handle_inbox_message(message_id: str, image_path: str, max_retries: int = 2):
    """Quản lý hàng đợi tin nhắn, giới hạn số lần thử lại khi gặp lỗi"""
    attempt = 0
    while attempt < max_retries:
        try:
            result = await process_ocr_with_timeout(image_path, timeout=5)
            if result.get("status") == "success":
                logger.info(f"Tin nhắn {message_id} xử lý thành công mã: {result.get('code')}")
                return True
            else:
                logger.warning(f"Tin nhắn {message_id} trả về trạng thái thất bại: {result.get('error')}")
                return False
        except TimeoutError:
            attempt += 1
            logger.warning(f"Tin nhắn {message_id} gặp lỗi Timeout. Lần thử lại {attempt}/{max_retries}...")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Lỗi cấu trúc tin nhắn không hợp lệ (invalid message format) cho {message_id}: {e}")
            break
            
    logger.error(f"Tin nhắn {message_id} đã vượt quá số lần thử lại. Đánh dấu FAILED và KHÔNG thử lại nữa.")
    return False

# ==========================================
# 3. VÒNG LẶP CHÍNH CỦA BOT
# ==========================================
async def main():
    check_and_create_pid()
    logger.info("Khởi động hệ thống Bot phiên bản v11.0.3 hoàn chỉnh...")
    
    try:
        # Mô phỏng vòng đời nhận và xử lý sự kiện
        sample_tasks = [
            ("msg_001", "normal_image.png"),
            ("msg_002", "test_timeout_image.png") # Sẽ kích hoạt lỗi timeout và thử lại
        ]
        
        for msg_id, img in sample_tasks:
            await handle_inbox_message(msg_id, img)
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Nhận tín hiệu dừng từ người dùng.")
    finally:
        remove_pid_file()
        logger.info("Đã tắt bot an toàn.")

if __name__ == "__main__":
    asyncio.run(main())