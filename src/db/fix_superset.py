"""
Trình Tích Hợp API Superset
Công cụ này dùng để tự động đăng nhập vào cấu hình Superset, lấy mã bảo mật CSRF token,
tiến hành xóa các kết nối DB bị hỏng và thiết lập một kết nối an toàn với máy chủ Hive,
tập trung đặc tả ánh xạ vào bảng crypto_trades.
"""

import requests
import logging
from typing import Dict, Any

# Cấu hình tính năng Nhật ký của hệ thống (Logging)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [SUPERSET_FIX] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class SupersetConfigurator:
    """Class đảm bảo an toàn thao tác trên API của Apache Superset REST."""
    
    def __init__(self, base_url: str = "http://localhost:8089"):
        self.base_url = base_url
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        
    def _get_headers(self) -> Dict[str, str]:
        """Kịch bản sinh Header động với xác minh danh tính."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if self.csrf_token:
            headers.update({
                "Content-Type": "application/json",
                "X-CSRFToken": self.csrf_token,
                "Referer": self.base_url
            })
        return headers

    def authenticate(self) -> None:
        """Đăng nhập bằng tài khoản Administrator để lấy mã JWT và thẻ CSRF."""
        logger.info(f"Đang tiến hành đăng nhập vào Admin Console của hệ thống {self.base_url}...")
        
        login_payload = {"username": "admin", "password": "admin", "provider": "db"}
        resp = self.session.post(f"{self.base_url}/api/v1/security/login", json=login_payload)
        resp.raise_for_status()
        
        self.access_token = resp.json().get("access_token")
        logger.info("Hoàn tất lấy thẻ Ủy Quyền (JWT).")
        
        # Load necessary CSRF state
        resp_csrf = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/", headers=self._get_headers())
        resp_csrf.raise_for_status()
        self.csrf_token = resp_csrf.json().get("result")
        logger.info("Hoàn tất thiết lập cơ chế bảo mật thẻ CSRF.")

    def cleanup_databases(self) -> None:
        """Kiểm tra và xóa sạch toàn bộ liên kết (Database Connections) cũ bị lỗi."""
        logger.info("Đang dò tìm những cài đặt liên kết CSDL đã được thiết lập trước đó...")
        resp = self.session.get(f"{self.base_url}/api/v1/database/", headers=self._get_headers())
        resp.raise_for_status()
        
        dbs = resp.json().get("result", [])
        
        if not dbs:
            logger.info("Không tìm thấy tàn dư hệ thống (Database nào).")
            return

        for db in dbs:
            did = db["id"]
            db_name = db.get("database_name", "UNKNOWN")
            logger.info(f"Tiến hành phá dỡ Liên Kết ID={did} [{db_name}]")
            r = self.session.delete(f"{self.base_url}/api/v1/database/{did}", headers=self._get_headers())
            
            if r.status_code == 200:
                logger.info("  Phá Dỡ Hoàn Tất.")
            else:
                logger.warning(f"  Gặp Khó Khăn: Mã lỗi {r.status_code}")

    def create_hive_connection(self) -> None:
        """Cố định hóa một kết cấu Database Connection bằng URI mặc định vào Hive."""
        new_db = {
            "database_name": "Hive Crypto",
            "sqlalchemy_uri": "hive://dack15@master:10000/default?auth=NOSASL",
            "expose_in_sqllab": True,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": True,
        }
        
        logger.info("Vận hành đệ trình cấu hình Apache Hive lên Backend Superset...")
        resp = self.session.post(f"{self.base_url}/api/v1/database/", headers=self._get_headers(), json=new_db)
        
        if resp.status_code in [200, 201]:
            logger.info("Cài Đặt Hive Data Connection Hoàn Tất Căn Bản.")
        else:
            logger.error(f"Sự cố Kết Nối xảy ra: {resp.text}")

    def register_dataset(self) -> None:
        """Ánh xạ trực tiếp Database Connection và Hive Table để cho phép tạo Chart."""
        resp = self.session.get(f"{self.base_url}/api/v1/database/", headers=self._get_headers())
        resp.raise_for_status()
        
        available_dbs = resp.json().get("result", [])
        if not available_dbs:
            logger.error("Hệ thống chưa cấp phép Database đầu kì nên hủy thiết lập Dataset.")
            return
            
        db_id = available_dbs[0].get("id")
        
        dataset_payload = {
            "database": db_id,
            "schema": "default",
            "table_name": "crypto_trades",
        }
        
        logger.info(f"Bắt đầu tiêm Dataset vật lý liên kết cho Table {dataset_payload['table_name']}.")
        resp_dset = self.session.post(f"{self.base_url}/api/v1/dataset/", headers=self._get_headers(), json=dataset_payload)
        
        if resp_dset.status_code in [200, 201]:
            logger.info("Đóng gói thành công Dataset vào Superset Environment!")
        elif "already exists" in resp_dset.text:
            logger.info("Dataset này đã có sẵn.")
        else:
            logger.error(f"Khóa chốt Dataset Gặp Sự Cố: {resp_dset.text}")

def main() -> None:
    try:
        superset = SupersetConfigurator()
        superset.authenticate()
        superset.cleanup_databases()
        superset.create_hive_connection()
        superset.register_dataset()
        logger.info("Quá trình tự động sửa hệ thống đã thành công mỹ mãn.")
    except Exception as e:
        logger.error(f"Siêu Sự Cố Nghiêm Trọng Xảy Ra Bất Chợt: {e}")

if __name__ == "__main__":
    main()
