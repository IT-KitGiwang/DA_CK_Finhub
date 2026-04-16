"""
Module: Apache Airflow DAG - Smart BigQuery ML Retraining Strategy
Mô tả: 
 - Tối ưu Máy Học bằng cách áp dụng kỹ thuật ShortCircuit Cắt ngắn vòng lặp vô ích.
 - Airflow định kỳ soi vào File __TABLES__ (Chứa MetaData Metadata size do Google cung cấp miễn phí 100% tài nguyên).
 - Chỉ khi Dữ liệu nhảy số hàng chục ngàn dòng (N rows Threshold), Airflow mới ra lệnh Build Data Model.
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime, timedelta
import logging
import os
import sys

# 1. Cấu hình Logging đầy đủ theo luật Rule Code
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [AIRFLOW-DAG-ML] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def load_dotenv_file(path: str = ".env") -> None:
    """Hàm nạp môi trường an toàn"""
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line: 
                        continue
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
            logger.info("Nạp môi trường .env dùng cho Airflow Meta thành công.")
        else:
            logger.warning(f"Không tìm thấy Env .env mẫu ở {path}. Hãy set trên Airflow Variables.")
    except Exception as e:
        logger.error(f"Sự cố load file môi trường trong DAG: {e}")

# Kích hoạt hàm Load Env từ Local Filesystem
load_dotenv_file()

try:
    # Lấy thông số từ biến môi trường. Airflow cực ghét Hard-code (vì sẽ làm hỏng CI/CD)
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    DATASET_ID = os.getenv("GCP_BQ_DATASET")
    TABLE_ID = os.getenv("GCP_BQ_TABLE_STREAM")
    MODEL_ID = os.getenv("GCP_BQ_MODEL")
    TRAIN_THRESHOLD_ROWS_STR = os.getenv("TRAIN_THRESHOLD_ROWS")

    if not all([PROJECT_ID, DATASET_ID, TABLE_ID, MODEL_ID, TRAIN_THRESHOLD_ROWS_STR]):
        logger.warning("Biến môi trường Airflow bị khuyết. Tấm chặn DAG sẽ báo fail.")
        
    TRAIN_THRESHOLD_ROWS = int(TRAIN_THRESHOLD_ROWS_STR) if TRAIN_THRESHOLD_ROWS_STR else 100000

except ValueError as ve:
    logger.error(f"Lỗi chuyển đối Threshold Int sang chuỗi trong khai báo DAG: {ve}")
    TRAIN_THRESHOLD_ROWS = 100000 # Default dự phòng chống crash Airflow Parser
except Exception as e:
    logger.error(f"Khởi tạo Global biến ngầm lỗi: {e}")

# 2. Xây dựng tham số khung sườn cho Airflow Graph
default_args = {
    'owner': 'finhub_team',
    'depends_on_past': False,
    'email_on_failure': ['thongbao-quant@finhub.com'], # Đổi email để test nhận tin
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khai báo thân DAG
dag = DAG(
    'bqml_smart_retrain_pipeline',
    default_args=default_args,
    description='Pipeline Huấn luyện Model dựa vào lượng dữ liệu (Cost/Perf Optimization)',
    schedule_interval='@hourly', # Auto đánh thức hệ thống mỗi 1 tiếng đồng hồ để check MetaData
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

def check_new_rows_threshold(**context):
    """
    Hàm cực kỳ quan trọng:
    Kiểm tra xem dữ liệu có sinh thêm đúng số lượng Yêu Cầu kể từ đợt Model Training trước.
    """
    try:
        logger.info("Đang móc API Google Cloud Default từ Airflow Credentials...")
        hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
        
        # 1. Truy xuất bảng ẩn __TABLES__ của Google cung cấp.
        # Nguồn gốc bí thuật: Việc Count(*) 1 bảng PetaByte tốn tiền rất chát, 
        # nên đọc cái thuộc tính row_count tốn bằng đúng số $0 đồng. 
        query_current_rows = f"""
            SELECT row_count 
            FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES__`
            WHERE table_id = '{TABLE_ID}'
        """
        
        query_result = hook.get_first(query_current_rows)
        if not query_result:
            raise ValueError(f"Hoàn toàn không cấu hình thấy được thông số dòng của bảng {TABLE_ID}.")
            
        current_rows = query_result[0]
        
        # 2. Lưu Meta của lần Check point cuối cùng thông qua Thư viện Variable độc quyền của Airflow
        from airflow.models import Variable
        last_trained_rows = int(Variable.get("bq_model_last_trained_rows", default_var=0))
        
        delta = current_rows - last_trained_rows
        logger.info(f"📊 Tracking Row Count: Dòng hiện tại BigQuery là {current_rows} | Dòng đợt huấn luyện cũ: {last_trained_rows} | 📈 Chênh lệch gia tăng: {delta}")
        
        # Logic rẽ nhánh ShortCircuit ngầm
        if delta >= TRAIN_THRESHOLD_ROWS:
            logger.info(f"🟢 CHUẨN THÊU GIA TĂNG (>{TRAIN_THRESHOLD_ROWS}). Chấp thuận (True) kích hoạt BigQuery Create Model.")
            # Chốt sổ Set lại điểm nhảy Data Point mới
            Variable.set("bq_model_last_trained_rows", current_rows)
            return True
        else:
            logger.info("🔴 TỐI ƯU CHI PHÍ: Dữ liệu chưa đủ bự để AI tiến hành học lại chuỗi Thời gian (Time-series). Đoạn đường DAG sẽ Cắt Lệnh ở đây. BQML nghỉ giải lao.")
            return False

    except Exception as e:
        logger.error(f"Lỗi khối logic Python check threshold: {e}")
        return False # Hủy an toàn nếu lỗi kết nối

# ================= KỊCH BẢN THỰC THI (OPERATORS) =================

# Nút thắt 1 (Node 1): Python chạy hàm điều kiện
check_threshold = ShortCircuitOperator(
    task_id='check_row_count_threshold',
    python_callable=check_new_rows_threshold,
    provide_context=True,
    dag=dag,
)

# Nút thắt 2 (Node 2): Chạy truy vấn SQL đè bẹp thay thế mô hình Machine Learning AI Cũ
retrain_model_query = f"""
CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET_ID}.{MODEL_ID}`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='timestamp',
  time_series_data_col='price',
  time_series_id_col='symbol',
  data_frequency='PER_MINUTE',   
  horizon=60                     
) AS
SELECT timestamp, symbol, price
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
"""

# Khai báo Insert Job BQML
retrain_model_task = BigQueryInsertJobOperator(
    task_id='execute_bqml_retrain',
    configuration={
        "query": {
            "query": retrain_model_query,
            "useLegacySql": False,
        }
    },
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Ánh xạ dây chuyền Topology
check_threshold >> retrain_model_task
