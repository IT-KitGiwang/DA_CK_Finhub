"""
Apache Airflow DAG: Smart BigQuery ML Retraining Strategy
Goal: Retrain the BigQuery ML ARIMA forecasting model ONLY when there are
at least N new rows. This optimizes cost and performance by preventing 
excessive retraining on identical data.
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime, timedelta
import os

def load_dotenv_file(path: str = "../../.env") -> None:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line: continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

load_dotenv_file()

# ================= Cấu Hình Chiến Lược =================
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("GCP_BQ_DATASET")
TABLE_ID = os.getenv("GCP_BQ_TABLE_STREAM")
MODEL_ID = os.getenv("GCP_BQ_MODEL")
TRAIN_THRESHOLD_ROWS = int(os.getenv("TRAIN_THRESHOLD_ROWS"))

if not all([PROJECT_ID, DATASET_ID, TABLE_ID, MODEL_ID, TRAIN_THRESHOLD_ROWS]):
    raise ValueError("Thiếu biến môi trường! Vui lòng cấu hình đầy đủ trong .env")

default_args = {
    'owner': 'finhub_team',
    'depends_on_past': False,
    'email_on_failure': ['ds@finhub.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bqml_smart_retrain_pipeline',
    default_args=default_args,
    description='Retrain model if row threshold is met (Cost & Perf Optimization)',
    schedule_interval='@hourly', # Quét kiểm tra mỗi giờ, nhưng chỉ chạy nếu thỏa mãn ShortCircuit
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

def check_new_rows_threshold(**context):
    """
    Kiểm tra xem dữ liệu có sinh thêm quá ngưỡng N dòng kể từ lần Train cuối.
    Sử dụng Metadata nội bộ BigQuery để tiết kiệm chi phí Query (Scan=0 Bytes).
    """
    hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    
    # 1. Lấy tổng số dòng hiện tại của table cực nhanh qua bảng __TABLES__ (Cost = 0)
    query_current_rows = f"""
        SELECT row_count 
        FROM `{PROJECT_ID}.{DATASET_ID}.__TABLES__`
        WHERE table_id = '{TABLE_ID}'
    """
    current_rows = hook.get_first(query_current_rows)[0]
    
    # 2. Ở hệ thống thực tế, bạn sẽ lưu `last_trained_rows` vào Variable của Airflow
    from airflow.models import Variable
    last_trained_rows = int(Variable.get("bq_model_last_trained_rows", default_var=0))
    
    delta = current_rows - last_trained_rows
    logging.info(f"Dòng hiện tại: {current_rows} | Dòng đợt trước: {last_trained_rows} | Tăng thêm: {delta}")
    
    if delta >= TRAIN_THRESHOLD_ROWS:
        logging.info("Đã đạt Ngưỡng dòng mới -> Cho phép Tái Train Mô Hình.")
        # Cập nhật số dòng đang có trước khi train
        Variable.set("bq_model_last_trained_rows", current_rows)
        return True
    else:
        logging.info("Chưa đủ lượng dữ liệu thay đổi -> Dừng tiến trình để TIẾT KIỆM CHI PHÍ.")
        return False

# Node 1: Kiểm Tra Điều Kiện Thông Minh (Tiết kiệm Token & Chi Phí Vertex / BQ)
check_threshold = ShortCircuitOperator(
    task_id='check_row_count_threshold',
    python_callable=check_new_rows_threshold,
    provide_context=True,
    dag=dag,
)

# Node 2: Kích hoạt Retrain BQML Model bằng BigQuery Job (Chỉ chạy khi Node 1 is True)
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

check_threshold >> retrain_model_task
