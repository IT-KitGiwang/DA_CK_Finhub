# HƯỚNG DẪN TRIỂN KHAI THỰC TẾ (STEP-BY-STEP MIGRATION TO GCP)

Để biến mã nguồn và kiến trúc của bạn thành một mô hình đang chạy thực tế trên Google Cloud Platform kết hợp với cụm Local Docker, hãy thực hiện tuần tự đúng 7 bướcc sau đây:

---

## BƯỚC 1: Khởi tạo Máy chủ Đám mây (GCP Project)

1. Truy cập [Google Cloud Console](https://console.cloud.google.com/).
2. Nhấn vào nút danh sách Project ở góc trên bên trái -> **New Project** (Tạo dự án mới). Đặt tên là `real-estate-finhub`, hệ thống sẽ cấp cho bạn một **Project ID** (VD: `real-estate-finhub-123456`).
3. Truy cập Menu ≡ -> **APIs & Services** -> **Enable APIs and Services**. Bật (Enable) các thẻ sau:
   - *BigQuery API*
   - *Cloud Storage API*
   - *Cloud Pub/Sub API*
   - *Vertex AI API* (Dành cho ML).

---

## BƯỚC 2: Cấp Quyền & Lấy Chìa Khóa Server (IAM & Service Account)

Spark ở trên máy Docker của bạn cần "Chìa khóa" để chui được vào Data Center của Google.
1. Tại GCP Console, vào **IAM & Admin** -> **Service Accounts**.
2. Nhấn **Create Service Account** -> Tên: `finhub-hybrid-agent` -> Create.
3. Chỗ cấp quyền (Roles), hãy thêm 3 quyền cực kỳ quan trọng sau:
   - `BigQuery Admin` (Để Spark tự tạo bảng)
   - `Storage Admin` (Để Spark tạo Data Lake Parquet)
   - `Pub/Sub Admin` (Nếu xài Kafka bridge).
4. Nhấn Done. Click vào tài khoản vừa tạo -> Trỏ tab **Keys** -> **Add Key** -> **Create new key** -> Trích xuất định dạng **JSON**.
5. Đổi tên file tải về thành `gcp-sa.json`. Copy thả nó vào thư mục `config/` (Ví dụ `e:\JOURNEY DATA ENGINEERING\ptdl\config\gcp-sa.json`).

---

## BƯỚC 3: Chuẩn Bị Môi Trường Local (.env)

Mở siêu tệp `.env` ở thư mục gốc của bạn (`ptdl\.env`) và điền đầy đủ thông tin bạn vừa thiết lập:

```env
GCP_PROJECT_ID=real-estate-finhub-123456
GOOGLE_APPLICATION_CREDENTIALS=config/gcp-sa.json

GCP_GCS_BUCKET=finhub-datalake-prod  # Tên bucket ko được trùng nhe
GCP_BQ_DATASET=finhub_dw
GCP_BQ_TABLE_STREAM=streaming_crypto_trades
GCP_PUBSUB_TOPIC=crypto_stream_hybrid

GCP_BQ_MODEL=crypto_price_forecaster
TRAIN_THRESHOLD_ROWS=100000

OPENROUTER_API_KEY=sk-or-v1-xxx-cua-ban
```

---

## BƯỚC 4: Tạo Vùng Đất GCS (Data Lake) & BQ Dataset

1. **Storage:** Lên GCP Console -> **Cloud Storage** -> **Buckets** -> **Create**. Đặt tên y như biến `.env` (`finhub-datalake-prod`). Cứ để phân vùng mặc định (Multi-region / US). 
2. **BigQuery:** Lên GCP Console -> **BigQuery** -> **SQL Workspace**. Cạnh tên project ID của bạn có dấu 3 chấm -> *Create Dataset*. Nhập Dataset ID là `finhub_dw` và ấn Lưu. (Không cần thủ công tạo Bảng, Spark sẽ tự động đúc bảng cho bạn bằng Schema tự động).

---

## BƯỚC 5: Chạy Pipeline Nạp Dữ Liệu Dạng Lai (Hybrid Execution)

1. **Bật cụm Local:**
Mở PowerShell tại e:\JOURNEY DATA ENGINEERING\ptdl:
```bash
docker-compose up -d master slave1 slave2 kafka
```
*(Bạn đã có cụm Kafka / Spark cực mạnh nằm cục bộ để đệm tín hiệu).*

2. **Chọc mồi Finnhub Streaming:**
Bắt đầu lấy data liên tục từ Mỹ:
```bash
python src/kafka/producer.py
```
*(Kafka lúc này đang bị nhồi data thị trường liên tục).*

3. **Bắn Spark Lên Không Gian GCP:**
Mở 1 Terminal khác. Chúng ta chạy bộ xử lý siêu đẳng `spark_gcp_processor` mà mình đã viết để bắt sóng Kafka và bắn song song 2 đường lên Google:
*(Cần cài Pyspark vào môi trường Python máy bạn trước: `pip install pyspark kafka-python google-cloud-pubsub google-cloud-bigquery`)*
```bash
python src/cloud/spark_gcp_processor.py
```
*Lưu ý: Script auto nhồi `--packages` trong code nên sẽ mất 1 phút ở lần chạy đầu để Spark kéo connector tải từ Server Apache xuống.*

---

## BƯỚC 6: Trực Quan Hóa Tới Chuẩn Enterprise (BI Dashboard)

Pipeline đang chạy, data bay liên tiếp về Google Data Center mỗi chục giây một micro-batch. Giờ bạn đi nghiệm thu:
1. Mở **GCP BigQuery**, vào `finhub_dw` -> Bấm nút **Preview** bảng `streaming_crypto_trades`. Bạn sẽ thấy row nhảy realtime ở đây!
2. Mở web **[Google Looker Studio](https://lookerstudio.google.com/)**, tạo **Blank Report**.
3. Ở Tab Add Data -> Chọn **BigQuery** -> Tìm đến Project ID của bạn -> Bấm chọn bảng `streaming_crypto_trades` -> **Add**.
4. Quăng thử 1 biểu đồ đường (Line chart): X axis = `timestamp`, Y axis = `price`, Breakdown by = `symbol`. Góc trên biểu đồ -> Chọn Auto-refresh (Tự động cập nhật mỗi phút). XONG! Một Dashboard đẳng cấp quốc tế của Enterprise!

---

## BƯỚC 7: Vận Hành Khối Machine Learning & AI

Bây giờ biểu đồ có rồi, số liệu có rồi. Hãy cho Robot tài chính phân tích:

1. Phân tích giá cả và Pump/Dump tự động (Thay thế giám đốc phân tích):
```bash
python src/cloud/ai_financial_analyst.py
```
-> Code sẽ cựa quậy, đọc BQ, và nã OpenRouter. Trên Terminal của bạn sẽ xuất hiện nguyên 1 report phân tích ngôn ngữ tự nhiên cực chuyên sâu cảnh báo các rủi ro.

2. Build Auto Retraining (Airflow):
Nếu bạn đã setup cụm Apache Airflow cục bộ, hãy copy tệp `src/cloud/airflow_bqml_retrain_dag.py` qua thư mục `dags/` của Airflow.
Và cứ đúng 1 giờ Airflow sẽ kiểm tra BigQuery 1 lần, nếu data tăng đủ 100K dòng, nó kích hoạt **BigQuery ML** huấn luyện lại mô hình dự báo Time-series ngay lập tức trên Server của Google mà chả tốn 1 mg RAM máy nhà của bạn!
