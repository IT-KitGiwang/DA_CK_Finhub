# Hướng Dẫn Triển Khai Hạ Tầng Google Cloud (Hybrid Big Data Pipeline)

Tài liệu này đề xuất thiết kế kiến trúc và giải pháp chi tiết nhằm nâng cấp hệ thống **Real Estate Finhub** từ "Cục bộ (Local Docker / On-Premise)" lên "Lai (Hybrid Cloud)" dựa trên tiêu chuẩn Doanh nghiệp (Enterprise Standards).

## 1. Mục Tiêu Kiến Trúc

- **Tái sử dụng hạ tầng có sẵn:** Giữ nguyên các container Docker (Hadoop HDFS, Spark, Kafka) nội bộ làm vùng đệm (Edge computing / Local buffering), tối ưu chi phí truyền dẫn liên tục.
- **Lưu trữ dài hạn (Data Lake):** Đẩy dữ liệu thô / dạng Parquet lên **Google Cloud Storage (GCS)**, chi phí lưu trữ thấp và độ bền dữ liệu 99.999999999%.
- **Kho dữ liệu chuẩn (Data Warehouse):** Sử dụng **Google BigQuery** cho phân tích Data Warehouse cực nhanh, xử lý PetaByte trong vài giây. Hỗ trợ truy vấn SQL chuẩn.
- **Mở rộng tương lai:** Hệ sinh thái mở ra **Cloud Pub/Sub** nếu muốn streaming lên nhiều cloud khác, và **Google Data Studio (Looker Studio)** cho Report trực quan hóa.

---

## 2. Các Dịch Vụ GCP Đề Xuất (GCP Services Map)

1. **Cloud IAM (Identity and Access Management):**
   - Tạo Service Account (`finhub-hybrid-agent@<project>.iam.gserviceaccount.com`).
   - Cấp quyền: `BigQuery Data Editor`, `Storage Object Admin`. Sinh cặp khóa JSON lưu về máy local `.credentials/gcp-sa.json`.
2. **Google Cloud Storage (GCS):**
   - Đóng vai trò làm Data Lake. Tạo bucket (ví dụ: `gs://finhub-datalake-prod`). Tất cả dữ liệu thị trường sẽ được nén định dạng Parquet theo phân vùng thời gian (Năm/Tháng/Ngày) để lưu trữ vĩnh viễn (Cold Storage).
3. **Google BigQuery:**
   - Serverless Data Warehouse. Chứa dataset `finhub_dw`. Bảng `streaming_crypto_trades` sẽ được ghi liên tục từ Spark.
4. **Google Cloud Pub/Sub (Optional - Advanced Streaming):**
   - Forward dữ liệu từ Kafka local lên Pub/Sub nếu muốn kết nối với Dataflow.
5. **Google Dataproc (Optional - Fully managed Hadoop/Spark):**
   - Nếu local Spark quá tải, có thể đẩy thẳng Spark Job lên Dataproc để xử lý song song trên Cloud.

---

## 3. Cách Kết Nối Với Hệ Thống Cũ (Hybrid Bridge)

Luồng Data Pipeline mới:
`Finnhub Socket` -> `Kafka (Local)` -> `Spark Streaming (Local)` -> `(Phân nhánh 2 đường)` 
  - Đường 1: -> Ghi vào HDFS (như cụ)
  - Đường 2: -> Ghi vào `Google Cloud Storage (GCS)` (Lưu trữ Parquet vĩnh viễn)
  - Đường 3: -> Ghi vào `BigQuery` (Phục vụ Superset / BI Query thời gian thực).

---

## 4. Các File Mã Nguồn Được Cung Cấp Tại `src/cloud/`

- `spark_gcp_processor.py`: Cấu trúc lại file Spark, đọc từ Kafka và đẩy trực tiếp lên GCS và BigQuery sử dụng `spark-bigquery-connector` và `gcs-connector`.
- `kafka_to_pubsub.py`: Đoạn mã chạy độc lập lấy dữ liệu từ local Kafka, "bridge" lên Cloud Pub/Sub cho các ứng dụng Serverless của GCP tiêu thụ.

## 5. Hướng dẫn thiết lập Môi trường

Bổ sung thêm các biến sau vào siêu tệp `.env` ở gốc dự án:
```env
# --- GOOGLE CLOUD CONFIGURATION ---
GCP_PROJECT_ID=your-gcp-project-id
GCP_GCS_BUCKET=finhub-datalake-prod
GCP_BQ_DATASET=finhub_dw
GCP_BQ_TABLE_STREAM=streaming_crypto_trades
GCP_BQ_MODEL=crypto_price_forecaster
TRAIN_THRESHOLD_ROWS=100000
GCP_PUBSUB_TOPIC=crypto_stream_hybrid
GOOGLE_APPLICATION_CREDENTIALS=/opt/hadoop/config/gcp-sa.json
```

**Chú ý khi chạy Spark với GCP:**
Cần tải các file jar bổ trợ thả vào thư mục `spark/jars` hoặc truyền qua tham số `--packages`:
- `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2`
- `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14`

---

## 6. Mở Rộng: Trực Quan Hóa (BI) & Học Máy (Machine Learning) trên Cloud

Thế mạnh cực lớn của GCP là khả năng chạy mô hình AI trực tiếp trên cơ sở dữ liệu lớn và các công cụ biểu diễn Data siêu mượt (Dashboard doanh nghiệp). Mình đã thiết kế thêm 2 hướng phát triển cao cấp cho bạn:

**1. Trực quan hoá Hiện Đại - Google Looker Studio:**
- Thay vì chỉ dùng Superset nội bộ khá nặng với Local DB, giờ đây Data Warehouse `finhub_dw.streaming_crypto_trades` đã sẵn sàng trên BigQuery.
- Chỉ việc mở **Looker Studio**, kết nối (Connect) qua Data source là BigQuery, bạn có thể tạo Dashboard Real-time (Auto-refresh) cho biểu đồ Candlestick, Volume với giao diện đẳng cấp quốc tế của Google mà hoàn toàn miễn phí, không bắt tài nguyên Cụm Docker của bạn chạy thêm dịch vụ render biểu đồ!

**2. Machine Learning Trực tiếp vào Data Streaming:**
Mình đã cấu trúc 2 file mới trong thư mục này chuyên trị Machine Learning chứng khoán:

- **`bqml_crypto_analytics.sql`**: 
Sử dụng **BigQuery ML (BQML)**. Chức năng thần kỳ của BigQuery là cho phép dùng cú pháp SQL chuẩn để tạo và gọi các mô hình Machine Learning siêu mạnh mà không cần copy hay luân chuyển dữ liệu đi đâu. Mình đã khai báo:
  * Mô hình `ARIMA_PLUS`: Dự đoán giá 60 phút ở tương lai dựa theo mùa vụ chuỗi thời gian của Dữ liệu giá hiện tại.
  * Mô hình `K-Means`: Đọc luồng real-time và phát hiện Lệnh giao dịch bất thường (Anomaly Detection - Phát hiện thao túng giá, Pump/Dump rác).

- **`vertex_ai_predictor.py`**: 
Sử dụng **Google Vertex AI**. Từ luồng Pub/Sub đẩy lên liên tục ở chế độ Real-time, file này hướng dẫn lập trình viên sử dụng mô hình Deep Learning riêng biệt kết nối vào API Endpoint của Vertex AI. Thích hợp nếu bạn có một Mô hình TensorFlow/PyTorch tinh xảo phức tạp và muốn inference (dự đoán) với tốc độ độ trễ < 50ms (Real-time).

---

## 7. Lập Lịch Retrain Thông Minh & Tích Hợp AI Phân Tích (OpenRouter)

Để tối ưu **Hiệu Suất + Chi Phí** theo yêu cầu, hệ thống sẽ KHÔNG tái huấn luyện (retrain) ML bừa bãi. Máy học sinh ra là để xử lý dữ liệu lớn, việc retrain từng phút sẽ phung phí tiền của. Thay vào đó, chúng ta lập trình luồng tự động hoá Airflow:

**1. Lập lịch Retrain Model Nâng Cao (Apache Airflow):**
- 📂 File **`airflow_bqml_retrain_dag.py`**: 
  - Là một DAG (Directed Acyclic Graph) định kì 1 giờ sẽ đánh thức hệ thống dậy.
  - Sử dụng chiến thuật **ShortCircuit**: Query hàm kiểm tra MetaData của BigQuery. Chỉ khi nào `streaming_crypto_trades` tăng đủ cột mốc **X dòng** (ví dụ 100,000 lượt trade mới được sinh ra), Airflow mới cho phép Pipeline tiến hành cập nhật lại Mô hình dự đoán chuỗi thời gian ARIMA_PLUS.
  - **Lợi ích**: Tuyệt đối tiết kiệm tài nguyên tính toán (cost) và tối đa hóa sức mạnh của BQML.

**2. Gắn kết Robot Phân Tích Tài Chính bằng OpenRouter:**
- 📂 File **`ai_financial_analyst.py`**:
  - Script này khai thác triệt để "dữ liệu chuẩn sau khi qua Cloud".
  - Nó liên tục đọc 5 tín hiệu Streaming biến động gần nhất hoặc cảnh báo Pump/Dump lấy từ BigQuery. Sau đó đúc kết thành **Prompt**.
  - Truyền thẳng Prompt này qua API của **OpenRouter** (sử dụng GPT-4 hoặc Claude 3.5). Kết quả bạn nhận được là một đoạn Báo cáo (Report) hoặc Cảnh báo (Alert Warning) phân tích chỉ dấu bằng ngôn ngữ tự nhiên – hoàn toàn tự động!

**Chú ý cập nhật thêm biến môi trường (AI Analyst)**
Bổ sung đoạn sau vào file `.env`:
```env
# --- AI ANALYST CONFIGURATION ---
OPENROUTER_API_KEY=sk-or-v1-xxxxxxxxxxxxxxxxx
```
