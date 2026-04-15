# MỤC LỤC BÁO CÁO TOÀN DIỆN: HỆ THỐNG DATA PIPELINE REAL-TIME (LAMBDA ARCHITECTURE)

*Dưới đây là khung sườn (Outline) chi tiết để bạn viết báo cáo cuối kỳ/đồ án. Khung này bao quát toàn bộ hệ thống từ cấu hình DevOps cục bộ (Docker) đến điều phối Dagster, xử lý luồng, và trực quan hóa.*

---

## CHƯƠNG 1: TỔNG QUAN HỆ THỐNG
1.1. Bối cảnh và Mục tiêu dự án (Phân tích dữ liệu tiền điện tử Real-time)
1.2. Lựa chọn kiến trúc hệ thống: Lambda Architecture
1.3. Các công nghệ cốt lõi được sử dụng:
   - Hệ sinh thái Apache: Hadoop (HDFS), Spark Streaming, Hive (Thrift Server), Kafka
   - Điều phối (Orchestration): Dagster
   - Trực quan hóa (BI): Apache Superset
   - Cơ sở hạ tầng: Docker & Docker Compose
1.4. Sơ đồ kiến trúc tổng thể của Data Pipeline *(📸 Chèn ảnh sơ đồ Architecture Diagram)*

## CHƯƠNG 2: XÂY DỰNG VÀ CẤU HÌNH MÔI TRƯỜNG LOCAL (DOCKER)
2.1. Thiết kế cơ sở hạ tầng Container với `docker-compose.yml`
2.2. Cấu hình Cụm Apache Hadoop:
   - NameNode & DataNode (Hệ thống lưu trữ phân tán)
   - Thiết lập các Port giao tiếp (9000, 9870)
2.3. Cấu hình Cụm Apache Spark & Hive:
   - Spark Master & Worker
   - Hive Metastore (Quản lý Schema - Port 9083)
   - Hive Thrift Server (Cổng giao tiếp BI - Port 10000)
2.4. Cấu hình Apache Kafka & Zookeeper (Hàng đợi thông điệp thực thời)
2.5. Cấu hình Apache Superset (Công cụ BI)
2.6. Quy hoạch biến môi trường (`.env`) và quản lý thư viện (`src/jars`)
*(📸 Chèn ảnh chụp `docker ps` thể hiện các container đang chạy khoẻ mạnh)*

## CHƯƠNG 3: CHI TIẾT TỪNG THÀNH PHẦN TRONG DATA PIPELINE
3.1. Phân hệ Data Ingestion (Kafka Producer)
   - Kết nối Finhub API Websocket
   - Đẩy luồng dữ liệu (Streaming Data) vào Kafka Topic
3.2. Phân hệ Xử lý Dữ liệu Thực thời (Spark Streaming)
   - Đọc luồng dữ liệu từ Kafka (`spark-sql-kafka.jar`)
   - Biến đổi (Transformation) dữ liệu cấu trúc JSON
   - Ghi dữ liệu Parquet xuống HDFS (Append Mode)
3.3. Phân hệ Lưu trữ và Data Warehouse (Hive / Hadoop)
   - Thiết kế mô hình dữ liệu đa chiều (Star Schema)
   - Tạo các bảng Dimension (Bảng chiều tĩnh: `dim_exchange`, `dim_symbol`)
   - Bảng Fact thực thời (`crypto_trades`)
   - Xây dựng các Hive Views tổng hợp (`vw_fact_crypto_trades`, `vw_fact_daily_summary`)
   *(📸 Chèn sơ đồ chuẩn Star Schema)*

## CHƯƠNG 4: ĐIỀU PHỐI TỰ ĐỘNG BẰNG DAGSTER (ORCHESTRATION)
4.1. Vai trò của Dagster trong dự án (Thay thế Airflow)
4.2. Khai báo Dependency Graph (Luồng các tác vụ)
4.3. Phân tích chi tiết chuỗi 6 tác vụ (Ops) tự động hoá:
   - `boot_infrastructure`: Đánh thức Docker.
   - `cleanup_old_garbage`: Dọn dẹp Checkpoint & HDFS, đảm bảo tính luỹ đẳng (Idempotency).
   - `activate_thrift_server`: Khởi tạo & nạp mới Hive Thrift Server.
   - `load_dimension_tables` & `config_superset`: Nạp dữ liệu tĩnh và cấu hình DB.
   - `start_realtime_streams`: Kích hoạt tiến trình chạy ngầm Kafka & Spark.
   - `create_star_views`: Đảm bảo đồng bộ hoá (đợi bảng có Data mới tạo Views).
4.4. Hệ thống Cảnh báo tự động (Email Alert Hooks):
   - Cấu hình SMTP bắt rủi ro Pipeline
   *(📸 Chèn ảnh chụp giao diện Dagster UI báo xanh toàn bộ vòng đời)*
   *(📸 Chèn ảnh thông báo Alert gửi về Email)*

## CHƯƠNG 5: HƯỚNG DẪN CHẠY HỆ THỐNG End-to-End (A → Z)
5.1. Các bước chạy lệnh CLI `dagster dev`
5.2. Quan sát tiến trình điều phối (Web UI 3000)
5.3. Giám sát luồng thông điệp từ WebSocket vào Kafka qua Console Logs
5.4. Xác minh dữ liệu sinh ra tại thư mục HDFS (`hdfs dfs -ls`)
5.5. Truy vấn kiểm soát chất lượng dữ liệu bằng Hive Beeline CLI
*(📸 Chèn hình chụp Terminal ghi nhận "HỆ THỐNG ĐÃ BAY LÊN CLOUD REAL-TIME! XONG!")*
*(📸 Chèn ảnh truy vấn Beeline ra 5 dòng dữ liệu mới nhất)*

## CHƯƠNG 6: TRỰC QUAN HOÁ DỮ LIỆU THỰC THỜI (APACHE SUPERSET)
6.1. Cấu hình kết nối Superset với Hive Thrift Server mang giao thức `hive2://`
6.2. Mapping Hive Views với Physical Datasets trong Superset
6.3. Tiêm tự động cấu hình Superset bằng `fix_superset.py` (Script tự động hóa)
6.4. Xây dựng Real-time Analytics Dashboard:
   - Biểu đồ nến xu hướng giá (Trend)
   - Bảng tỷ lệ giao dịch tự động làm mới
   - Khối lượng giao dịch luân chuyển từng phút
   - Heatmap phân bổ khối lượng giao dịch
   *(📸 Chèn ảnh GIF hoặc Screenshot độ nét cao của Dashboard Superset đang hiển thị)*

## CHƯƠNG 7: CÁC VẤN ĐỀ ĐÃ XỬ LÝ (LESSONS LEARNED & TROUBLESHOOTING)
*Chương này để ghi điểm sáng tạo và xử lý sự cố thực tế với giảng viên/hội đồng*
7.1. Lỗi xung đột Metadata Hive (`[LOCATION_ALREADY_EXISTS]`) ngốn tài nguyên và cách giải quyết bằng cơ chế Idempotent Cleanup của Dagster.
7.2. Giải quyết bài toán Race Condition ("Gà và Trứng") giữa Spark Streaming khởi tạo bảng và Hive SQL khởi tạo View.
7.3. Xử lý lỗi Port ảo trên Docker (Netstat đui mù trong quá trình health-check cổng 10000) và giải pháp dùng Bash Beeline Check.
7.4. Khắc phục vấn đề PowerShell nuốt Exit Code của Bash (Warnings bị xem thành Errors).

## CHƯƠNG 8: KẾT LUẬN & HƯỚNG PHÁT TRIỂN
8.1. Đánh giá kết quả đạt được.
8.2. Ưu và nhược điểm của dự án.
8.3. Hướng tiếp cận tương lai (Thêm Spark Structured, Deploy Kubernetes, Di chuyển lên nền tảng Cloud AWS/GCP, v.v.)

---
*Ghi chú cho bạn: Tất cả cấu trúc bên trên đều là xương sống dựa trên các file mà chúng ta đã làm việc cùng nhau (dagster_pipeline.py, create_star_schema.sql, drop.py, producer.py). Khi viết báo cáo, bạn chỉ cần gọt dũa lại và mở từng script ra copy code snippet thả vào là thành bài siêu hoàn chỉnh!*
