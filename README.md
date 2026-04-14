# 🏢 Real Estate Finhub - Mạng Lưới Xử Lý Dữ Liệu Thời Gian Thực (Big Data & Streaming Pipeline)

Dự án này là một hệ thống phân tán toàn diện (End-to-End Pipeline) được thiết kế theo trường phái **Clean Architecture** để mô phỏng một môi trường Big Data chuyên nghiệp. Hệ thống đảm nhiệm việc thu thập dữ liệu giao dịch tài chính (tiền điện tử/chứng khoán) theo thời gian thực (Real-time Streaming), sau đó xử lý song song, lưu trữ trên nền tảng phân tán và cuối cùng trực quan hoá lên trang quản trị (Dashboard).

<div align="center">
  <img src="https://img.shields.io/badge/Hadoop-3.5.0-yellow?style=for-the-badge&logo=apachehadoop"/>
  <img src="https://img.shields.io/badge/Apache_Spark-4.1.0-orange?style=for-the-badge&logo=apachespark"/>
  <img src="https://img.shields.io/badge/Apache_Kafka-3.8.x-black?style=for-the-badge&logo=apachekafka"/>
  <img src="https://img.shields.io/badge/Hive-4.2.0-yellow?style=for-the-badge"/>
  <img src="https://img.shields.io/badge/Superset-4.0-blue?style=for-the-badge&logo=apachesuperset"/>
  <img src="https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python"/>
  <img src="https://img.shields.io/badge/Docker-Enabled-2496ED?style=for-the-badge&logo=docker"/>
</div>

---

## 🏗️ Kiến Trúc Hệ Thống (Architecture Flow)

Luồng chảy của dòng dữ liệu trong hệ thống đi tuần tự qua 5 trạm cốt lõi:

1. **Finnhub Data Ingestion (Producer)**: Dùng Socket (WebSocket API) kết nối liên tục để cào thông tin giao dịch của các cặp coin/cổ phiếu từ sàn.
2. **Apache Kafka (Message Broker)**: Đóng vai trò là ống dẫn tín hiệu tốc độ cao và điểm đệm (Buffer). Dữ liệu mã hoá từ tầng Ingestion đẩy tuỳ ý vào các Topic (e.g., `crypto_trades`).
3. **Apache Spark (Streaming Engine)**: Một vi mạch phân tán lắng nghe (subscribe) liên tục trên Kafka Topic, phân tách JSON, gán lại Schema (cấu trúc dữ liệu) thành Dataset cứng cáp.
4. **Hadoop HDFS / Apache Hive (Data Warehouse)**: Spark chuyển đổi luồng dữ liệu sang Micro-batchs chuẩn hoá, lưu dưới dạng Parquet xuống HDFS. Metadata của bảng được quản trị bởi Thrift Server qua chuẩn Hive Metastore 4.x.
5. **Apache Superset (BI & Analytics)**: Khởi chạy trên cổng `8089`, tương tác với Hive qua giao thức JDBC (`NOSASL`). Truy vấn theo thời gian thực để vẽ biểu đồ và phân tích Insight.

---

## 📁 Cấu Trúc Mã Nguồn (Clean Architecture)

Dự án đã được chia nhỏ, tái cơ cấu thành các tầng để duy trì Single Responsibility (Trách nhiệm đơn):

```text
📦 Real-Estate-Finhub
 ┣ 📂 config/           # Các file lõi .xml để thiết lập mạng lưới cho Hadoop, YARN và Hive.
 ┣ 📂 data/             # Kho tàng chứa các file Output/Input offline tĩnh (như .csv).
 ┣ 📂 logs/             # Giữ lại các Tracking Logs của toàn bộ ứng dụng bị ngắt mạch từ Docker.
 ┣ 📂 scripts/          # Tầng DevOps & Automation
 ┃ ┣ 📜 entrypoint.sh      # Trí tuệ của Container (Nhận biết ai làm Master/Slave để cài đặt vai).
 ┃ ┣ 📜 run_e2e_test.ps1   # PowerShell Script Đỉnh cao giúp tự động hoá 1 Click chạy từ đầu đến cuối!
 ┃ ┗ ...
 ┣ 📂 src/              # ⭐ Tầng Logic Core (Mã nguồn phần mềm cực kỳ sạch sẽ)
 ┃ ┣ 📂 db/               # Utility API cho Superset, drop bảng và test Hive Connection.
 ┃ ┣ 📂 kafka/            # OOP Logic cho Producer (đẩy) & Consumer (Nhận) dữ liệu thời gian thực.
 ┃ ┗ 📂 spark/            # Mã nguồn lắng nghe stream và write xuống Hive Warehouse.
 ┣ 📜 docker-compose.yml# Bản thiết kế tổng mạng của Toàn bộ Docker.
 ┣ 📜 Dockerfile        # Bản thiết kế nguyên liệu cài JDK21, Hadoop, Spark cho OS Base.
 ┣ 📜 MANUAL_TEST_GUIDE.md # 📖 CẨM NANG HƯỚNG DẪN TEST TAY 100% (Phải Đọc)
 ┗ 📜 .env              # Khoá bí mật (Ví dụ API Key FINNHUB), Cổng Kafka...
```

---

## 🚀 Hướng Dẫn Sử Dụng & Khởi Chạy

### 1️⃣ Thiết Lập Môi Trường (Prerequisites)
- Đảm bảo bạn đã cài **Docker** và **Docker Compose**.
- Đã cài **Python 3.10+**.
- Cấu hình file biến môi trường: Đổi tên file `.env.example` thành `.env` và cung cấp chuỗi `FINNHUB_API_KEY` được cấp phép của bạn. Mật khẩu khởi chạy mặc định của Superset cũng nằm ở đây.

### 2️⃣ Khởi Chạy Tự Động (Automation Test)
Thay vì gõ từng lệnh, cung cấp sẵn cho bạn file chạy E2E (End to End). Chạy lệnh sau trong thư mục Root trên PowerShell:
```powershell
.\scripts\run_e2e_test.ps1
```
*(Chi tiết logic hệ thống tự làm: Clean Docker -> Build Docker Cluster -> Chờ Master Port 10000 -> Bật Python Kafka -> Bật Apache Spark)*.

### 3️⃣ Chạy Test Thủ Công (Manual Override)
Nếu bạn muốn tìm hiểu cơ chế hoạt động từng công nghệ nhỏ lẻ bên trong, dự án cung cấp bộ tài liệu cực kì chi tiết nằm tại:
👉 **[Xem `MANUAL_TEST_GUIDE.md`](./MANUAL_TEST_GUIDE.md)**

### 4️⃣ Truy Cập Web UIs:
Khi hệ thống chạy xong, bạn có thể giám sát tính trạng phần cứng thông qua các UI tích hợp cực kỳ hiện đại được Expose ra máy host:
- **Hadoop NameNode** (Check phân vùng HDFS): `http://localhost:9870`
- **Hadoop YARN** (Check tác vụ & nodeManager): `http://localhost:8088`
- **Spark Master UI** (Kiểm tra Workers): `http://localhost:8080`
- **Apache Superset** (Kho lưu trữ Chart Analytics): `http://localhost:8089` *(Tài khoản mặc định: `admin/admin`)*.

---

## 🔧 Xử Lý Sự Cố Thường Gặp (Troubleshooting)

| Vấn đề | Khắc Phục |
| :--- | :--- |
| Port 10000 báo `CLOSED` quá lấu | Xin hãy chờ bình tĩnh (thường 3-5 phút do Hive Metastore rất nặng với Derby). |
| Docker báo hết RAM | Cluster tiêu thụ tối thiểu từ 6 - 8GB Ram để nhồi các DataNodes. Hãy cấp cấu hình đủ trong Settings của Docker Desktop. |
| Beeline báo lỗi Protocol | Hãy đảm bảo chuỗi truy vấn luôn có tham số `;auth=noSasl` đằng sau chuỗi `localhost:10000`. |
| Superset lỗi Database Connect | Bạn hãy chạy file `python src/db/fix_superset.py`, file này sẽ tự động đạn tiêm lại cấu hình kết nối chuẩn vào Dashboard cho bạn. |

---
