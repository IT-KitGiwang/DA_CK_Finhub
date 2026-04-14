# 🚀 Hướng Dẫn Test Thủ Công Toàn Bộ Luồng Dự Án Từ A -> Z

Tài liệu này hướng dẫn cách kiểm thử toàn bộ luồng pipeline của dự án (từ thu thập dữ liệu Finnhub -> Kafka -> Spark Streaming -> Hadoop HDFS / Hive -> Apache Superset) từng bước một để bạn dễ dàng kiểm soát cũng như theo dõi tiến trình. Cấu trúc của dự án đã được sắp xếp lại để gọn gàng nhưng vẫn đảm bảo tính toán cục bộ và logic 100% không đổi.

## 🛠️ Trạng thái Cấu Trúc Dự Án (Mới)
```text
📦 Real-Estate-Finhub
 ┣ 📂 config/           # Cấu hình Hadoop, Spark, Hive
 ┣ 📂 data/             # Thư mục chứa dữ liệu tĩnh hoặc file kết xuất tạm thời (VD: crypto_multi.csv)
 ┣ 📂 logs/             # Thư mục lưu file log từ hệ thống (VD: log của Spark)
 ┣ 📂 scripts/          # Nơi chứa Shell script/PowerShell hỗ trợ test & setup
 ┃ ┣ 📜 entrypoint.sh
 ┃ ┣ 📜 fix_hive.sh
 ┃ ┣ 📜 restart_hive.sh
 ┃ ┗ 📜 run_e2e_test.ps1
 ┣ 📂 src/              # ⭐ Core dự án (toàn bộ mã nguồn Python)
 ┃ ┣ 📂 db/               # Tương tác Hive và Superset (drop, fix, test_conn)
 ┃ ┣ 📂 kafka/            # Producer & Consumer thực
 ┃ ┗ 📂 spark/            # Mã nguồn xử lý Stream Spark
 ┣ 📜 docker-compose.yml
 ┣ 📜 Dockerfile
 ┣ 📜 Dockerfile.superset
 ┗ 📜 .env
```

---

## 🏃 Quy Trình Thực Thi:

### 👉 Bước 1: Build & Khởi động Docker Compose
Tại thư mục gốc của dự án, mở Terminal/PowerShell và chạy:
```powershell
docker-compose down

# Build lại image (quan trọng: cập nhật lại config path và scripts sau khi cấu trúc lại)
docker build -t hadoop-spark-jdk21:latest .

# Đẩy container lên background
docker-compose up -d
```

### 👉 Bước 2: Đảm bảo Container đã chạy xong & Sẵn sàng Port 10000
Spark Thrift Server và Hive Metastore cần khoảng 3 - 5 phút để khởi động hoàn toàn.
Hãy kiểm tra tính khả dụng của Service Thrift trên master:
```powershell
docker exec master bash -c "nc -z localhost 10000 && echo OPEN || echo CLOSED"
```
Khi kết quả trả về `OPEN`, bạn có thể duyệt sang Bước 3.

### 👉 Bước 3: Khởi động Kafka Producer (Thu thập data Real-Time từ Finnhub)
Mở một cửa sổ Terminal/PowerShell mới (ở thư mục gốc), chạy:
```powershell
python src/kafka/producer.py
```
*Bạn sẽ bắt đầu thấy log "KAFKA SENT ..." phản hồi các gói dữ liệu giao dịch mã hóa.* (Hãy để Terminal này chạy liên tục)

### 👉 Bước 4: Khởi động Spark Streaming (Tiêu thụ Kafka -> Xử lý -> Đẩy vào Hive/HDFS)
Mở một cửa sổ Terminal/PowerShell thứ ba, chạy:
```powershell
docker exec -it master hdfs dfs -rm -r /user/hive/warehouse/crypto_trades

docker exec -it master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 /home/dack15/src/spark/spark_processor.py
```
*Lưu ý script sẽ connect tới Kafka, lấy Micro-batch sau đó lưu định dạng Parquet trên HDFS.* (Hãy để Terminal này mở để theo dõi quá trình streaming)

### 👉 Bước 5: Truy vấn Dữ liệu Thủ công (Kiểm tra xem dữ liệu đã vào Hive chưa)
Hãy đợi khoảng 30 giây đến 1 phút để ít nhất một vài Batch đã được nộp thành công vào Hive. Sau đó mở Terminal thứ tư và chạy:
```powershell

docker exec master bash -c "beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n dack15 -e 'SELECT * FROM crypto_trades LIMIT 5;'"
```
*Kết quả sẽ trả về một bảng CLI hiển thị các cột dữ liệu như: event_time, symbol, price, volume.*

### 👉 Bước 6: Phân tích & Trực Quan Hoá với Apache Superset
1. Truy cập vào trình duyệt bằng địa chỉ: `http://localhost:8089`
2. **Tài khoản mặc định:** `admin` | **Mật khẩu:** `admin`
3. Trong giao diện Superset:
   - Truy cập **Settings -> Database Connections -> + Database**
   - Lọc tìm SQL Engine: `Apache Hive`
   - Cung cấp **SQLAlchemy URI**: `hive://dack15@master:10000/default?auth=NOSASL`
   - Bấm **Test Connection** để xác thực xem có thông báo Connected thành công.
   - Bấm **Connect** kết nối thành công Database.
4. Có thể vào SQL Lab để check `SELECT * FROM crypto_trades` hoặc trực tiếp tạo Charts.

---

## ⚡ Chạy Tự Động Toàn Bộ Bằng Script E2E
Nếu không muốn chạy thủ công từng lệnh ở trên, bạn hoàn toàn có thể chạy file kiểm tra E2E đã cung cấp:
Tại màn hình Terminal gốc:
```powershell
.\scripts\run_e2e_test.ps1
```
Script này sẽ tự động build môi trường, pop-up từng cửa sổ để chạy và verify các flow tự động hoá.

