# 🚀 Hướng Dẫn Test Thủ Công Toàn Bộ Luồng Dự Án Từ A -> Z

Tài liệu này hướng dẫn cách kiểm thử toàn bộ luồng pipeline của dự án một cách chi tiết, ổn định nhất (từ thu thập dữ liệu Finnhub -> Kafka -> Spark Streaming -> Hadoop HDFS / Hive -> Apache Superset).
Các bước dưới đây đã được tối ưu hóa đặc biệt về tài nguyên (RAM) giúp cho hệ thống Docker Desktop chạy ổn định 100%, không bị crash giữa chừng và xử lí mượt mà trên môi trường Windows.

---

## 🏃 Quy Trình Thực Thi:

### 👉 Bước 0: Khởi động lại Docker Desktop
Để dọn dẹp toàn bộ bộ nhớ RAM dư thừa do các lần chạy trước, bạn hãy **Click chuột phải vào icon ứng dụng Docker Desktop (hình chú cá voi)** trên thanh công cụ Windows gốc bên dưới (Taskbar) -> chọn **Restart**. Đợi cho đến khi icon xanh lại (Docker is running).

### 👉 Bước 1: Khởi động Docker Compose
Tại thư mục gốc của dự án, mở Terminal (PowerShell) và chạy:
```powershell
docker-compose up -d
```
*(Đợi khoảng 60-90 giây cho tất cả các container khởi động hoàn toàn. Nếu muốn chắc ăn, gõ `docker ps` để kiểm tra có đủ master, slave1, slave2, kafka, superset đang chạy không).*

### 👉 Bước 2: Dọn Lịch Sử Cũ & Bật Máy Chủ Thrift Server Chuẩn
Mở Terminal, chạy lần lượt 3 lệnh sau để dọn dữ liệu rác, tắt máy chủ port 10000 cũ của image và bật máy chủ đã căn chỉnh thư viện Hive 4.x:

**1. Dọn dẹp HDFS Checkpoint cũ:**
```powershell
docker exec master bash -c "rm -rf /tmp/spark_checkpoint_crypto_final && hdfs dfs -rm -r /user/hive/warehouse/crypto_trades 2>/dev/null; echo CLEAN"
```

**2. Tắt máy chủ Thrift Server mặc định (nếu nó đang chạy sai cài đặt):**
```powershell
docker exec master bash -c "/opt/spark/sbin/stop-thriftserver.sh"
```

**3. Khởi chạy Thrift Server với cấu hình tương thích Hive 4.0.0 (Copy toàn bộ khối này dán vào và Enter):**
```powershell
docker exec master bash -c "/opt/spark/sbin/start-thriftserver.sh --master local[2] --name 'Spark-Thrift-Server' --conf spark.sql.hive.metastore.version=4.0.0 --conf spark.sql.hive.metastore.jars=path --conf spark.sql.hive.metastore.jars.path=file:///opt/hive/lib/* --hiveconf hive.metastore.uris=thrift://master:9083 --hiveconf hive.metastore.warehouse.dir=hdfs://master:9000/user/hive/warehouse --hiveconf hive.server2.thrift.port=10000 --hiveconf hive.server2.thrift.bind.host=0.0.0.0 --hiveconf hive.server2.transport.mode=binary --hiveconf hive.server2.authentication=NOSASL"
```

### 👉 Bước 3: Khởi động Kafka Producer (Thu thập data Real-Time từ API Finnhub)
Mở một cửa sổ **Terminal/PowerShell CỦA RIÊNG NÓ** (ở thư mục gốc), chạy:
```powershell
python src/kafka/producer.py
```
*Bạn sẽ bắt đầu thấy log "KAFKA SENT ..." xuất hiện.*
**(⚠️ Hãy để nguyên Terminal này mở, KHÔNG ĐƯỢC TẮT)**

### 👉 Bước 4: Khởi động Spark Streaming (Tiêu thụ Kafka -> Làm Sạch -> HDFS)
Mở một cửa sổ **Terminal/PowerShell THỨ 2**, chạy lệnh với các file JAR vật lý do chúng ta đã mount:
```powershell
docker exec -it master spark-submit --jars /home/dack15/src/jars/spark-sql-kafka.jar,/home/dack15/src/jars/spark-token-provider-kafka-0-10_2.13-4.1.0.jar,/home/dack15/src/jars/kafka-clients-3.9.1.jar,/home/dack15/src/jars/commons-pool2-2.12.1.jar /home/dack15/src/spark/spark_processor.py
```
*(Spark giờ được cấu hình giới hạn chạy trên `local[2]` với 1GB RAM, bảo vệ Docker khỏi crash. Đợi cho đến khi hệ thống báo `Writing cleaned stream to Hive table...` là dòng dữ liệu đã bắt đầu đổ xuống).*
**(⚠️ Hãy để nguyên Terminal này mở cùng với Terminal Producer)**

### 👉 Bước 5: Tạo Star Schema & Views bằng BeeLine
Mở tiếp một **Terminal THỨ 3** và thực thi File mã SQL thiết lập cấu trúc cho dự án. File này tạo 2 bảng vật lý Dimension và 3 bảng ảo (Views) gắn thẳng vào Kafka Data Streaming:
```powershell
docker exec master bash -c "beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n dack15 -f /home/dack15/src/db/create_star_schema.sql"
```

### 👉 Bước 6: Truy vấn & Phân tích tại BI Dashboard (Apache Superset)
Toàn bộ luồng Real-Time đã được thiết lập. Hãy tận hưởng thành quả của luồng Lakehouse này:

1. Mở Cốc Cốc / Chrome truy cập vào: `http://localhost:8089`
2. Đăng nhập bằng tài khoản: **`admin`** | Mật khẩu: **`admin`**
3. Bấm vào chữ `SQL Lab` (thanh công cụ trên cao) -> `SQL Editor`.
4. Khung cấu hình bên trái:
   - **Database**: `Hive Crypto` (Nếu không có, chạy lệnh `python src/db/fix_superset.py`)
   - **Schema**: `default`
5. Dán đoạn Query báo cáo tích hợp đầy đủ các luồng sau đây vào khung soạn thảo trắng:

```sql
SELECT 
    f.trade_id, 
    f.trade_time,
    ds.asset_name, 
    de.exchange_name,
    f.price, 
    f.volume, 
    f.trade_value
FROM vw_fact_crypto_trades f 
JOIN dim_symbol ds ON f.symbol_key = ds.symbol_key
JOIN dim_exchange de ON f.exchange_key = de.exchange_key
ORDER BY f.trade_time DESC;
```
6. Bấm nút **RUN**. 
Kết quả sẽ là luồng dữ liệu thời gian thực được xử lý sạch sẽ, giá trị chính xác và các ID được tra cứu chéo theo bảng Dim chuẩn mực theo nghiệp vụ Data Engineering. Sẵn sàng tạo các biểu diễn Chart tự động làm mới!
---
