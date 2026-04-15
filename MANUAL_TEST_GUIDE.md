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
All container: docker ps -a
docker exec -it master bash
Kiêm tra file panquet lưu trữ: hdfs dfs -ls /user/hive/warehouse (nằm ở host 9000)
-> spark-sql -> SELECT * FROM crypto_trades LIMIT 10;
check xem hive metastore có đang chạy chưa: 
-> netstat -tulnp | grep 9083 (9083 dùng để spark, hive -> hive metastore (biết được metadata, dữ liệu dạng gì, lưu ở đâu))
-> jps

file .sql: 
-> dùng để đăng kí bảng vào Hive metastore để spark/superset query được

KAFKA_CLUSTER_ID=NjY0MDlkYzgtYWM1MS00ZT
--> số CMNN or id dịnh danh only of kafka trong docker 
-> là hệ thống phân tán: 
   + dữ lieu và cong việc dược chia cho nhiều máy (chạy trên nhiều server gọi là broker(tương ứng với một máy)
   + broker: chịu trách nhiệm lưu trữ dữ lieu (các partition của topic) và sử lí yêu cầu từ producer (gửi dữ lieu) và consumer (doc dữ lieu) 	
   + replication > 1 --> partition (bản sao > 1)  --> broker .
--> quản lí metadata: dảm bao các dữ lieu (topic, partitions) lưu trong các folder của docker
   + tính on dịnh: giúp hẹ thong khoi dọng nhanh và it loi hon , neu kh co thi moi lần chạy kafka tự sinh ra một id mới ngẫu nhiên, doi khi gây xung dọt khi dữ lieu cụ còn sót lại trong volume
   + kết nối công cụ giám sát: sau này nếu muốn mở rộng công cụ UI trực quan dữ lieu vào thì dung nó de kết noi
   + quản lí metadata: DỮ LIỆU ƯỢC LƯU TRÊN VOLUME CỦA DOCKER + HDFS CHO SPARK XỬ LÍ XONG VĂNG VÔ CHO SUPERSET VẼ HÌNH
     - DE XEM DỮ LIEU BEN TRONG SỦ DỤNG LỆNH: 
       + MỞ TRÌNH DUYET WEB: http://localhost:9870 -> Utilities -> Browse the file system. GÕ: /user/hive/warehouse/crypto_trades 
       + XEM BANG TERMINAL: docker exec -it master bash , truy vấn trong mục hdfs
         -> kết quả: year=2026 (kỹ thuật partitioning): file .parquet (dữ liệu thực tế) + _spark_metadata (metadata: cho biết mình đã cào đến đâu ròi, dữ liệu bị lỗi hay không) 
        9092: số nội bộ (internal): cho các container bên trong docker nói chuyện với nhau (spark -> kafka trong docker, producer -> kafka trong docker, consumer -> kafka trong docker)
        9094: số bên ngoài (external): cho phép các ứng dụng bên ngoài docker nói chuyện với kafka (producer -> kafka bên ngoài, consumer -> kafka bên ngoài, superset -> kafka bên ngoài)

1. 🌐 Danh sách Link Web UI (Mở bằng Chrome/Edge)
Dịch vụ	Link (URL)	Công dụng
Apache Superset	http://localhost:8089	Nơi vẽ Chart và Dashboard (admin/admin)
HDFS Explorer	http://localhost:9870	Xem file .parquet lưu trong "ổ cứng" Hadoop
Spark Master	http://localhost:8080	Xem cụm Spark có đang "khỏe" không
YARN Manager	http://localhost:8088	Quản lý tài nguyên của toàn cụm Hadoop
Spark App UI	http://localhost:4040	Xem chi tiết tiến trình Spark đang cào data (chỉ hiện khi đang chạy) 

- lịch sử
+ xem lịch sử cào dữ liệu luôn
+ xem trong 9870

2. 💻 Lệnh Terminal để kiểm tra "Sức khỏe" hệ thống
Bạn mở Terminal/PowerShell trên Windows và gõ các lệnh sau để check xem "thủ môn" có đang gác đền không:

A. Kiểm tra Container có đang chạy không? (Lệnh cơ bản nhất)
powershell
docker ps

KHI CHẠY LỆNH: docker-compose up -d 
- Window sẽ lôi tất cả các cổng ra mà chạy, ngoại trừ cổng 10000 (cổng kết nối với superset), để nó mở thì nó phải đảm bảo hdfs (9000) và metadata (9083) phải sẵn sàng 100% ròi, nếu mở quá nhanh nó sẽ kh tìm thấy mấy cổng kia thì nó crash luôn

Kết quả mong đợi: Thấy đủ danh sách master, slave1, slave2, kafka, superset và ở cột STATUS ghi là Up ... seconds/minutes.
B. Kiểm tra Cổng (Port) có đang MỞ hay không? (Dùng PowerShell)
Nếu bạn nghi ngờ một dịch vụ bị treo, gõ lệnh này để test kết nối:

powershell
# Kiểm tra Kafka (đường ngoài)
Test-NetConnection localhost -Port 9094
# Kiểm tra Hive/Thrift (để Superset kết nối)
Test-NetConnection localhost -Port 10000
# Kiểm tra Superset
Test-NetConnection localhost -Port 8089
# Kiểm tra HDFS
Test-NetConnection localhost -Port 9870
# Kiểm tra Spark
Test-NetConnection localhost -Port 8080
# Kiểm tra cổng 10000 (cổng bảo vệ canh cửa chứ không phải cổng kết nối đến superset).
Test-NetConnection localhost -Port 10000

Kết quả mong đợi: Ở dòng cuối cùng hiện chữ TcpTestSucceeded : True. Nếu nó hiện False là dịch vụ đó đang "ngất", cần restart docker.
C. Kiểm tra Log (Xem bên trong đang nói gì)
Nếu dashboard không nhảy số, hãy xem Kafka/Spark đang la hét gì bằng lệnh:

# Lệnh chạy Dagster
python -m dagster dev -f src/dagster_pipeline.py
taskkill /F /IM python.exe