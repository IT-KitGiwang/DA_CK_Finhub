# Real Estate Finhub - Big Data Infrastructure

## 1. Tổng quan Kiến Trúc (Architecture Review)
Dự án yêu cầu xây dựng một cụm (cluster) Big Data sử dụng Docker để phục vụ việc xử lý, lưu trữ và Streaming dữ liệu, bao gồm các thành phần chính:

### Thành phần & Phiên bản (Versions)
- **Hệ điều hành cơ sở**: Ubuntu 22.04 / Debian Hướng tới tương thích cao.
- **Java**: **JDK 21** (Phiên bản LTS mới nhất, hỗ trợ tốt các package hiện đại của Hadoop/Spark).
- **Hadoop**: **3.5.0** (Phiên bản mới nhất có độ ổn định cao với JDK 21). Môi trường phân tán (Distributed File System - HDFS và YARN).
- **Spark**: **4.1.0** (Tối ưu để chạy trên nền Hadoop 3, kết hợp với Kafka).
- **Kafka**: **3.7.x / 3.8.x** (Sử dụng KRaft mode, không cần Zookeeper để giảm footprint và tăng hiệu năng).

### Cấu trúc Cluster (Nodes)
1. **Master Node** (`dack15@master`):
   - Đóng vai trò là Hadoop NameNode và YARN ResourceManager.
   - Thường được dùng làm Spark Master.
2. **Worker/Slave Nodes** (`dack15@slave1`, `dack15@slave2`):
   - Đóng vai trò là Hadoop DataNode và YARN NodeManager.
   - Các Spark Worker được cài đặt tại đây để xử lý tính toán phân tán.
3. **Message Broker Node** (`kafka`):
   - Chạy Apache Kafka để phục vụ Data Ingestion realtime.

---

## 2. Quy trình Thực thi (Step-by-Step Execution Flow)
Theo yêu cầu dự án, chúng ta sẽ thực hiện theo các bước sau:

- **Bước 1**: Tạo `Dockerfile` tùy chỉnh cài đặt OpenJDK 21, cấu hình user/group `dack15` và thiết lập các biến môi trường cho Hadoop/Spark.
- **Bước 2**: Viết shell script (entrypoint) định tuyến các tiến trình khởi động (NameNode, DataNode, Spark Worker, v.v.) dựa trên ROLE truyền vào container.
- **Bước 3**: Viết `docker-compose.yml` định nghĩa mạng nội bộ của cluster (`hadoop-net`) và liên kết các nodes `master`, `slave1`, `slave2`, `kafka`.
- **Bước 4**: Mở các thư mục volume dùng cho HDFS (kết xuất log & data ra storage ngoài để không bị mất khi down core).
- **Bước 5**: Build Images & Start Up: `docker compose up -d`
- **Bước 6**: Verify thông qua WEB UI (Hadoop 9870, YARN 8088, Spark 8080).

*Lưu ý: Bạn vui lòng xem kỹ README này trước khi đồng ý cho tôi tiến hành sinh mã Code/Logic (Dockerfile, docker-compose.yml) ở các bước tiếp theo để đảm bảo đi đúng hướng kiến trúc.*

# Cài đặt Superset

# 1. Tạo tài khoản đăng nhập (User: admin | Pass: admin)
docker exec -it superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@localhost --password admin

# 2. Khởi tạo Database cho Superset ráp bảng (Dòng này sẽ fix triệt để lỗi 500)
docker exec -it superset superset db upgrade

# 3. Mở khóa, khởi tạo Roles
docker exec -it superset superset init

# 4. (Quan trọng) Cài Driver PyHive để tương lai nối được vào Hive
docker exec -it -u root superset pip install pyhive thrift
