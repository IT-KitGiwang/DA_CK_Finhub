import subprocess
import time
import sys
import logging
import os

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [AUTO-RUNNER] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def run_command(command: str, wait: bool = True, shell: bool = True, description: str = ""):
    """Hàm chạy các lệnh terminal."""
    logger.info(f"⏳ Đang thực thi: {description}")
    logger.info(f"> {command}")
    
    if wait:
        try:
            # Chạy và chờ kết thúc
            result = subprocess.run(command, shell=shell, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            logger.info(f"✅ Thành công: {description}")
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ LỖI KHI CHẠY: {description}")
            logger.error(f"Chi tiết lỗi: {e.stderr}")
            sys.exit(1)
    else:
        # Chạy ngầm (Daemon mode)
        process = subprocess.Popen(command, shell=shell)
        logger.info(f"🚀 Bắn chạy ngầm thành công: {description}")
        return process

def main():
    logger.info("="*60)
    logger.info("🚀 KHỞI ĐỘNG HỆ THỐNG REAL-ESTATE FINHUB TỰ ĐỘNG (A-Z)".center(60))
    logger.info("="*60)

    # 1. Bật Docker Compose
    run_command("docker-compose up -d", description="Khởi động cụm Docker Compose")
    
    logger.info("⏳ Đợi 20 giây để các container (Hadoop, Kafka, Superset) khởi động mạng...")
    time.sleep(20)

    # 2. Xóa dữ liệu cũ (HDFS Clean)
    run_command('docker exec master bash -c "rm -rf /tmp/spark_checkpoint_crypto_final && hdfs dfs -rm -r /user/hive/warehouse/crypto_trades 2>/dev/null; echo CLEAN"', description="Dọn rác HDFS phiên làm việc cũ")

    # 2.5 Tắt Thrift Server cũ bị lỗi
    try:
        run_command('docker exec master bash -c "/opt/spark/sbin/stop-thriftserver.sh || true"', wait=True, description="Tắt Thrift Server mặc định nếu có")
    except SystemExit:
        logger.info("Thrift Server chưa chạy, bỏ qua bước tắt.")

    # 3. Khởi chạy Spark Thrift Server
    thrift_cmd = "/opt/spark/sbin/start-thriftserver.sh --master local[2] --name 'Spark-Thrift-Server' --conf spark.sql.hive.metastore.version=4.0.0 --conf spark.sql.hive.metastore.jars=path --conf spark.sql.hive.metastore.jars.path=file:///opt/hive/lib/* --hiveconf hive.metastore.uris=thrift://master:9083 --hiveconf hive.metastore.warehouse.dir=hdfs://master:9000/user/hive/warehouse --hiveconf hive.server2.thrift.port=10000 --hiveconf hive.server2.thrift.bind.host=0.0.0.0 --hiveconf hive.server2.transport.mode=binary --hiveconf hive.server2.authentication=NOSASL"
    run_command(f'docker exec master bash -c "{thrift_cmd}"', description="Kích hoạt Spark Thrift Server (Port 10000)")

    logger.info("⏳ Đợi 15 giây để Thrift Server mở cổng 10000...")
    time.sleep(15)

    # 4. Trải thảm Star Schema
    beeline_cmd = "beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n dack15 -f /home/dack15/src/db/create_star_schema.sql"
    run_command(f'docker exec master bash -c "{beeline_cmd}"', description="Triển khai DWH Star Schema & Views bằng Beeline")

    # 5. Fix kết nối Superset
    logger.info("⏳ Tự động sửa lại kết nối DB trên Superset...")
    run_command(f'python src/db/fix_superset.py', description="Tiêm kết nối Hive vào Superset")

    # 6. Bật Kafka Producer (Chạy ngầm)
    producer_process = run_command("python src/kafka/producer.py", wait=False, description="Kích hoạt Máy Bơm Kafka (Subprocess)")
    
    logger.info("⏳ Đợi 10 giây để Producer làm nóng kết nối Websocket...")
    time.sleep(10)

    # 7. Bật Spark Streaming (Chạy ngầm)
    spark_submit_cmd = (
        "docker exec master spark-submit "
        "--jars /home/dack15/src/jars/spark-sql-kafka.jar,"
        "/home/dack15/src/jars/spark-token-provider-kafka-0-10_2.13-4.1.0.jar,"
        "/home/dack15/src/jars/kafka-clients-3.9.1.jar,"
        "/home/dack15/src/jars/commons-pool2-2.12.1.jar "
        "/home/dack15/src/spark/spark_processor.py"
    )
    spark_process = run_command(spark_submit_cmd, wait=False, description="Khởi động Động cơ Spark Streaming lõi")

    logger.info("="*60)
    logger.info("✅ HỆ THỐNG ĐÃ KÍCH HOẠT HOÀN TOÀN!".center(60))
    logger.info("1. Spark Streaming và Finnhub Producer hiện đang chạy ngầm trên máy của bạn.")
    logger.info("2. Hãy mở trình duyệt, truy cập Superset (http://localhost:8089) và xem Dashboard Real-time.")
    logger.info("3. Ấn nút [Ctrl + C] tại Terminal này để TẮT luồng ngầm.")
    logger.info("="*60)

    try:
        # Treo terminal gốc để giữ các subprocess sống
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Đang tắt toàn bộ hệ thống ngầm...")
        producer_process.terminate()
        spark_process.terminate()
        logger.info("Đã dọn dẹp sạch sẽ. Chúc bạn báo cáo đồ án thành công!")

if __name__ == "__main__":
    main()
