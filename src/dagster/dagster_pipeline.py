import subprocess
import time
from dagster import op, job, get_dagster_logger, success_hook, failure_hook, HookContext
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = get_dagster_logger()

# ----------------------------------------------------
# KHUNG TRỤC GỬI EMAIL THÔNG BÁO (ALERTS)
# ----------------------------------------------------
def send_gmail_alert(subject: str, body: str):
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    receiver_email = os.getenv("ALERT_RECEIVER", sender_email)
    
    if not sender_email or not sender_password:
        logger.warning("Chưa cấu hình tài khoản Email trong file .env, nên không thể gửi mail (Nhưng Ops vẫn chạy).")
        return

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        logger.info(f"📧 Đã bắn Email thành công: {subject}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi bắn Email: {e}")

@success_hook
def email_on_success(context: HookContext):
    op_name = context.op.name
    subject = f"✅ [DAGSTER THÀNH CÔNG] Nhiệm vụ '{op_name}' đã hoàn tất!"
    body = f"<h3>Xin chào Data Engineer,</h3><p>Khối công việc <b>{op_name}</b> vừa chạy trót lọt và an toàn 100%.</p>"
    send_gmail_alert(subject, body)

@failure_hook
def email_on_failure(context: HookContext):
    op_name = context.op.name
    error_msg = str(context.op_exception)
    subject = f"🚨 [DAGSTER BÁO LỖI KHẨN] Khối '{op_name}' SỤP ĐỔ!"
    body = f"<h3>🚨 Báo Động Đỏ Hệ Thống Data!</h3><p>Hệ thống vừa sụp đổ tại chốt chặn: <b>{op_name}</b></p><br><b>💡 Nguyên nhân (StackTrace):</b><br><pre>{error_msg}</pre><br><p>Yêu cầu Engineer vào kiểm tra ngay lập tức!</p>"
    send_gmail_alert(subject, body)
# ----------------------------------------------------

def run_cmd(command: str):
    """Hàm chạy shell command."""
    logger.info(f"Đang thực thi: {command}")
    subprocess.run(command, shell=True, check=True)

@op
def boot_infrastructure():
    """Nhiệm vụ 1: Lễ tân khởi động hệ thống Docker"""
    logger.info("Khởi động cụm Docker Compose...")
    run_cmd("docker-compose up -d")
    logger.info("Đợi 20 giây để hệ thống rễ (HDFS, Metastore) cứng cáp...")
    time.sleep(20)

@op
def cleanup_old_garbage(boot_infrastructure):
    """Nhiệm vụ 2: Cô Tấm dọn rác - Dọn dẹp HDFS phiên trước"""
    logger.info("Dọn dẹp Checkpoint và Data rác từ phiên hôm trước...")
    run_cmd('docker exec master bash -c "rm -rf /tmp/spark_checkpoint_crypto_final && hdfs dfs -rm -r /user/hive/warehouse/crypto_trades 2>/dev/null; echo CLEAN"')

@op
def activate_thrift_server(cleanup_old_garbage):
    """Nhiệm vụ 3: Đánh thức Thrift Server để kết nối BI"""
    run_cmd('docker exec master bash -c "/opt/spark/sbin/stop-thriftserver.sh || true"')
    thrift_cmd = (
        "/opt/spark/sbin/start-thriftserver.sh --master local[2] --name 'Spark-Thrift-Server' "
        "--conf spark.sql.hive.metastore.version=4.0.0 --conf spark.sql.hive.metastore.jars=path "
        "--conf spark.sql.hive.metastore.jars.path=file:///opt/hive/lib/* "
        "--hiveconf hive.metastore.uris=thrift://master:9083 "
        "--hiveconf hive.metastore.warehouse.dir=hdfs://master:9000/user/hive/warehouse "
        "--hiveconf hive.server2.thrift.port=10000 --hiveconf hive.server2.thrift.bind.host=0.0.0.0 "
        "--hiveconf hive.server2.transport.mode=binary --hiveconf hive.server2.authentication=NOSASL"
    )
    run_cmd(f'docker exec master bash -c "{thrift_cmd}"')
    logger.info("Chờ 30s để Thrift Server mở cổng 10000 (Đôi khi rùa bò một chút)...")
    time.sleep(30)

@op
def load_dimension_tables(activate_thrift_server):
    """Nhiệm vụ 4: Nạp Bảng Dữ Liệu Tĩnh (Dim) chuẩn DataOps"""
    logger.info("Bắn câu lệnh Beeline SQL để tạo bảng HIVE (Dim/Fact View)...")
    beeline_cmd = "beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n dack15 -f /home/dack15/src/db/create_star_schema.sql"
    run_cmd(f'docker exec master bash -c "{beeline_cmd}"')

@op
def config_superset(activate_thrift_server):
    """Tiêm cấu hình vào Superset"""
    logger.info("Cấu hình Database Connection cho Superset...")
    run_cmd('python src/db/fix_superset.py')

@op
def start_realtime_streams(load_dimension_tables, config_superset):
    """Nhiệm vụ 5: Kích hoạt Công nhân (Kafka + Spark) chạy ngầm"""
    logger.info("Khởi động Máy Bơm Kafka (Background)...")
    subprocess.Popen("python src/kafka/producer.py", shell=True)
    
    logger.info("Đợi 5 giây để Websocket làm nóng...")
    time.sleep(5)
    
    logger.info("Cắm điện Động Cơ Spark Streaming (Background)...")
    spark_submit_cmd = (
        "docker exec master spark-submit "
        "--jars /home/dack15/src/jars/spark-sql-kafka.jar,"
        "/home/dack15/src/jars/spark-token-provider-kafka-0-10_2.13-4.1.0.jar,"
        "/home/dack15/src/jars/kafka-clients-3.9.1.jar,"
        "/home/dack15/src/jars/commons-pool2-2.12.1.jar "
        "/home/dack15/src/spark/spark_processor.py"
    )
    subprocess.Popen(spark_submit_cmd, shell=True)
    logger.info("HỆ THỐNG ĐÃ BAY LÊN CLOUD REAL-TIME! XONG! 🚀")

@job(hooks={email_on_success, email_on_failure})
def finhub_realtime_setup_pipeline():
    """Đường ống Khởi tạo Data Warehouse chuẩn Lambda Architecture"""
    # Khai báo sự phụ thuộc (Dependency Graph)
    step1 = boot_infrastructure()
    step2 = cleanup_old_garbage(step1)
    step3 = activate_thrift_server(step2)
    
    # Hai bước này chạy song song sau khi Thrift Server mở
    step4a = load_dimension_tables(step3)
    step4b = config_superset(step3)
    
    # Kích nổ luồng Streaming Real-time ngầm
    start_realtime_streams(step4a, step4b)
