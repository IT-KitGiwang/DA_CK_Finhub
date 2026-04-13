import subprocess
import time
import os
import sys

def run_command(command, shell=True, capture=False):
    try:
        if capture:
            result = subprocess.run(command, shell=shell, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return result.stdout.strip()
        else:
            subprocess.run(command, shell=shell, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Lỗi khi thực thi: {command}")
        if capture:
            print(f"Chi tiết: {e.stderr}")
        return None

def check_port_10000():
    cmd = 'docker exec master bash -c "nc -z localhost 10000 && echo OPEN || echo CLOSED"'
    output = run_command(cmd, capture=True)
    return "OPEN" in str(output)

def main():
    print("="*60)
    print("🚀 STARTING E2E PIPELINE TEST (PYTHON VERSION)")
    print("="*60)

    # 2. Chờ Port 10000

    # 3. Chạy Producer (Local)
    print("\n[3/6] 📡 Bật Producer thu thập dữ liệu...")
    # Chạy trong cửa sổ mới (Windows)
    subprocess.Popen(["start", "cmd", "/k", "python producer.py"], shell=True)
    print("✅ Đã bật producer.py trong cửa sổ mới.")

    # 4. Chạy Spark Processor (Inside Docker)
    print("\n[4/6] ⚡ Bật Spark Streaming Processor...")
    spark_cmd = 'docker exec -it master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 /home/dack15/spark_processor.py'
    # Chạy trong cửa sổ mới (Windows)
    subprocess.Popen(["start", "cmd", "/k", spark_cmd], shell=True)
    print("✅ Đã bật spark_processor.py trong cửa sổ mới.")

    # 5. Chờ data ghi vào
    print("\n[5/6] ⏳ Chờ 40 giây cho Spark xử lý đợt dữ liệu đầu tiên...")
    time.sleep(40)

    # 6. Query kiểm tra
    print("\n[6/6] 🔍 Kiểm tra dữ liệu trong Hive bằng Beeline...")
    query_cmd = 'docker exec master bash -c "beeline -u jdbc:hive2://localhost:10000 -n dack15 -e \'SELECT * FROM crypto_trades LIMIT 10;\'"'
    run_command(query_cmd)

    print("\n" + "="*60)
    print("🎉 KẾT THÚC TEST LUỒNG!")
    print("="*60)
    print("1. Hãy kiểm tra các cửa sổ Command Prompt mới mở.")
    print("2. Truy cập Superset: http://localhost:8089")
    print("3. Kết nối Hive: hive://dack15@master:10000/default?auth=NOSASL")

if __name__ == "__main__":
    main()
