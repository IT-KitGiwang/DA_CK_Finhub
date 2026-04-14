[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "🚀 TEST PIPELINE E2E: FINNHUB -> KAFKA -> SPARK -> HIVE -> SUPERSET" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

Write-Host "`n[1/6] 🐳 Build và khởi động lại Docker Compose..." -ForegroundColor Yellow
docker-compose down
docker build -t hadoop-spark-jdk21:latest .
docker-compose up -d

Write-Host "`n[2/6] ⏳ Đang chờ hệ thống Hadoop, Hive và Spark Thrift Server khởi động (khoảng 3-5 phút)..." -ForegroundColor Yellow
$maxAttempts = 60
$port10000Open = $false
for ($i = 1; $i -le $maxAttempts; $i++) {
    $result = docker exec master bash -c "nc -z localhost 10000 && echo OPEN || echo CLOSED" 2>$null
    if ($result -match "OPEN") {
        $port10000Open = $true
        Write-Host "✅ Port 10000 (Spark Thrift Server) đã MỞ!" -ForegroundColor Green
        break
    }
    Write-Host "  ... Đang chờ các dịch vụ khởi động ($i/$maxAttempts)"
    Start-Sleep -Seconds 5
}

if (-not $port10000Open) {
    Write-Host "❌ Port 10000 không mở sau 5 phút. Hãy kiểm tra lại log container master!" -ForegroundColor Red
    exit 1
}

Write-Host "`n[3/6] 📡 Bật Producer (Thu thập data từ Finnhub) trong cửa sổ mới..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python src/kafka/producer.py"
Write-Host "✅ Đã chạy producer.py ở cửa sổ mới (Đang gửi dữ liệu vào Kafka)." -ForegroundColor Green

Write-Host "`n[4/6] ⚡ Bật Spark Streaming Processor trong cửa sổ mới..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "docker exec -it master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 /home/dack15/src/spark/spark_processor.py"
Write-Host "✅ Đã chạy spark_processor.py ở cửa sổ mới (Đang đọc Kafka -> ghi Hive)." -ForegroundColor Green

Write-Host "`n[5/6] ⏳ Chờ 30 giây để Spark gom micro-batch và ghi file Parquet xuống HDFS/Hive..." -ForegroundColor Yellow
for ($seconds = 30; $seconds -gt 0; $seconds--) {
    Write-Host -NoNewline "`r  Chờ thêm $seconds giây..."
    Start-Sleep -Seconds 1
}
Write-Host "`n✅ Đã qua 30 giây." -ForegroundColor Green

Write-Host "`n[6/6] 🔍 Kiểm tra dữ liệu được ghi vào Hive bằng Beeline (truy vấn qua Thrift Server)..." -ForegroundColor Yellow
Write-Host "Chạy: SELECT * FROM crypto_trades LIMIT 5;" -ForegroundColor DarkGray
docker exec master bash -c "beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n dack15 -e 'SELECT * FROM crypto_trades LIMIT 5;'"

Write-Host "`n==================================================================" -ForegroundColor Cyan
Write-Host "🎉 HOÀN THÀNH TEST LUỒNG HỆ THỐNG!" -ForegroundColor Green
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "👉 Hãy kiểm tra cửa số terminal của Spark Streaming xem có lỗi báo Exception không."
Write-Host "👉 Hãy truy cập Superset tại URL: http://localhost:8089 (Tài khoản: admin / Mật khẩu: admin)"
Write-Host "   🔗 Thêm Database connection với thông tin sau:"
Write-Host "      - Engine: Apache Hive"
Write-Host "      - SQLAlchemy URI: hive://dack15@master:10000/default?auth=NOSASL"
Write-Host "      - Nhấn: Test Connection"
