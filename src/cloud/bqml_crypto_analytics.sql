-- Khởi tạo Mô hình Machine Learning Trực tiếp trên Data Warehouse (BigQuery ML)
-- Phân tích dữ liệu chứng khoán realtime mà không cần di chuyển dữ liệu ra ngoài

-- 1. TẠO MÔ HÌNH DỰ ĐOÁN GIÁ (ARIMA_PLUS Time-series Forecasting)
-- Mô hình này sẽ tự động học các mẫu chuỗi thời gian (mùa vụ, xu hướng) của dữ liệu giá
CREATE OR REPLACE MODEL `finhub_dw.crypto_price_forecaster`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='timestamp',
  time_series_data_col='price',
  time_series_id_col='symbol',
  data_frequency='PER_MINUTE',   -- Dữ liệu streaming update theo phút
  horizon=60                     -- Dự đoán trước 60 phút
) AS
SELECT 
  timestamp,
  symbol,
  price
FROM 
  `finhub_dw.streaming_crypto_trades`
WHERE 
  timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY); -- Train trên dữ liệu 7 ngày gần nhất


-- 2. TRUY VẤN DỰ ĐOÁN (Forecasting & Visualization in Looker)
-- Query này trả về khoảng tin cậy (Confidence Interval) và giá dự đoán, rất phù hợp vẽ biểu đồ mảng
SELECT
  *
FROM
  ML.FORECAST(MODEL `finhub_dw.crypto_price_forecaster`,
              STRUCT(30 AS horizon, 0.9 AS confidence_level))
ORDER BY
  symbol, forecast_timestamp;


-- 3. TẠO MÔ HÌNH PHÁT HIỆN BẤT THƯỜNG (Anomaly Detection)
-- K-Means Clustering để phát hiện thao túng giá (Pump/Dump) hoặc khối lượng đột biến
CREATE OR REPLACE MODEL `finhub_dw.crypto_anomaly_detector`
OPTIONS(
  model_type='kmeans',
  num_clusters=4,
  standardize_features=TRUE
) AS
SELECT
  symbol,
  price,
  volume
FROM
  `finhub_dw.streaming_crypto_trades`
WHERE 
  timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY);


-- 4. TRUY VẤN TÌM KIẾM CÁC GIAO DỊCH BẤT THƯỜNG (Real-time Anomaly Detection)
-- Tìm các giao dịch bị mô hình đánh giá là "Bất thường" (Anomaly) do cách xa các trung tâm cụm
SELECT
  *
FROM
  ML.DETECT_ANOMALIES(
    MODEL `finhub_dw.crypto_anomaly_detector`,
    STRUCT(0.02 AS contamination), -- Khai báo 2% dữ liệu là gian lận/bất thường
    (
      SELECT symbol, price, volume, timestamp 
      FROM `finhub_dw.streaming_crypto_trades`
      WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
    )
  )
WHERE is_anomaly = TRUE
ORDER BY timestamp DESC;
