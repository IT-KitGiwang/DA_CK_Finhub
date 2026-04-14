-- =============================================================================
-- FINHUB CRYPTO PIPELINE — STAR SCHEMA DDL
-- Mô hình: 1 Fact Table + 4 Dimension Tables
-- Database: Apache Hive (Spark Thrift Server, port 10000)
-- Tác giả: DA_CK_Finhub Team
-- =============================================================================

-- -----------------------------------------------------------------------------
-- BƯỚC 0: Đảm bảo đang dùng đúng database
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS finhub;
USE finhub;


-- =============================================================================
-- DIMENSION TABLES
-- Tạo các bảng dimension trước (không có FK dependency)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIM_EXCHANGE — Thông tin sàn giao dịch
-- Dữ liệu tĩnh, seed thủ công (hiện tại chỉ có BINANCE)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_exchange;
CREATE TABLE dim_exchange (
    exchange_key        INT         COMMENT 'Surrogate key (PK)',
    exchange_name       STRING      COMMENT 'Tên viết tắt: BINANCE',
    exchange_fullname   STRING      COMMENT 'Tên đầy đủ: Binance Exchange',
    country             STRING      COMMENT 'Quốc gia đăng ký: Malta',
    exchange_type       STRING      COMMENT 'Loại sàn: CEX / DEX',
    quote_currency      STRING      COMMENT 'Đồng tiền định danh: USDT'
)
COMMENT 'Dimension: Thông tin sàn giao dịch'
STORED AS PARQUET;

-- Seed data cho DIM_EXCHANGE
INSERT INTO dim_exchange VALUES
    (1, 'BINANCE', 'Binance Exchange', 'Malta',   'CEX', 'USDT');


-- -----------------------------------------------------------------------------
-- DIM_SYMBOL — Thông tin đồng tiền crypto
-- Dữ liệu tĩnh, seed thủ công theo danh sách symbol trong .env
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_symbol;
CREATE TABLE dim_symbol (
    symbol_key          INT         COMMENT 'Surrogate key (PK)',
    symbol              STRING      COMMENT 'Symbol đầy đủ: BINANCE:BTCUSDT',
    symbol_code         STRING      COMMENT 'Mã giao dịch: BTCUSDT',
    base_currency       STRING      COMMENT 'Đồng gốc: BTC / ETH / BNB',
    quote_currency      STRING      COMMENT 'Đồng định giá: USDT',
    asset_name          STRING      COMMENT 'Tên đầy đủ: Bitcoin',
    asset_type          STRING      COMMENT 'Loại tài sản: Cryptocurrency',
    market_cap_tier     STRING      COMMENT 'Phân nhóm vốn hóa: Large-cap / Mid-cap'
)
COMMENT 'Dimension: Thông tin đồng tiền crypto'
STORED AS PARQUET;

-- Seed data cho DIM_SYMBOL
INSERT INTO dim_symbol VALUES
    (1, 'BINANCE:BTCUSDT', 'BTCUSDT', 'BTC', 'USDT', 'Bitcoin',  'Cryptocurrency', 'Large-cap'),
    (2, 'BINANCE:ETHUSDT', 'ETHUSDT', 'ETH', 'USDT', 'Ethereum', 'Cryptocurrency', 'Large-cap'),
    (3, 'BINANCE:BNBUSDT', 'BNBUSDT', 'BNB', 'USDT', 'BNB',      'Cryptocurrency', 'Mid-cap');


-- -----------------------------------------------------------------------------
-- DIM_DATE — Chiều thời gian theo ngày
-- Dữ liệu được generate bằng Spark hoặc insert thủ công cho range cần thiết
-- date_key format: YYYYMMDD (ví dụ: 20260414)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key            INT         COMMENT 'Surrogate key (PK): YYYYMMDD',
    full_date           STRING      COMMENT 'Ngày đầy đủ: 2026-04-14',
    year                INT         COMMENT 'Năm: 2026',
    quarter             INT         COMMENT 'Quý: 1/2/3/4',
    month               INT         COMMENT 'Tháng: 1-12',
    month_name          STRING      COMMENT 'Tên tháng: January...December',
    week_of_year        INT         COMMENT 'Tuần trong năm: 1-52',
    day_of_month        INT         COMMENT 'Ngày trong tháng: 1-31',
    day_of_week         INT         COMMENT 'Ngày trong tuần: 1=Mon...7=Sun',
    day_name            STRING      COMMENT 'Tên ngày: Monday...Sunday',
    is_weekend          BOOLEAN     COMMENT 'Là cuối tuần: true/false'
)
COMMENT 'Dimension: Chiều thời gian theo ngày'
STORED AS PARQUET;

-- Generate DIM_DATE từ bảng raw crypto_trades bằng Spark SQL
-- Chạy lệnh này sau khi đã có dữ liệu trong bảng nguồn
INSERT INTO dim_date
SELECT DISTINCT
    CAST(DATE_FORMAT(time, 'yyyyMMdd') AS INT)      AS date_key,
    DATE_FORMAT(time, 'yyyy-MM-dd')                  AS full_date,
    YEAR(time)                                        AS year,
    QUARTER(time)                                     AS quarter,
    MONTH(time)                                       AS month,
    DATE_FORMAT(time, 'MMMM')                         AS month_name,
    WEEKOFYEAR(time)                                  AS week_of_year,
    DAY(time)                                         AS day_of_month,
    DAYOFWEEK(time)                                   AS day_of_week,
    DATE_FORMAT(time, 'EEEE')                         AS day_name,
    CASE WHEN DAYOFWEEK(time) IN (1, 7) THEN TRUE
         ELSE FALSE END                               AS is_weekend
FROM crypto_trades
ORDER BY date_key;


-- -----------------------------------------------------------------------------
-- DIM_TIME — Chiều thời gian theo giờ:phút:giây
-- time_key format: HHMMSS (ví dụ: 164518)
-- trading_session dựa trên giờ UTC+7 (Việt Nam)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_time;
CREATE TABLE dim_time (
    time_key            INT         COMMENT 'Surrogate key (PK): HHMMSS',
    hour                INT         COMMENT 'Giờ: 0-23',
    minute              INT         COMMENT 'Phút: 0-59',
    second              INT         COMMENT 'Giây: 0-59',
    period              STRING      COMMENT 'Buổi: Morning/Afternoon/Evening/Night',
    trading_session     STRING      COMMENT 'Phiên: Asia/Europe/US/Off-hours'
)
COMMENT 'Dimension: Chiều thời gian theo giờ phút giây'
STORED AS PARQUET;

-- Generate DIM_TIME từ bảng raw crypto_trades bằng Spark SQL
INSERT INTO dim_time
SELECT DISTINCT
    CAST(DATE_FORMAT(time, 'HHmmss') AS INT)         AS time_key,
    HOUR(time)                                        AS hour,
    MINUTE(time)                                      AS minute,
    SECOND(time)                                      AS second,
    CASE
        WHEN HOUR(time) BETWEEN 6  AND 11 THEN 'Morning'
        WHEN HOUR(time) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN HOUR(time) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE                                   'Night'
    END                                               AS period,
    -- Phiên giao dịch (UTC+7):
    -- Asia:   01:00 - 09:00 (Tokyo, Singapore)
    -- Europe: 14:00 - 22:00 (London)
    -- US:     19:30 - 04:00 (New York)
    CASE
        WHEN HOUR(time) BETWEEN 1  AND 9  THEN 'Asia'
        WHEN HOUR(time) BETWEEN 14 AND 21 THEN 'Europe'
        WHEN HOUR(time) >= 19 OR HOUR(time) <= 4 THEN 'US'
        ELSE                                         'Off-hours'
    END                                               AS trading_session
FROM crypto_trades
ORDER BY time_key;


-- =============================================================================
-- FACT TABLE
-- Tạo sau khi đã có đủ dữ liệu trong tất cả dimension tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- FACT_CRYPTO_TRADES — Bảng sự kiện chính
-- Mỗi row = 1 giao dịch crypto được ghi nhận từ Finnhub
-- Partition by date_key để tối ưu query theo ngày
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS fact_crypto_trades;
CREATE TABLE fact_crypto_trades (
    trade_id            BIGINT      COMMENT 'Surrogate key tự tăng (PK)',
    date_key            INT         COMMENT 'FK → dim_date.date_key (YYYYMMDD)',
    time_key            INT         COMMENT 'FK → dim_time.time_key (HHMMSS)',
    symbol_key          INT         COMMENT 'FK → dim_symbol.symbol_key',
    exchange_key        INT         COMMENT 'FK → dim_exchange.exchange_key',
    price               DOUBLE      COMMENT 'Giá giao dịch tại thời điểm đó (USDT)',
    volume              DOUBLE      COMMENT 'Khối lượng giao dịch (số coin)',
    trade_value         DOUBLE      COMMENT 'Tổng giá trị = price × volume (USDT)',
    ingested_at         TIMESTAMP   COMMENT 'Thời điểm Spark ghi dữ liệu vào Hive'
)
COMMENT 'Fact Table: Mỗi giao dịch crypto real-time từ Finnhub'
PARTITIONED BY (trade_date STRING COMMENT 'Partition: yyyy-MM-dd để tăng tốc query theo ngày')
STORED AS PARQUET;


-- -----------------------------------------------------------------------------
-- ETL: Load dữ liệu từ bảng raw crypto_trades vào FACT_CRYPTO_TRADES
-- Chạy lệnh này sau khi đã tạo đủ các bảng dim và đã có data trong crypto_trades
-- -----------------------------------------------------------------------------
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO fact_crypto_trades PARTITION (trade_date)
SELECT
    -- Sinh trade_id bằng monotonically_increasing_id() trong Spark,
    -- hoặc dùng ROW_NUMBER() ở đây cho Spark SQL
    ROW_NUMBER() OVER (ORDER BY t.time)                             AS trade_id,

    -- FK: date_key từ DIM_DATE
    CAST(DATE_FORMAT(t.time, 'yyyyMMdd') AS INT)                    AS date_key,

    -- FK: time_key từ DIM_TIME
    CAST(DATE_FORMAT(t.time, 'HHmmss') AS INT)                      AS time_key,

    -- FK: symbol_key từ DIM_SYMBOL (lookup theo symbol string)
    ds.symbol_key                                                    AS symbol_key,

    -- FK: exchange_key từ DIM_EXCHANGE (hiện chỉ có BINANCE = 1)
    1                                                                AS exchange_key,

    -- Measures (các chỉ số đo lường)
    t.price                                                          AS price,
    t.volume                                                         AS volume,
    ROUND(t.price * t.volume, 6)                                     AS trade_value,

    -- Metadata
    CURRENT_TIMESTAMP()                                              AS ingested_at,

    -- Partition column
    DATE_FORMAT(t.time, 'yyyy-MM-dd')                               AS trade_date

FROM crypto_trades t
JOIN dim_symbol ds ON t.symbol = ds.symbol;


-- =============================================================================
-- KIỂM TRA DỮ LIỆU SAU KHI LOAD
-- =============================================================================

-- Đếm tổng số bản ghi trong từng bảng
SELECT 'dim_exchange'      AS table_name, COUNT(*) AS row_count FROM dim_exchange
UNION ALL
SELECT 'dim_symbol'        AS table_name, COUNT(*) AS row_count FROM dim_symbol
UNION ALL
SELECT 'dim_date'          AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL
SELECT 'dim_time'          AS table_name, COUNT(*) AS row_count FROM dim_time
UNION ALL
SELECT 'fact_crypto_trades' AS table_name, COUNT(*) AS row_count FROM fact_crypto_trades;


-- Kiểm tra dữ liệu mẫu trong FACT (kèm dim join)
SELECT
    f.trade_id,
    d.full_date,
    t.hour,
    t.minute,
    t.trading_session,
    s.asset_name,
    s.base_currency,
    e.exchange_name,
    f.price,
    f.volume,
    f.trade_value
FROM fact_crypto_trades f
JOIN dim_date     d ON f.date_key     = d.date_key
JOIN dim_time     t ON f.time_key     = t.time_key
JOIN dim_symbol   s ON f.symbol_key   = s.symbol_key
JOIN dim_exchange e ON f.exchange_key = e.exchange_key
LIMIT 20;


-- =============================================================================
-- CÁC QUERY PHÂN TÍCH CHO SUPERSET DASHBOARD
-- =============================================================================

-- Query 1: Giá trung bình theo giờ từng đồng tiền (Line Chart)
SELECT
    d.full_date,
    t.hour,
    s.asset_name,
    ROUND(AVG(f.price), 2)          AS avg_price,
    ROUND(MAX(f.price), 2)          AS high_price,
    ROUND(MIN(f.price), 2)          AS low_price
FROM fact_crypto_trades f
JOIN dim_date   d ON f.date_key   = d.date_key
JOIN dim_time   t ON f.time_key   = t.time_key
JOIN dim_symbol s ON f.symbol_key = s.symbol_key
GROUP BY d.full_date, t.hour, s.asset_name
ORDER BY d.full_date, t.hour;


-- Query 2: Tổng volume và giá trị giao dịch theo phiên (Bar Chart)
SELECT
    t.trading_session,
    s.asset_name,
    ROUND(SUM(f.volume), 4)         AS total_volume,
    ROUND(SUM(f.trade_value), 2)    AS total_usd_value,
    COUNT(f.trade_id)               AS trade_count
FROM fact_crypto_trades f
JOIN dim_time   t ON f.time_key   = t.time_key
JOIN dim_symbol s ON f.symbol_key = s.symbol_key
GROUP BY t.trading_session, s.asset_name
ORDER BY total_usd_value DESC;


-- Query 3: Bảng OHLC theo ngày (Candlestick / Table)
SELECT
    d.full_date,
    s.symbol_code,
    ROUND(FIRST_VALUE(f.price) OVER (
        PARTITION BY d.full_date, s.symbol_code
        ORDER BY f.time_key ASC), 2)                AS open_price,
    ROUND(LAST_VALUE(f.price) OVER (
        PARTITION BY d.full_date, s.symbol_code
        ORDER BY f.time_key ASC), 2)                AS close_price,
    ROUND(MAX(f.price), 2)                           AS high_price,
    ROUND(MIN(f.price), 2)                           AS low_price,
    ROUND(SUM(f.volume), 4)                          AS total_volume
FROM fact_crypto_trades f
JOIN dim_date   d ON f.date_key   = d.date_key
JOIN dim_symbol s ON f.symbol_key = s.symbol_key
GROUP BY d.full_date, s.symbol_code, f.price, f.time_key;


-- Query 4: Big Number — Giá mới nhất từng đồng tiền (Superset Big Number)
SELECT
    s.asset_name,
    s.base_currency,
    f.price         AS latest_price,
    f.trade_value   AS latest_trade_value,
    f.ingested_at   AS last_updated
FROM fact_crypto_trades f
JOIN dim_symbol s ON f.symbol_key = s.symbol_key
WHERE f.trade_id IN (
    SELECT MAX(trade_id)
    FROM fact_crypto_trades
    GROUP BY symbol_key
);
