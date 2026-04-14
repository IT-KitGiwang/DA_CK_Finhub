-- =============================================================================
-- FINHUB CRYPTO PIPELINE — REAL-TIME STAR SCHEMA (HỆ VIEW)
-- =============================================================================
-- Kiến trúc: Logical Data Warehouse trên Lakehouse
--   • Dim tĩnh (Physical): dim_symbol, dim_exchange  → Lưu thật trên HDFS
--   • Dim động (View):     vw_dim_date, vw_dim_time  → Tự sinh từ dữ liệu
--   • Fact (View):         vw_fact_crypto_trades     → Real-time, không tốn ổ cứng
--
-- Bảng nguồn: crypto_trades (do Spark Streaming ghi liên tục)
-- Database:   default (Spark Thrift Server, port 10000)
-- =============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  PHẦN 1: DIMENSION VẬT LÝ (PHYSICAL TABLES)                            ║
-- ║  Dữ liệu tĩnh, seed thủ công, lưu trên HDFS dạng Parquet              ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- -----------------------------------------------------------------------------
-- DIM_EXCHANGE — Thông tin sàn giao dịch
-- Dữ liệu Master Data: hiện tại chỉ có Binance
-- Khi mở rộng thêm sàn mới (Coinbase, OKX...) → INSERT thêm dòng
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS dim_exchange;

CREATE TABLE dim_exchange (
    exchange_key        INT         COMMENT 'Surrogate key (PK)',
    exchange_name       STRING      COMMENT 'Tên viết tắt: BINANCE',
    exchange_fullname   STRING      COMMENT 'Tên đầy đủ: Binance Exchange',
    country             STRING      COMMENT 'Quốc gia đăng ký',
    exchange_type       STRING      COMMENT 'Loại sàn: CEX (Tập trung) / DEX (Phi tập trung)',
    quote_currency      STRING      COMMENT 'Đồng tiền định giá mặc định: USDT'
)
COMMENT 'Dimension vật lý: Thông tin sàn giao dịch tiền mã hóa'
STORED AS PARQUET;

INSERT INTO dim_exchange VALUES
    (1, 'BINANCE', 'Binance Exchange', 'Malta', 'CEX', 'USDT');


-- -----------------------------------------------------------------------------
-- DIM_SYMBOL — Thông tin đồng tiền crypto
-- Dữ liệu Master Data: 3 đồng đang theo dõi (BTC, ETH, BNB)
-- Khi thêm đồng mới vào .env → INSERT thêm dòng tương ứng
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
    market_cap_tier     STRING      COMMENT 'Phân nhóm vốn hóa thị trường'
)
COMMENT 'Dimension vật lý: Thông tin đồng tiền mã hóa'
STORED AS PARQUET;

INSERT INTO dim_symbol VALUES
    (1, 'BINANCE:BTCUSDT', 'BTCUSDT', 'BTC', 'USDT', 'Bitcoin',  'Cryptocurrency', 'Large-cap'),
    (2, 'BINANCE:ETHUSDT', 'ETHUSDT', 'ETH', 'USDT', 'Ethereum', 'Cryptocurrency', 'Large-cap'),
    (3, 'BINANCE:BNBUSDT', 'BNBUSDT', 'BNB', 'USDT', 'BNB',      'Cryptocurrency', 'Mid-cap');


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  PHẦN 2: DIMENSION ĐỘNG (LOGICAL VIEWS)                                 ║
-- ║  Tự sinh từ dữ liệu trong bảng crypto_trades                           ║
-- ║  Không chiếm ổ cứng, luôn cập nhật real-time                           ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- -----------------------------------------------------------------------------
-- VW_DIM_DATE — Chiều thời gian theo NGÀY (View ảo)
-- Tự động sinh ra ngày mới khi Spark ghi dữ liệu ngày mới vào crypto_trades
-- Không cần chạy batch INSERT định kỳ
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS vw_dim_date;

CREATE VIEW vw_dim_date AS
SELECT DISTINCT
    CAST(DATE_FORMAT(time, 'yyyyMMdd') AS INT)       AS date_key,
    CAST(time AS DATE)                                AS full_date,
    YEAR(time)                                        AS year,
    QUARTER(time)                                     AS quarter,
    MONTH(time)                                       AS month,
    DATE_FORMAT(time, 'MMMM')                         AS month_name,
    WEEKOFYEAR(time)                                  AS week_of_year,
    DAY(time)                                         AS day_of_month,
    DAYOFWEEK(time)                                   AS day_of_week,
    DATE_FORMAT(time, 'EEEE')                         AS day_name,
    CASE
        WHEN DAYOFWEEK(time) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                               AS is_weekend
FROM crypto_trades;


-- -----------------------------------------------------------------------------
-- VW_DIM_TIME — Chiều thời gian theo GIỜ:PHÚT:GIÂY (View ảo)
-- Bao gồm phân loại buổi (Morning/Afternoon/Evening/Night)
-- và phiên giao dịch quốc tế (Asia/Europe/US) theo múi giờ UTC+7
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS vw_dim_time;

CREATE VIEW vw_dim_time AS
SELECT DISTINCT
    CAST(DATE_FORMAT(time, 'HHmmss') AS INT)          AS time_key,
    HOUR(time)                                        AS hour,
    MINUTE(time)                                      AS minute,
    SECOND(time)                                      AS second,
    CASE
        WHEN HOUR(time) BETWEEN  6 AND 11 THEN 'Morning'
        WHEN HOUR(time) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN HOUR(time) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE                                    'Night'
    END                                               AS period,
    CASE
        WHEN HOUR(time) BETWEEN  1 AND  9 THEN 'Asia'
        WHEN HOUR(time) BETWEEN 14 AND 21 THEN 'Europe'
        WHEN HOUR(time) >= 20 OR HOUR(time) <= 4 THEN 'US'
        ELSE                                         'Off-hours'
    END                                               AS trading_session
FROM crypto_trades;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  PHẦN 3: FACT TABLE (LOGICAL VIEW)                                      ║
-- ║  Trái tim của Star Schema — View trỏ thẳng vào luồng Streaming         ║
-- ║  Real-time 100%: Mỗi lần Superset query = lấy data mới nhất            ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- -----------------------------------------------------------------------------
-- VW_FACT_CRYPTO_TRADES — Bảng sự kiện chính (View ảo)
--
-- Mỗi dòng = 1 giao dịch crypto đã qua bước Data Cleaning trên Spark
-- View này JOIN bảng crypto_trades (đang real-time) với dim_symbol
-- để gắn symbol_key, đồng thời tính date_key, time_key, trade_value
--
-- Ưu điểm:
--   • Không chiếm thêm dung lượng HDFS (0 bytes dư thừa)
--   • Luôn phản ánh dữ liệu mới nhất (real-time)
--   • Superset query trực tiếp trên View = Star Schema chuẩn
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS vw_fact_crypto_trades;

CREATE VIEW vw_fact_crypto_trades AS
SELECT
    -- ═══ Surrogate Key ═══
    ROW_NUMBER() OVER (ORDER BY t.time)                        AS trade_id,

    -- ═══ Foreign Keys (liên kết tới Dimensions) ═══
    CAST(DATE_FORMAT(t.time, 'yyyyMMdd') AS INT)               AS date_key,
    CAST(DATE_FORMAT(t.time, 'HHmmss')  AS INT)                AS time_key,
    ds.symbol_key                                               AS symbol_key,
    1                                                           AS exchange_key,

    -- ═══ Measures (Chỉ số đo lường) ═══
    t.price                                                     AS price,
    t.volume                                                    AS volume,
    ROUND(t.price * t.volume, 6)                                AS trade_value,

    -- ═══ Metadata ═══
    t.time                                                      AS trade_time

FROM crypto_trades t
JOIN dim_symbol ds ON t.symbol = ds.symbol;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  PHẦN 4: KIỂM TRA DỮ LIỆU (VALIDATION)                                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- 4.1: Đếm số dòng trong từng bảng/view
SELECT 'dim_exchange (Physical)'         AS object_name, COUNT(*) AS rows FROM dim_exchange
UNION ALL
SELECT 'dim_symbol (Physical)'           AS object_name, COUNT(*) AS rows FROM dim_symbol
UNION ALL
SELECT 'vw_dim_date (View)'              AS object_name, COUNT(*) AS rows FROM vw_dim_date
UNION ALL
SELECT 'vw_dim_time (View)'              AS object_name, COUNT(*) AS rows FROM vw_dim_time
UNION ALL
SELECT 'vw_fact_crypto_trades (View)'    AS object_name, COUNT(*) AS rows FROM vw_fact_crypto_trades;


-- 4.2: Xem dữ liệu mẫu trong Fact View (kèm join đầy đủ Star Schema)
SELECT
    f.trade_id,
    f.trade_time,
    dd.full_date,
    dd.day_name,
    dt.hour,
    dt.minute,
    dt.trading_session,
    ds.asset_name,
    ds.base_currency,
    ds.market_cap_tier,
    de.exchange_name,
    f.price,
    f.volume,
    f.trade_value
FROM vw_fact_crypto_trades f
JOIN vw_dim_date   dd ON f.date_key     = dd.date_key
JOIN vw_dim_time   dt ON f.time_key     = dt.time_key
JOIN dim_symbol    ds ON f.symbol_key   = ds.symbol_key
JOIN dim_exchange  de ON f.exchange_key = de.exchange_key
ORDER BY f.trade_time DESC
LIMIT 20;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  PHẦN 5: CÁC QUERY PHÂN TÍCH CHO SUPERSET DASHBOARD                    ║
-- ║  Copy trực tiếp vào SQL Lab của Superset để tạo Dataset / Chart         ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝


-- ─────────────────────────────────────────────────────────────────────────────
-- CHART 1: Line Chart — Giá trung bình theo giờ từng đồng tiền
-- Dùng cho: Time-series line chart trong Superset
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    dd.full_date,
    dt.hour,
    ds.asset_name,
    ROUND(AVG(f.price), 2)       AS avg_price,
    ROUND(MAX(f.price), 2)       AS high_price,
    ROUND(MIN(f.price), 2)       AS low_price,
    COUNT(*)                     AS trade_count
FROM vw_fact_crypto_trades f
JOIN vw_dim_date   dd ON f.date_key   = dd.date_key
JOIN vw_dim_time   dt ON f.time_key   = dt.time_key
JOIN dim_symbol    ds ON f.symbol_key = ds.symbol_key
GROUP BY dd.full_date, dt.hour, ds.asset_name
ORDER BY dd.full_date, dt.hour;


-- ─────────────────────────────────────────────────────────────────────────────
-- CHART 2: Bar Chart — Volume theo phiên giao dịch (Asia/Europe/US)
-- Dùng cho: Grouped bar chart so sánh khối lượng
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    dt.trading_session,
    ds.asset_name,
    ROUND(SUM(f.volume), 4)      AS total_volume,
    ROUND(SUM(f.trade_value), 2) AS total_usd_value,
    COUNT(*)                     AS trade_count
FROM vw_fact_crypto_trades f
JOIN vw_dim_time   dt ON f.time_key   = dt.time_key
JOIN dim_symbol    ds ON f.symbol_key = ds.symbol_key
GROUP BY dt.trading_session, ds.asset_name
ORDER BY total_usd_value DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- CHART 3: Pie Chart — Tỷ trọng giao dịch theo đồng tiền
-- Dùng cho: Donut/Pie chart phân bổ portfolio
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ds.asset_name,
    ds.market_cap_tier,
    COUNT(*)                          AS so_giao_dich,
    ROUND(SUM(f.trade_value), 2)     AS tong_gia_tri_usd,
    ROUND(SUM(f.volume), 4)          AS tong_volume
FROM vw_fact_crypto_trades f
JOIN dim_symbol ds ON f.symbol_key = ds.symbol_key
GROUP BY ds.asset_name, ds.market_cap_tier;


-- ─────────────────────────────────────────────────────────────────────────────
-- CHART 4: Big Number — Giá mới nhất từng đồng (Real-time indicator)
-- Dùng cho: Big Number card trên đầu Dashboard
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    ds.asset_name,
    ds.base_currency,
    f.price                AS latest_price,
    f.volume               AS latest_volume,
    f.trade_value          AS latest_trade_value,
    f.trade_time           AS last_updated
FROM vw_fact_crypto_trades f
JOIN dim_symbol ds ON f.symbol_key = ds.symbol_key
WHERE f.trade_time = (
    SELECT MAX(f2.trade_time)
    FROM vw_fact_crypto_trades f2
    WHERE f2.symbol_key = f.symbol_key
);


-- ─────────────────────────────────────────────────────────────────────────────
-- CHART 5: Table — Top giao dịch có giá trị lớn nhất (Whale Alert)
-- Dùng cho: Data table sắp xếp theo trade_value giảm dần
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    f.trade_time,
    ds.asset_name,
    de.exchange_name,
    f.price,
    f.volume,
    f.trade_value          AS value_usd,
    dt.trading_session
FROM vw_fact_crypto_trades f
JOIN dim_symbol    ds ON f.symbol_key   = ds.symbol_key
JOIN dim_exchange  de ON f.exchange_key = de.exchange_key
JOIN vw_dim_time   dt ON f.time_key    = dt.time_key
ORDER BY f.trade_value DESC
LIMIT 50;


-- ─────────────────────────────────────────────────────────────────────────────
-- CHART 6: Heatmap — Phân bố giao dịch theo Giờ × Ngày trong tuần
-- Dùng cho: Calendar heatmap trong Superset
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    dd.day_name,
    dt.hour,
    COUNT(*)                         AS trade_count,
    ROUND(AVG(f.price), 2)          AS avg_price,
    ROUND(SUM(f.trade_value), 2)    AS total_value
FROM vw_fact_crypto_trades f
JOIN vw_dim_date dd ON f.date_key = dd.date_key
JOIN vw_dim_time dt ON f.time_key = dt.time_key
GROUP BY dd.day_name, dt.hour
ORDER BY dd.day_name, dt.hour;
