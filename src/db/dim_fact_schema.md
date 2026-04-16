# Mô Hình Dim/Fact — Finhub Crypto Real-Time Pipeline

> Mô hình Star Schema thiết kế cho dữ liệu crypto streaming từ Finnhub (Binance).  
> Nguồn dữ liệu: `time`, `symbol` (BINANCE:BTCUSDT/ETHUSDT/BNBUSDT), `price`, `volume`

---

## Sơ Đồ ER (Star Schema)

```mermaid
erDiagram
    FACT_CRYPTO_TRADES {
        bigint   trade_id        PK  "Surrogate key (auto-increment)"
        int      date_key        FK  "FK → DIM_DATE"
        int      time_key        FK  "FK → DIM_TIME"
        int      symbol_key      FK  "FK → DIM_SYMBOL"
        int      exchange_key    FK  "FK → DIM_EXCHANGE"
        double   price               "Giá giao dịch (USDT)"
        double   volume              "Khối lượng giao dịch (coin)"
        double   trade_value        "Tổng giá trị = price × volume"
        timestamp ingested_at        "Thời điểm Spark ghi vào Hive"
    }

    DIM_DATE {
        int      date_key        PK  "YYYYMMDD (ví dụ: 20260414)"
        date     full_date           "2026-04-14"
        int      year               "2026"
        int      quarter            "2"
        int      month              "4"
        string   month_name         "April"
        int      week_of_year       "16"
        int      day_of_month       "14"
        string   day_name           "Monday"
        boolean  is_weekend         "false"
    }

    DIM_TIME {
        int      time_key        PK  "HHMMSS (ví dụ: 164518)"
        int      hour               "16"
        int      minute             "45"
        int      second             "18"
        string   period             "Afternoon / Morning / Evening / Night"
        string   trading_session    "Asia / Europe / US / Off-hours"
    }

    DIM_SYMBOL {
        int      symbol_key      PK  "Surrogate key"
        string   symbol              "BINANCE:BTCUSDT"
        string   symbol_code        "BTCUSDT"
        string   base_currency      "BTC"
        string   quote_currency     "USDT"
        string   asset_type         "Cryptocurrency"
        string   asset_name         "Bitcoin"
        string   market_cap_tier    "Large-cap / Mid-cap / Small-cap"
    }

    DIM_EXCHANGE {
        int      exchange_key    PK  "Surrogate key"
        string   exchange_name      "BINANCE"
        string   exchange_fullname  "Binance Exchange"
        string   country            "Malta"
        string   exchange_type      "CEX"
        string   currency           "USDT"
    }

    FACT_CRYPTO_TRADES ||--o{ DIM_DATE     : "date_key"
    FACT_CRYPTO_TRADES ||--o{ DIM_TIME     : "time_key"
    FACT_CRYPTO_TRADES ||--o{ DIM_SYMBOL   : "symbol_key"
    FACT_CRYPTO_TRADES ||--o{ DIM_EXCHANGE : "exchange_key"
```

---

## Luồng Dữ Liệu Vào Mô Hình

```mermaid
flowchart LR
    A["📡 Finnhub WebSocket\ntime, symbol, price, volume"] 
    --> B["⚡ Kafka\ncrypto_trades topic"]
    --> C["🔥 Spark Streaming\nparse + transform"]
    --> D{"🏭 Dimension\nLookup / Generate"}
    D --> E["📅 DIM_DATE\n20260414"]
    D --> F["🕐 DIM_TIME\n164518"]
    D --> G["🪙 DIM_SYMBOL\nBINANCE:BTCUSDT"]
    D --> H["🏦 DIM_EXCHANGE\nBINANCE"]
    E & F & G & H --> I["⭐ FACT_CRYPTO_TRADES\nHive / HDFS Parquet"]
    I --> J["📊 Superset\nReal-time Dashboard"]
```

---

## Mapping Dữ Liệu Thực Tế → Mô Hình

| Trường gốc (CSV/Kafka) | Đích trong mô hình | Ghi chú |
|---|---|---|
| `time` = `2026-03-25 16:45:18` | `DIM_DATE.date_key = 20260325` | Extract ngày |
| `time` = `2026-03-25 16:45:18` | `DIM_TIME.time_key = 164518` | Extract giờ:phút:giây |
| `symbol` = `BINANCE:BTCUSDT` | `DIM_EXCHANGE.exchange_name = BINANCE` | Split by `:` |
| `symbol` = `BINANCE:BTCUSDT` | `DIM_SYMBOL.symbol_code = BTCUSDT` | Split by `:` |
| `symbol` = `BINANCE:BTCUSDT` | `DIM_SYMBOL.base_currency = BTC` | Split USDT |
| `price` = `71263.02` | `FACT.price = 71263.02` | Trực tiếp |
| `volume` = `0.002` | `FACT.volume = 0.002` | Trực tiếp |
| `price × volume` | `FACT.trade_value = 142.53` | Tính khi Spark transform |

---

## Các Query Phân Tích Điển Hình (Superset)

```sql
-- 1. Giá trung bình theo giờ từng đồng tiền
SELECT d.full_date, t.hour, s.symbol_code,
       AVG(f.price) as avg_price,
       SUM(f.trade_value) as total_value
FROM fact_crypto_trades f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_symbol s ON f.symbol_key = s.symbol_key
GROUP BY d.full_date, t.hour, s.symbol_code;

-- 2. Volume giao dịch theo phiên (Asia/Europe/US)
SELECT t.trading_session, s.symbol_code,
       SUM(f.volume) as total_volume,
       SUM(f.trade_value) as total_usd_value
FROM fact_crypto_trades f
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_symbol s ON f.symbol_key = s.symbol_key
GROUP BY t.trading_session, s.symbol_code;

-- 3. So sánh BTC vs ETH vs BNB theo ngày
SELECT d.full_date, s.asset_name,
       MIN(f.price) as low,
       MAX(f.price) as high,
       AVG(f.price) as avg_price
FROM fact_crypto_trades f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_symbol s ON f.symbol_key = s.symbol_key
GROUP BY d.full_date, s.asset_name;
```

---

## Hiện Tại vs Tương Lai

| | Hiện tại (Flat Table) | Sau nâng cấp (Star Schema) |
|---|---|---|
| **Bảng** | `crypto_trades` (1 bảng) | `fact_crypto_trades` + 4 dim |
| **Phân tích theo ngày/giờ** | Phải parse string mỗi lần | Dùng dim_date, dim_time sẵn |
| **Lọc theo đồng tiền** | WHERE symbol LIKE 'BINANCE:BTC%' | JOIN dim_symbol đơn giản |
| **Tính trade_value** | Phải tính trong mỗi query | Đã tính sẵn trong fact |
| **Performance** | Scan toàn bảng | Partition by date_key |
