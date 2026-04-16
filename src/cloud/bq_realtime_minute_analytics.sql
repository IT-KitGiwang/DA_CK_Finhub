-- --------------------------------------------------------------------------------------
-- BIGQUERY DASHBOARD: CRYPTO MARKET ANALYTICS
-- Data Source: streaming_crypto_trades (Realtime pipeline)
-- GHI CHÚ: processed_at lưu dạng STRING/DATETIME → phải ép TIMESTAMP() khi so sánh
-- --------------------------------------------------------------------------------------

-- --------------------------------------------------------------------------------------
-- 1. Cơ Cấu Dòng Vốn Crypto (60 Phút)
-- --------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `phan-tich-du-lieu-lon.finhub_dw.vw_1_ti_trong_dong_tien_usd_1h` AS
SELECT
    CASE symbol
        WHEN 'BINANCE:BTCUSDT'  THEN 'Bitcoin'
        WHEN 'BINANCE:ETHUSDT'  THEN 'Ethereum'
        WHEN 'BINANCE:BNBUSDT'  THEN 'BNB'
        WHEN 'BINANCE:SOLUSDT'  THEN 'Solana'
        ELSE symbol
    END AS Dong_Coin,
    ROUND(SUM(price * volume), 2) AS Tong_Tien_USD
FROM `phan-tich-du-lieu-lon.finhub_dw.streaming_crypto_trades`
WHERE TIMESTAMP(processed_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY Dong_Coin;


-- --------------------------------------------------------------------------------------
-- 2. Biên Độ Dao Động Giá (60 Phút)
-- --------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `phan-tich-du-lieu-lon.finhub_dw.vw_2_bien_do_gia_min_max_1h` AS
SELECT
    CASE symbol
        WHEN 'BINANCE:BTCUSDT'  THEN 'Bitcoin'
        WHEN 'BINANCE:ETHUSDT'  THEN 'Ethereum'
        WHEN 'BINANCE:BNBUSDT'  THEN 'BNB'
        WHEN 'BINANCE:SOLUSDT'  THEN 'Solana'
        ELSE symbol
    END AS Dong_Coin,
    ROUND(MIN(price), 4) AS Gia_Thap_Nhat,
    ROUND(MAX(price), 4) AS Gia_Cao_Nhat
FROM `phan-tich-du-lieu-lon.finhub_dw.streaming_crypto_trades`
WHERE TIMESTAMP(processed_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
GROUP BY Dong_Coin;


-- --------------------------------------------------------------------------------------
-- 3. Xung Lực Giao Dịch Theo Tài Sản
-- --------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `phan-tich-du-lieu-lon.finhub_dw.vw_3_suc_nong_giao_dich_1h` AS
SELECT
    CASE symbol
        WHEN 'BINANCE:BTCUSDT'  THEN 'Bitcoin'
        WHEN 'BINANCE:ETHUSDT'  THEN 'Ethereum'
        WHEN 'BINANCE:BNBUSDT'  THEN 'BNB'
        WHEN 'BINANCE:SOLUSDT'  THEN 'Solana'
        ELSE symbol
    END AS Dong_Coin,
    COUNT(1) AS Tong_So_Khop_Lenh
FROM `phan-tich-du-lieu-lon.finhub_dw.streaming_crypto_trades`
WHERE TIMESTAMP(processed_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY Dong_Coin;


-- --------------------------------------------------------------------------------------
-- 4. Biến Động Thanh Khoản & Tần Suất Lệnh
-- --------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `phan-tich-du-lieu-lon.finhub_dw.vw_4_tuong_quan_tien_va_lenh_1h` AS
SELECT
    TIMESTAMP_TRUNC(TIMESTAMP(processed_at), MINUTE) AS Phut_Giao_Dich,
    COUNT(1) AS So_Luong_Lenh,
    ROUND(SUM(price * volume), 2) AS Tong_Tien_USD_Khop
FROM `phan-tich-du-lieu-lon.finhub_dw.streaming_crypto_trades`
WHERE TIMESTAMP(processed_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY Phut_Giao_Dich
ORDER BY Phut_Giao_Dich ASC;


-- --------------------------------------------------------------------------------------
-- 5. Chỉ Số Giá Tham Chiếu Real-time (VWAP)
-- --------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `phan-tich-du-lieu-lon.finhub_dw.vw_5_xu_huong_gia_trung_binh_5p` AS
SELECT
    TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(TIMESTAMP(processed_at)), 300) * 300) AS Moc_Thoi_Gian_5_Phut,
    CASE symbol
        WHEN 'BINANCE:BTCUSDT'  THEN 'Bitcoin'
        WHEN 'BINANCE:ETHUSDT'  THEN 'Ethereum'
        WHEN 'BINANCE:BNBUSDT'  THEN 'BNB'
        WHEN 'BINANCE:SOLUSDT'  THEN 'Solana'
        ELSE symbol
    END AS Dong_Coin,
    ROUND(SUM(price * volume) / SUM(volume), 4) AS Gia_Chot_Trung_Binh
FROM `phan-tich-du-lieu-lon.finhub_dw.streaming_crypto_trades`
WHERE TIMESTAMP(processed_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY Moc_Thoi_Gian_5_Phut, Dong_Coin
ORDER BY Moc_Thoi_Gian_5_Phut ASC;


-- --------------------------------------------------------------------------------------
-- 6. Lệnh Giao Dịch Đột Biến (Whale Alert)
-- --------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `phan-tich-du-lieu-lon.finhub_dw.vw_6_ky_luc_lenh_ca_map_hom_nay` AS
SELECT
    CASE symbol
        WHEN 'BINANCE:BTCUSDT'  THEN 'Bitcoin'
        WHEN 'BINANCE:ETHUSDT'  THEN 'Ethereum'
        WHEN 'BINANCE:BNBUSDT'  THEN 'BNB'
        WHEN 'BINANCE:SOLUSDT'  THEN 'Solana'
        ELSE symbol
    END AS Dong_Coin,
    ROUND(MAX(price * volume), 2) AS Lenh_Bom_Tien_Khung_Nhat_USD
FROM `phan-tich-du-lieu-lon.finhub_dw.streaming_crypto_trades`
WHERE DATE(TIMESTAMP(processed_at)) = CURRENT_DATE()
GROUP BY Dong_Coin;


-- --------------------------------------------------------------------------------------
-- 7. Mật Độ Phân Bổ Giao Dịch 5P
-- --------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `phan-tich-du-lieu-lon.finhub_dw.vw_7_ban_do_mat_do_lenh_5p` AS
SELECT
    TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(TIMESTAMP(processed_at)), 300) * 300) AS Moc_Thoi_Gian_5_Phut,
    CASE symbol
        WHEN 'BINANCE:BTCUSDT'  THEN 'Bitcoin'
        WHEN 'BINANCE:ETHUSDT'  THEN 'Ethereum'
        WHEN 'BINANCE:BNBUSDT'  THEN 'BNB'
        WHEN 'BINANCE:SOLUSDT'  THEN 'Solana'
        ELSE symbol
    END AS Dong_Coin,
    COUNT(1) AS So_Lan_Xuat_Chieu
FROM `phan-tich-du-lieu-lon.finhub_dw.streaming_crypto_trades`
WHERE TIMESTAMP(processed_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY Moc_Thoi_Gian_5_Phut, Dong_Coin
ORDER BY Moc_Thoi_Gian_5_Phut ASC;
