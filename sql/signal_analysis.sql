-- =========================================
-- STEP 2: BASIC SIGNAL ACTIVITY
-- =========================================

-- 2.1 Basic signal activity for 5m
SELECT
    'ETH_5m' AS dataset,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN final_signal = TRUE THEN 1 ELSE 0 END) AS total_signals,
    ROUND(100.0 * SUM(CASE WHEN final_signal = TRUE THEN 1 ELSE 0 END) / COUNT(*), 4) AS signal_rate_pct,
    ROUND(AVG(CAST(match_score AS DOUBLE)), 4) AS avg_match_score
FROM read_csv_auto('paper_outputs/signals_5m.csv');

-- 2.2 Basic signal activity for 15m
SELECT
    'ETH_15m' AS dataset,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN final_signal = TRUE THEN 1 ELSE 0 END) AS total_signals,
    ROUND(100.0 * SUM(CASE WHEN final_signal = TRUE THEN 1 ELSE 0 END) / COUNT(*), 4) AS signal_rate_pct,
    ROUND(AVG(CAST(match_score AS DOUBLE)), 4) AS avg_match_score
FROM read_csv_auto('paper_outputs/signals_15m.csv');

-- 2.3 Combined basic signal activity comparison
SELECT
    dataset,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN final_signal = TRUE THEN 1 ELSE 0 END) AS total_signals,
    ROUND(100.0 * SUM(CASE WHEN final_signal = TRUE THEN 1 ELSE 0 END) / COUNT(*), 4) AS signal_rate_pct,
    ROUND(AVG(CAST(match_score AS DOUBLE)), 4) AS avg_match_score
FROM (
    SELECT 'ETH_5m' AS dataset, * FROM read_csv_auto('paper_outputs/signals_5m.csv')
    UNION ALL
    SELECT 'ETH_15m' AS dataset, * FROM read_csv_auto('paper_outputs/signals_15m.csv')
) t
GROUP BY dataset
ORDER BY dataset;

-- 2.4 Match score distribution for 5m
SELECT
    match_score,
    COUNT(*) AS row_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct_of_rows
FROM read_csv_auto('paper_outputs/signals_5m.csv')
GROUP BY match_score
ORDER BY match_score;

-- 2.5 Match score distribution for 15m
SELECT
    match_score,
    COUNT(*) AS row_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct_of_rows
FROM read_csv_auto('paper_outputs/signals_15m.csv')
GROUP BY match_score
ORDER BY match_score;

-- 2.6 Match score distribution for final signals only (5m)
SELECT
    match_score,
    COUNT(*) AS signal_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct_of_signals
FROM read_csv_auto('paper_outputs/signals_5m.csv')
WHERE final_signal = TRUE
GROUP BY match_score
ORDER BY match_score;

-- 2.7 Match score distribution for final signals only (15m)
SELECT
    match_score,
    COUNT(*) AS signal_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct_of_signals
FROM read_csv_auto('paper_outputs/signals_15m.csv')
WHERE final_signal = TRUE
GROUP BY match_score
ORDER BY match_score;


-- =========================================
-- STEP 3: FILTER DOMINANCE ANALYSIS
-- =========================================

-- 3.1 Raw filter frequency for 5m
SELECT
    'ETH_5m' AS dataset,
    SUM(CASE WHEN recent_high_break = TRUE THEN 1 ELSE 0 END) AS recent_high_break_count,
    SUM(CASE WHEN range_breakout = TRUE THEN 1 ELSE 0 END) AS range_breakout_count,
    SUM(CASE WHEN strong_candle = TRUE THEN 1 ELSE 0 END) AS strong_candle_count,
    SUM(CASE WHEN volume_spike = TRUE THEN 1 ELSE 0 END) AS volume_spike_count,
    SUM(CASE WHEN rsi_bounce = TRUE THEN 1 ELSE 0 END) AS rsi_bounce_count,
    SUM(CASE WHEN macd_cross_up = TRUE THEN 1 ELSE 0 END) AS macd_cross_up_count,
    SUM(CASE WHEN ema_trend = TRUE THEN 1 ELSE 0 END) AS ema_trend_count,
    SUM(CASE WHEN vwap_above = TRUE THEN 1 ELSE 0 END) AS vwap_above_count,
    SUM(CASE WHEN bb_upper_break = TRUE THEN 1 ELSE 0 END) AS bb_upper_break_count,
    SUM(CASE WHEN bb_squeeze_breakout = TRUE THEN 1 ELSE 0 END) AS bb_squeeze_breakout_count
FROM read_csv_auto('paper_outputs/signals_5m.csv');

-- 3.2 Raw filter frequency for 15m
SELECT
    'ETH_15m' AS dataset,
    SUM(CASE WHEN recent_high_break = TRUE THEN 1 ELSE 0 END) AS recent_high_break_count,
    SUM(CASE WHEN range_breakout = TRUE THEN 1 ELSE 0 END) AS range_breakout_count,
    SUM(CASE WHEN strong_candle = TRUE THEN 1 ELSE 0 END) AS strong_candle_count,
    SUM(CASE WHEN volume_spike = TRUE THEN 1 ELSE 0 END) AS volume_spike_count,
    SUM(CASE WHEN rsi_bounce = TRUE THEN 1 ELSE 0 END) AS rsi_bounce_count,
    SUM(CASE WHEN macd_cross_up = TRUE THEN 1 ELSE 0 END) AS macd_cross_up_count,
    SUM(CASE WHEN ema_trend = TRUE THEN 1 ELSE 0 END) AS ema_trend_count,
    SUM(CASE WHEN vwap_above = TRUE THEN 1 ELSE 0 END) AS vwap_above_count,
    SUM(CASE WHEN bb_upper_break = TRUE THEN 1 ELSE 0 END) AS bb_upper_break_count,
    SUM(CASE WHEN bb_squeeze_breakout = TRUE THEN 1 ELSE 0 END) AS bb_squeeze_breakout_count
FROM read_csv_auto('paper_outputs/signals_15m.csv');

-- 3.3 Filter frequency among final signals only (5m)
SELECT
    'ETH_5m' AS dataset,
    COUNT(*) AS final_signal_rows,
    SUM(CASE WHEN recent_high_break = TRUE THEN 1 ELSE 0 END) AS recent_high_break_count,
    SUM(CASE WHEN range_breakout = TRUE THEN 1 ELSE 0 END) AS range_breakout_count,
    SUM(CASE WHEN strong_candle = TRUE THEN 1 ELSE 0 END) AS strong_candle_count,
    SUM(CASE WHEN volume_spike = TRUE THEN 1 ELSE 0 END) AS volume_spike_count,
    SUM(CASE WHEN rsi_bounce = TRUE THEN 1 ELSE 0 END) AS rsi_bounce_count,
    SUM(CASE WHEN macd_cross_up = TRUE THEN 1 ELSE 0 END) AS macd_cross_up_count,
    SUM(CASE WHEN ema_trend = TRUE THEN 1 ELSE 0 END) AS ema_trend_count,
    SUM(CASE WHEN vwap_above = TRUE THEN 1 ELSE 0 END) AS vwap_above_count,
    SUM(CASE WHEN bb_upper_break = TRUE THEN 1 ELSE 0 END) AS bb_upper_break_count,
    SUM(CASE WHEN bb_squeeze_breakout = TRUE THEN 1 ELSE 0 END) AS bb_squeeze_breakout_count
FROM read_csv_auto('paper_outputs/signals_5m.csv')
WHERE final_signal = TRUE;

-- 3.4 Filter frequency among final signals only (15m)
SELECT
    'ETH_15m' AS dataset,
    COUNT(*) AS final_signal_rows,
    SUM(CASE WHEN recent_high_break = TRUE THEN 1 ELSE 0 END) AS recent_high_break_count,
    SUM(CASE WHEN range_breakout = TRUE THEN 1 ELSE 0 END) AS range_breakout_count,
    SUM(CASE WHEN strong_candle = TRUE THEN 1 ELSE 0 END) AS strong_candle_count,
    SUM(CASE WHEN volume_spike = TRUE THEN 1 ELSE 0 END) AS volume_spike_count,
    SUM(CASE WHEN rsi_bounce = TRUE THEN 1 ELSE 0 END) AS rsi_bounce_count,
    SUM(CASE WHEN macd_cross_up = TRUE THEN 1 ELSE 0 END) AS macd_cross_up_count,
    SUM(CASE WHEN ema_trend = TRUE THEN 1 ELSE 0 END) AS ema_trend_count,
    SUM(CASE WHEN vwap_above = TRUE THEN 1 ELSE 0 END) AS vwap_above_count,
    SUM(CASE WHEN bb_upper_break = TRUE THEN 1 ELSE 0 END) AS bb_upper_break_count,
    SUM(CASE WHEN bb_squeeze_breakout = TRUE THEN 1 ELSE 0 END) AS bb_squeeze_breakout_count
FROM read_csv_auto('paper_outputs/signals_15m.csv')
WHERE final_signal = TRUE;

-- 3.5 Filter activation rates (percentage of all rows) for 5m
SELECT
    'ETH_5m' AS dataset,
    ROUND(100.0 * AVG(CASE WHEN recent_high_break = TRUE THEN 1 ELSE 0 END), 4) AS recent_high_break_pct,
    ROUND(100.0 * AVG(CASE WHEN range_breakout = TRUE THEN 1 ELSE 0 END), 4) AS range_breakout_pct,
    ROUND(100.0 * AVG(CASE WHEN strong_candle = TRUE THEN 1 ELSE 0 END), 4) AS strong_candle_pct,
    ROUND(100.0 * AVG(CASE WHEN volume_spike = TRUE THEN 1 ELSE 0 END), 4) AS volume_spike_pct,
    ROUND(100.0 * AVG(CASE WHEN rsi_bounce = TRUE THEN 1 ELSE 0 END), 4) AS rsi_bounce_pct,
    ROUND(100.0 * AVG(CASE WHEN macd_cross_up = TRUE THEN 1 ELSE 0 END), 4) AS macd_cross_up_pct,
    ROUND(100.0 * AVG(CASE WHEN ema_trend = TRUE THEN 1 ELSE 0 END), 4) AS ema_trend_pct,
    ROUND(100.0 * AVG(CASE WHEN vwap_above = TRUE THEN 1 ELSE 0 END), 4) AS vwap_above_pct,
    ROUND(100.0 * AVG(CASE WHEN bb_upper_break = TRUE THEN 1 ELSE 0 END), 4) AS bb_upper_break_pct,
    ROUND(100.0 * AVG(CASE WHEN bb_squeeze_breakout = TRUE THEN 1 ELSE 0 END), 4) AS bb_squeeze_breakout_pct
FROM read_csv_auto('paper_outputs/signals_5m.csv');

-- 3.6 Filter activation rates (percentage of all rows) for 15m
SELECT
    'ETH_15m' AS dataset,
    ROUND(100.0 * AVG(CASE WHEN recent_high_break = TRUE THEN 1 ELSE 0 END), 4) AS recent_high_break_pct,
    ROUND(100.0 * AVG(CASE WHEN range_breakout = TRUE THEN 1 ELSE 0 END), 4) AS range_breakout_pct,
    ROUND(100.0 * AVG(CASE WHEN strong_candle = TRUE THEN 1 ELSE 0 END), 4) AS strong_candle_pct,
    ROUND(100.0 * AVG(CASE WHEN volume_spike = TRUE THEN 1 ELSE 0 END), 4) AS volume_spike_pct,
    ROUND(100.0 * AVG(CASE WHEN rsi_bounce = TRUE THEN 1 ELSE 0 END), 4) AS rsi_bounce_pct,
    ROUND(100.0 * AVG(CASE WHEN macd_cross_up = TRUE THEN 1 ELSE 0 END), 4) AS macd_cross_up_pct,
    ROUND(100.0 * AVG(CASE WHEN ema_trend = TRUE THEN 1 ELSE 0 END), 4) AS ema_trend_pct,
    ROUND(100.0 * AVG(CASE WHEN vwap_above = TRUE THEN 1 ELSE 0 END), 4) AS vwap_above_pct,
    ROUND(100.0 * AVG(CASE WHEN bb_upper_break = TRUE THEN 1 ELSE 0 END), 4) AS bb_upper_break_pct,
    ROUND(100.0 * AVG(CASE WHEN bb_squeeze_breakout = TRUE THEN 1 ELSE 0 END), 4) AS bb_squeeze_breakout_pct
FROM read_csv_auto('paper_outputs/signals_15m.csv');

-- 3.7 Filter activation rates inside final signals only (5m)
SELECT
    'ETH_5m' AS dataset,
    ROUND(100.0 * AVG(CASE WHEN recent_high_break = TRUE THEN 1 ELSE 0 END), 4) AS recent_high_break_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN range_breakout = TRUE THEN 1 ELSE 0 END), 4) AS range_breakout_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN strong_candle = TRUE THEN 1 ELSE 0 END), 4) AS strong_candle_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN volume_spike = TRUE THEN 1 ELSE 0 END), 4) AS volume_spike_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN rsi_bounce = TRUE THEN 1 ELSE 0 END), 4) AS rsi_bounce_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN macd_cross_up = TRUE THEN 1 ELSE 0 END), 4) AS macd_cross_up_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN ema_trend = TRUE THEN 1 ELSE 0 END), 4) AS ema_trend_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN vwap_above = TRUE THEN 1 ELSE 0 END), 4) AS vwap_above_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN bb_upper_break = TRUE THEN 1 ELSE 0 END), 4) AS bb_upper_break_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN bb_squeeze_breakout = TRUE THEN 1 ELSE 0 END), 4) AS bb_squeeze_breakout_pct_in_signals
FROM read_csv_auto('paper_outputs/signals_5m.csv')
WHERE final_signal = TRUE;

-- 3.8 Filter activation rates inside final signals only (15m)
SELECT
    'ETH_15m' AS dataset,
    ROUND(100.0 * AVG(CASE WHEN recent_high_break = TRUE THEN 1 ELSE 0 END), 4) AS recent_high_break_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN range_breakout = TRUE THEN 1 ELSE 0 END), 4) AS range_breakout_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN strong_candle = TRUE THEN 1 ELSE 0 END), 4) AS strong_candle_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN volume_spike = TRUE THEN 1 ELSE 0 END), 4) AS volume_spike_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN rsi_bounce = TRUE THEN 1 ELSE 0 END), 4) AS rsi_bounce_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN macd_cross_up = TRUE THEN 1 ELSE 0 END), 4) AS macd_cross_up_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN ema_trend = TRUE THEN 1 ELSE 0 END), 4) AS ema_trend_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN vwap_above = TRUE THEN 1 ELSE 0 END), 4) AS vwap_above_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN bb_upper_break = TRUE THEN 1 ELSE 0 END), 4) AS bb_upper_break_pct_in_signals,
    ROUND(100.0 * AVG(CASE WHEN bb_squeeze_breakout = TRUE THEN 1 ELSE 0 END), 4) AS bb_squeeze_breakout_pct_in_signals
FROM read_csv_auto('paper_outputs/signals_15m.csv')
WHERE final_signal = TRUE;