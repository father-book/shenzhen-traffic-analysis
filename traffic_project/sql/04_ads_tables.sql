-- ADS层：交通指数日统计表
DROP TABLE IF EXISTS ads_exponent_daily;
CREATE TABLE ads_exponent_daily AS
SELECT 
    dt as stat_date,
    ROUND(AVG(exponent), 2) as avg_exponent,
    MAX(exponent) as max_exponent,
    MIN(exponent) as min_exponent
FROM dwd_traffic
GROUP BY dt;

-- ADS层：街道综合指标表
DROP TABLE IF EXISTS ads_block_comprehensive;
CREATE TABLE ads_block_comprehensive AS
SELECT 
    block_id,
    COUNT(*) as total_records,
    ROUND(AVG(speed), 2) as avg_speed,
    ROUND(AVG(exponent), 2) as avg_exponent,
    ROW_NUMBER() OVER (ORDER BY AVG(exponent) DESC) as rank_by_congestion
FROM dwd_traffic
GROUP BY block_id;

-- ADS层：小时级详细分析表
DROP TABLE IF EXISTS ads_hourly_detailed;
CREATE TABLE ads_hourly_detailed AS
SELECT 
    hour,
    COUNT(*) as total_records,
    ROUND(AVG(speed), 2) as avg_speed,
    ROUND(AVG(exponent), 2) as avg_exponent
FROM dwd_traffic
GROUP BY hour
ORDER BY hour;

-- ADS层：工作日周末对比表
DROP TABLE IF EXISTS ads_weekday_weekend_compare;
CREATE TABLE ads_weekday_weekend_compare AS
SELECT 
    CASE WHEN is_weekend = true THEN '周末' ELSE '工作日' END as day_type,
    COUNT(*) as total_records,
    ROUND(AVG(speed), 2) as avg_speed,
    ROUND(AVG(exponent), 2) as avg_exponent
FROM dwd_traffic
GROUP BY is_weekend;

-- ADS层：高峰时段分析表
DROP TABLE IF EXISTS ads_peak_hour_analysis;
CREATE TABLE ads_peak_hour_analysis AS
SELECT '早高峰' as peak_type, '7:00-9:00' as hour_range,
    ROUND(AVG(CASE WHEN hour BETWEEN 7 AND 9 THEN speed END), 2) as avg_speed,
    ROUND(AVG(CASE WHEN hour BETWEEN 7 AND 9 THEN exponent END), 2) as avg_exponent
FROM dwd_traffic;