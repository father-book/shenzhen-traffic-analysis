-- 1. 基础统计
SELECT COUNT(*) as total_records, ROUND(AVG(speed),2) as avg_speed FROM dwd_traffic;

-- 2. 拥堵等级分布
SELECT congestion_level, COUNT(*) as cnt FROM dwd_traffic GROUP BY congestion_level;

-- 3. 24小时模式
SELECT hour, COUNT(*) as vol FROM dwd_traffic GROUP BY hour ORDER BY hour;

-- 4. 最拥堵时段
SELECT hour, ROUND(AVG(speed),2) as avg_speed FROM dwd_traffic GROUP BY hour ORDER BY avg_speed LIMIT 5;

-- 5. 最繁忙时段
SELECT hour, COUNT(*) as vol FROM dwd_traffic GROUP BY hour ORDER BY vol DESC LIMIT 5;

-- 6. 星期模式
SELECT weekday_name, COUNT(*) as vol FROM dwd_traffic GROUP BY weekday_name;

-- 7. 最拥堵街道
SELECT block_id, ROUND(AVG(exponent),2) as avg_exp FROM dwd_traffic GROUP BY block_id ORDER BY avg_exp DESC LIMIT 10;

-- 8. 最繁忙街道
SELECT block_id, COUNT(*) as vol FROM dwd_traffic GROUP BY block_id ORDER BY vol DESC LIMIT 10;

-- 9. 周末对比
SELECT CASE WHEN is_weekend=true THEN '周末' ELSE '工作日' END as type, ROUND(AVG(speed),2) as avg_speed FROM dwd_traffic GROUP BY is_weekend;

-- 10. 时段统计
SELECT time_slot, COUNT(*) as vol FROM dwd_traffic GROUP BY time_slot;

-- 11. 速度分布
SELECT CASE WHEN speed<20 THEN '<20' WHEN speed<35 THEN '20-35' WHEN speed<50 THEN '35-50' ELSE '>50' END as range, COUNT(*) as cnt FROM dwd_traffic GROUP BY range;

-- 12. 相关性
SELECT ROUND(CORR(speed, exponent),4) as corr FROM dwd_traffic;