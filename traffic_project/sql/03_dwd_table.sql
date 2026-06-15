-- DWD层：明细数据表
DROP TABLE IF EXISTS dwd_traffic;
CREATE TABLE dwd_traffic (
    block_id STRING,
    speed DOUBLE,
    golen DOUBLE,
    period INT,
    exponent DOUBLE,
    go_time DOUBLE,
    event_time TIMESTAMP,
    hour INT,
    weekday INT,
    weekday_name STRING,
    is_weekend BOOLEAN,
    time_slot STRING,
    congestion_level STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_traffic PARTITION(dt)
SELECT 
    block_id,
    CASE WHEN speed <= 0 OR speed IS NULL THEN 30 ELSE speed END as speed,
    COALESCE(golen, 0) as golen,
    period,
    CASE WHEN exponent < 0 THEN 0 WHEN exponent > 10 THEN 10 ELSE exponent END as exponent,
    COALESCE(go_time, 0) as go_time,
    FROM_UNIXTIME(CAST(time_stamp / 1000 AS BIGINT)) as event_time,
    HOUR(FROM_UNIXTIME(CAST(time_stamp / 1000 AS BIGINT))) as hour,
    DAYOFWEEK(FROM_UNIXTIME(CAST(time_stamp / 1000 AS BIGINT))) as weekday,
    CASE DAYOFWEEK(FROM_UNIXTIME(CAST(time_stamp / 1000 AS BIGINT)))
        WHEN 1 THEN '周日' WHEN 2 THEN '周一' WHEN 3 THEN '周二'
        WHEN 4 THEN '周三' WHEN 5 THEN '周四' WHEN 6 THEN '周五'
        WHEN 7 THEN '周六'
    END as weekday_name,
    CASE WHEN DAYOFWEEK(FROM_UNIXTIME(CAST(time_stamp / 1000 AS BIGINT))) IN (1, 7) THEN true ELSE false END as is_weekend,
    CASE 
        WHEN HOUR(FROM_UNIXTIME(CAST(time_stamp / 1000 AS BIGINT))) BETWEEN 7 AND 9 THEN '早高峰'
        WHEN HOUR(FROM_UNIXTIME(CAST(time_stamp / 1000 AS BIGINT))) BETWEEN 17 AND 19 THEN '晚高峰'
        ELSE '平峰'
    END as time_slot,
    CASE 
        WHEN exponent <= 2 THEN '畅通'
        WHEN exponent <= 4 THEN '基本畅通'
        WHEN exponent <= 6 THEN '缓行'
        WHEN exponent <= 8 THEN '较拥堵'
        ELSE '拥堵'
    END as congestion_level,
    DATE_FORMAT(FROM_UNIXTIME(CAST(time_stamp / 1000 AS BIGINT)), 'yyyy-MM-dd') as dt
FROM ods_traffic
WHERE time_stamp IS NOT NULL AND block_id IS NOT NULL;