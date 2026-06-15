-- ODS层：原始数据表
DROP TABLE IF EXISTS ods_traffic;
CREATE EXTERNAL TABLE ods_traffic (
    block_id STRING,
    speed DOUBLE,
    golen DOUBLE,
    time_stamp BIGINT,
    period INT,
    exponent DOUBLE,
    go_time DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/traffic/ods'
TBLPROPERTIES ('skip.header.line.count'='1');