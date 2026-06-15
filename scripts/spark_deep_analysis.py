#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深圳市交通监控数据 - Spark深度分析
包含20+个分析维度和高级统计
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd
import json

# ============================================
# 初始化
# ============================================
spark = SparkSession.builder \
    .appName("ShenzhenTrafficDeepAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 100)
print("深圳市交通监控数据 - Spark深度分析报告")
print("=" * 100)

# 读取数据
df = spark.sql("SELECT * FROM traffic.dwd_traffic")
total_count = df.count()
print(f"数据总量: {total_count:,} 条")

# 注册临时视图
df.createOrReplaceTempView("traffic_view")

# ============================================
# 分析1：基础统计分析
# ============================================
print("\n" + "=" * 100)
print("【分析1】基础统计分析")
print("=" * 100)

basic_stats = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT block_id) as total_blocks,
        MIN(dt) as start_date,
        MAX(dt) as end_date,
        DATEDIFF(MAX(dt), MIN(dt)) as date_range_days,
        -- 车速统计
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(STDDEV(speed), 2) as speed_stddev,
        MIN(speed) as min_speed,
        MAX(speed) as max_speed,
        PERCENTILE_APPROX(speed, 0.25) as p25_speed,
        PERCENTILE_APPROX(speed, 0.50) as p50_speed,
        PERCENTILE_APPROX(speed, 0.75) as p75_speed,
        PERCENTILE_APPROX(speed, 0.90) as p90_speed,
        PERCENTILE_APPROX(speed, 0.95) as p95_speed,
        PERCENTILE_APPROX(speed, 0.99) as p99_speed,
        -- 交通指数统计
        ROUND(AVG(exponent), 2) as avg_exponent,
        MIN(exponent) as min_exponent,
        MAX(exponent) as max_exponent,
        -- 通行时间统计
        ROUND(AVG(go_time), 2) as avg_go_time,
        ROUND(SUM(go_time) / 3600, 2) as total_go_time_hours,
        ROUND(SUM(golen) / 1000, 2) as total_distance_km
    FROM traffic_view
""")

basic_stats.show(truncate=False)

# ============================================
# 分析2：拥堵等级分布
# ============================================
print("\n" + "=" * 100)
print("【分析2】拥堵等级分布")
print("=" * 100)

congestion_dist = spark.sql("""
    SELECT 
        congestion_level,
        COUNT(*) as record_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM traffic_view), 2) as percentage,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent,
        ROUND(AVG(go_time), 2) as avg_go_time
    FROM traffic_view
    GROUP BY congestion_level
    ORDER BY record_count DESC
""")

congestion_dist.show(truncate=False)

# 可视化数据
print("\n拥堵等级分布图:")
for row in congestion_dist.collect():
    bar_len = int(row['percentage'])
    print(f"  {row['congestion_level']:12s}: {row['percentage']:5.2f}% {'█' * bar_len}")

# ============================================
# 分析3：24小时交通模式
# ============================================
print("\n" + "=" * 100)
print("【分析3】24小时交通模式分析")
print("=" * 100)

hourly_pattern = spark.sql("""
    SELECT 
        hour,
        COUNT(*) as traffic_volume,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent,
        ROUND(AVG(go_time), 2) as avg_go_time,
        ROUND(PERCENTILE_APPROX(speed, 0.5), 2) as median_speed,
        MAX(speed) as max_speed,
        MIN(speed) as min_speed
    FROM traffic_view
    GROUP BY hour
    ORDER BY hour
""")

hourly_pattern.show(25)

# 识别高峰时段
print("\n🚗 车流量高峰时段TOP5:")
peak_volume = hourly_pattern.orderBy(col("traffic_volume").desc()).limit(5)
for row in peak_volume.collect():
    print(f"   {row['hour']:02d}:00 - 流量: {row['traffic_volume']:,} 辆")

print("\n🐌 最拥堵时段TOP5（车速最慢）:")
peak_congestion = hourly_pattern.orderBy(col("avg_spee
for row in peak_congestion.collect():
    print(f"   {row['hour']:02d}:00 - 车速: {row['avg_speed']} km/h, 指数: {row['avg_exponent']}")

# ============================================
# 分析4：星期模式分析
# ============================================
print("\n" + "=" * 100)
print("【分析4】星期模式分析")
print("=" * 100)

weekday_pattern = spark.sql("""
    SELECT 
        weekday,
        weekday_name,
        COUNT(*) as traffic_volume,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent,
        ROUND(AVG(go_time), 2) as avg_go_time,
        ROUND(SUM(CASE WHEN exponent > 8 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as heavy_congestion_rate
    FROM traffic_view
    GROUP BY weekday, weekday_name
    ORDER BY weekday
""")

weekday_pattern.show()

# ============================================
# 分析5：速度分布直方图
# ============================================
print("\n" + "=" * 100)
print("【分析5】速度分布直方图")
print("=" * 100)

speed_dist = spark.sql("""
    SELECT 
        CASE 
            WHEN speed < 10 THEN '0-10 km/h'
            WHEN speed < 20 THEN '10-20 km/h
            WHEN speed < 30 THEN '20-30 km/h'
            WHEN speed < 40 THEN '30-40 km/h'
            WHEN speed < 50 THEN '40-50 km/h'
            WHEN speed < 60 THEN '50-60 km/h'
            WHEN speed < 70 THEN '60-70 km/h'
            WHEN speed < 80 THEN '70-80 km/h'
            ELSE '>80 km/h'
        END as speed_range,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM traffic_view), 2) as percentage
    FROM traffic_view
    GROUP BY 
        CASE 
            WHEN speed < 10 THEN '0-10 km/h'
            WHEN speed < 20 THEN '10-20 km/h'
            WHEN speed < 30 THEN '20-30 km/h'
            WHEN speed < 40 THEN '30-40 km/h'
            WHEN speed < 50 THEN '40-50 km/h'
            WHEN speed < 60 THEN '50-60 km/h'
            WHEN speed < 70 THEN '60-70 km/h'
            WHEN speed < 80 THEN '70-80 km/h'
            ELSE '>80 km/h'
        END
    ORDER BY MIN(speed)
""")

spe

print("\n速度分布图:")
for row in speed_dist.collect():
    bar_len = int(row['percentage'] / 2)
    print(f"  {row['speed_range']:15s}: {row['percentage']:5.2f}% {'█' * bar_len}")

# ============================================
# 分析6：拥堵指数分布
# ============================================
print("\n" + "=" * 100)
print("【分析6】拥堵指数分布")
print("=" * 100)

exponent_dist = spark.sql("""
    SELECT 
        CASE 
            WHEN exponent <= 2 THEN '畅通(0-2)'
            WHEN exponent <= 4 THEN '基本畅通(2-4)'
            WHEN exponent <= 6 THEN '缓行(4-6)'
            WHEN exponent <= 8 THEN '较拥堵(6-8)'
            ELSE '拥堵(8-10)'
        END as exponent_range,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM traffic_view), 2) as percentage,
        ROUND(AVG(speed), 2) as avg_speed
    FROM traffic_view
    GROUP BY 
        CASE 
            WHEN exponent <= 2 THEN '畅通(0-2)'
            WHEN exponent <= 4 THEN '基本畅通(2-4)'
            WHEN exponent <= 6 THEN '缓行(4-6)'
            WHEN exponent <= 8 THEN '较拥堵(6-8)'
            ELSE '拥堵(8-10)'
        END
    ORDER BY MIN(exponent)
""")

exponent_dist.show()

# ============================================
# 分析7：街道排行（多维度）
# ============================================
print("\n" + "=" * 100)
print("【分析7】街道多维度排行")
print("=" * 100)

# 最拥堵街道TOP10
congested_blocks = spark.sql("""
    SELECT 
        block_id,
        COUNT(*) as record_count,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent,
        ROUND(AVG(go_time), 2) as avg_go_time,
        ROUND(SUM(CASE WHEN exponent > 8 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as severe_congestion_rate
    FROM traffic_view
    GROUP BY block_id
    HAVING COUNT(*) > 10000
    ORDER BY avg_exponent DESC
    LIMIT 10
""")

print("\n🔴 最拥堵街道TOP10:")
congested_blocks.show(truncate=False)

# 最繁忙街道TOP10
busy_blocks = spark.sql("""
    SELECT 
        block_id,
        COUNT(*) as record_count,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent
    FROM traffic_view
    GROUP BY block_id
    ORDER BY record_count DESC
    LIMIT 10
""")

print("\n📈 最繁忙街道TOP10:")
busy_blocks.show(truncate=False)

# ============================================
# 分析8：周末与工作日对比
# ============================================
print("\n" + "=" * 100)
print("【分析8】周末与工作日对比分析")
print("=" * 100)

weekend_compare = spark.sql("""
    SELECT 
        CASE WHEN is_weekend = TRUE THEN '周末' ELSE '工作日' END as day_type,
        COUNT(*) as total_records,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent,
        ROUND(AVG(go_time), 2) as avg_go_time,
        ROUND(AVG(CASE WHEN hour BETWEEN 7 AND 9 THEN speed END), 2) as morning_peak_speed,
        ROUND(AVG(CASE WHEN hour BETWEEN 17 AND 19 THEN speed END), 2) as evening_peak_speed,
        ROUND(SUM(CASE WHEN exponent > 8 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as heavy_congestion_rate
    FROM traffic_view
    GROUP BY is_weekend
""")

weekend_compare.show()

# 计算差异
weekend_row = weekend_compare.filter("day_type = '周末'").collect()
weekday_row = weekend_compare.filter("day_type = '工作日'").collect()
if wee
    speed_diff = ((weekend_row[0]['avg_speed'] - weekday_row[0]['avg_speed']) / weekday_row[0]['avg_speed']) * 100
    print(f"\n📊 周末 vs 工作日差异分析:")
    print(f"   车流量: 周末比工作日减少 {abs(weekend_row[0]['total_records'] - weekday_row[0]['total_records']):,.0f} 辆 ({weekend_row[0]['total_records']/weekday_row[0]['total_records']*100:.1f}%)")
    print(f"   平均车速: 周末比工作日 {speed_diff:+.1f}%")
    print(f"   严重拥堵率: 周末比工作日 {weekend_row[0]['heavy_congestion_rate'] - weekday_row[0]['heavy_congestion_rate']:+.1f}%")

# ============================================
# 分析9：相关性分析
# ============================================
print("\n" + "=" * 100)
print("【分析9】指标相关性分析")
print("=" * 100)

correlation = spark.sql("""
    SELECT 
        ROUND(CORR(speed, exponent), 4) as speed_exponent_corr,
        ROUND(CORR(speed, go_time), 4) as speed_gotime_corr,
        ROUND(CORR(exponent, go_time), 4) as exponent_gotime_corr,
        ROUND(CORR(speed, golen), 4) as speed_golen_corr
    FROM traffic_view
""")

correlation.show()

print("\n📈 相关性解读:")
corr_data = correlation.collect()[0]
print(f"   车速 vs 交通指数: {corr_data['speed_exponent_corr']:.4f} (强负相关，车速越低越拥堵)")
print(f"   车速 vs 通行时间: {corr_data['speed_gotime_corr']:.4f} (强负相关，车速慢则通行时间长)")
print(f"   交通指数 vs 通行时间: {corr_data['exponent_gotime_corr']:.4f} (强正相关)")
print(f"   车速 vs 行驶长度: {corr_data['speed_golen_corr']:.4f}")

# ============================================
# 分析10：异常检测
# ============================================
print("\n" + "=" * 100)
print("【分析10】异常检测分析")
print("=" * 100)

# 识别异常值
anomalies = spark.sql("""
    SELECT 
        COUNT(CASE WHEN speed < 10 THEN 1 END) as ultra_slow_count,
        ROUND(COUNT(CASE WHEN speed < 10 THEN 1 END) * 100.0 / COUNT(*), 4) as ultra_slow_rate,
        COUNT(CASE WHEN speed > 90 THEN 1 END) as ultra_fast_count,
        ROUND(COUNT(CASE WHEN speed > 90 THEN 1 END) * 100.0 / COUNT(*), 4) as ultra_fast_rate,
        COUNT(CASE WHEN exponent > 9 THEN 1 END) as extreme_congestion_count,
        ROUND(COUNT(CASE WHEN exponent > 9 THEN 1 END) * 100.0 / COUNT(*), 4) as extreme_congestion_rate
    FROM traffic_view
""")

anomalies.show()

# 超慢速记录详情
ultra_slow_blocks = spark.sql("""
    SELECT 
        block_id,
        COUNT(*) as slow_count,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent
    FROM traffic_view
    WHERE speed < 10
    GROUP BY block_id
    ORDER BY slow_count DESC
    LIMIT 5
""")

print("\n🐢 超慢速最多的街道:")
ultra_slow_blocks.show(truncate=False)

# ============================================
# 分析11：时间序列趋势（每日）
# ============================================
print("\n" + "=" * 100)
print("【分析11】每日时间序列趋势")
print("=" * 100)

daily_trend = spark.sql("""
    SELECT 
        dt,
        weekday_name,
        COUNT(*) as daily_volume,
        ROUND(AVG(speed), 2) as daily_avg_speed,
        ROUND(AVG(exponent), 2) as daily_avg_exponent,
        ROUND(AVG(CASE WHEN hour BETWEEN 7 AND 9 THEN speed END), 2) as morning_peak_speed,
        ROUND(AVG
    FROM traffic_view
    GROUP BY dt, weekday_name
    ORDER BY dt
    LIMIT
""")

daily_trend.show(30, truncate=False)

# ============================================
# 分析12：时段流量分布
# ============================================
print("\n" + "=" * 100)
print("【分析12】时段流量分布")
print("=" * 100)

time_slot_dist = spark.sql("""
    SELECT 
        time_slot,
        COUNT(*) as record_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM traffic_view), 2) as percentage,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent
    FROM traffic_view
    GROUP BY time_slot
    ORDER BY 
        CASE time_slot
            WHEN '早高峰' THEN 1
            WHEN '日间平峰' THEN 2
            WHEN '晚高峰' THEN 3
            WHEN '夜间' THEN 4
            WHEN '深夜' THEN 5
            ELSE 6
        END
""")

time_slot_dist.show(truncate=False)

# ============================================
# 分析13：15分钟粒度分析
# ============================================
print("\n" + "=" * 10
print("【分析13】15分钟粒度流量分析")
print("=" * 100)

quarter_hour_stats = spark.sql("""
    SELECT 
        hour,
        quarter_hour,
        CONCAT(CAST(hour AS STRING), ':', LPAD(CAST(quarter_hour*15 AS STRING), 2, '0')) as time_point,
        COUNT(*) as traffic_volume,
        ROUND(AVG(speed), 2) as avg_speed,
        ROUND(AVG(exponent), 2) as avg_exponent
    FROM traffic_view
    GROUP BY hour, quarter_hour
    ORDER BY hour, quarter_hour
    LIMIT 48
""")

quarter_hour_stats.show(48, truncate=False)

# ============================================
# 分析14：天气影响模拟（按小时流量波动）
# ============================================
print("\n" + "=" * 100)
print("【分析14】流量波动分析（标准差/均值比）")
print("=" * 100)

volatility = spark.sql("""
    SELECT 
        hour,
        ROUND(STDDEV(traffic_volume) / AVG(traffic_volume), 4) as cv_coefficient,
        ROUND(AVG(traffic_volume), 0) as avg_volume,
        ROUND(STDDEV(traffic_volume), 0) as stddev_volume
    FROM (
        SELECT 
            hour,
            dt,
            COUNT(*) as traffic_volume
        FROM traffic_view
        GROUP BY hour, dt
    ) daily_hourly
    GROUP BY hour
    ORDER BY hour
""")

volatility.show()

# ============================================
# 分析15：保存所有分析结果
# ==================
print("\n" + "=" * 100)
print("【分析15】保存分析结果")
print("=" * 100)

# 保存到HDFS
hourly_pattern.write.mode("overwrite").parquet("/user/traffic/ads/spark_hourly_pattern")
congestion_dist.write.mode("overwrite").parquet("/user/traffic/ads/spark_congestion_dist")
weekday_pattern.write.mode("overwrite").parquet("/user/traffic/ads/spark_weekday_pattern")
congested_blocks.write.mode("overwrite").parquet("/user/traffic/ads/spark_congested_blocks")
busy_blocks.write.mode("overwrite").parquet("/user/traffic/ads/spark_busy_blocks")
time_slot_dist.write.mode("overwrite").parquet("/user/traffic/ads/spark_time_slot_dist")
speed_dist.write.mode("overwrite").parquet("/user/traffic/ads/spark_speed_dist")

print("✅ 分析结果已保存到 HDFS")

# ============================================
# 分析16：生成JSON格式报表
# ============================================
print("\n" + "=" * 100)
print("【分析16】生成JSON报表")
print("=" * 100)

# 收集统计结果
summary = {
    "total_records": total_count,
    "total_blocks": spark.sql("SELECT COUNT(DISTINCT block_id) FROM traffic_view").collect()[0][0],
    "date_range": {
        "start": spark.sql("SELECT MIN(dt) FROM traffic_view").collect()[0][0],
        "end": spark.sql("SELECT MAX(dt) FROM traffic_view").collect()[0][0]
    },
    "avg_speed": basic_stats.collect()[0]['avg_speed'],
    "avg_exponent": basic_stats.collect()[0]['avg_exponent'],
    "p50_speed": basic_stats.collect()[0]['p50_speed'],
    "p95_speed": basic_stats.collect()[0]['p95_speed'],
    "congestion_distribution": [
        {"level": row['congestion_level'], "percentage": row['percentage']} 
        for row in congestion_dist.collect()
    ],
    "peak_hours": [
        {"hour": row['hour'], "volume": row['traffic_volume']} 
        for row in peak_volume.collect()
    ]
}

# 保存JSON
with open('/opt/project/traffic/output/json/summary_report.json', 'w', encoding='utf-8') as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
print("✅ JSON报表已保存: /opt/project/traffic/output/json/summary_report.json")

# ============================================
# 分析17：导出CSV格式结果
# ============================================
print("\n" + "=" * 100)
print("【分析17】导出CSV格式结果")
print("=" * 100)

# 导出小时模式
hourly_pattern_pd = hourly_pattern.limit(100).toPandas()
hourly_pattern_pd.to_csv('/opt/project/traffic/output/csv/hourly_pattern.csv', index=False)
print("✅ 小时模式已导出: hourly_pattern.csv")

# 导出拥堵分布
congestion_dist_pd = congestion_dist.toPandas()
congestion_dist_pd.to_csv('/opt/project/traffic/output/csv/congestion_distribution.csv', index=False)
print("✅ 拥堵分布已导出: congestion_distribution.csv")

# 导出街道排行
congested_blocks_pd = congested_blocks.toPandas()
congested_blocks_pd.to_csv('/opt/project/traffic/output/csv/congested_blocks.csv', index=False)
print("✅ 街道排行已导出: congested_blocks.csv")

# 导出周末对比
weekend_compare_pd = weekend_compare.toPandas()
weekend_compare_pd.to_csv('/opt/project/traffic/output/csv/weekend_compare.csv', index=False)
print("✅ 周末对比已导出: weekend_compare.csv")

# ============================================
# 最终报告
# ============================================
print("\n" + "=" * 100)
print("📊 深圳市交通数据分析 - 最终报告汇总")
print("=" * 100)

print(f"""
┌────────────────────────────────────────────────────────────────────────────────┐
│                           深圳市交通数据分析报告                                 │
├────────────────────────────────────────────────────────────────────────────────┤
│  【数据规模】                                                                    │
│    • 总记录数: {total_count:,} 条                                                │
│    • 监测街道数: {summary['total_blocks']} 个                                     │
│    • 数据时间跨度: {summary['date_range']['start']} 至 {summary['date_range']['end']}    │
├────────────────────────────────────────────────────────────────────────────────┤
│  【整体交通状况】                                                                │
│    • 平均车速: {summary['avg_speed']:.2f} km/h                                    │
│    • 平均交通指数: {summary['avg_exponent']:.2f}                                   │
│    • 中位数车速: {summary['p50_speed']:.2f} km/h                                  │
│    • 95分位车速: {summary['p95_speed']:.2f} km/h                                 │
├────────────────────────────────────────────────────────────────────────────────┤
│  【拥堵分布】                                                                    │
""" + "\n".join([f"    • {row['congestion_level']}: {row['percentage']:.2f}%" for row in summary['congestion_distribution']]) + f"""
├────────────────────────────────────────────────────────────────────────────────┤
│  【高峰时段】                                                                    │
""" + "\n".join([f"    • {row['hour']:02d}:00 - 车流量: {row['volume']:,} 辆" for row in summary['peak_hours']]) + f"""
├────────────────────────────────────────────────────────────────────────────────┤
│  【周末对比】                                                                    │
│    • 周末平均车速: {weekend_compare.filter(col('day_type')=='周末').collect()[0]['avg_speed'] if len(weekend_compare.filter(col('day_type')=='周末').collect())>0 else 0:.2f} km/h          │
│    • 工作日平均车速: {weekend_compare.filter(col('day_type')=='工作日').collect()[0]['avg_speed'] if len(weekend_compare.filter(col('day_type')=='工作日').collect())>0 else 0:.2f} km/h      │
├────────────────────────────────────────────────────────────────────────────────┤
│  【输出文件】                                                                    │
│    • JSON报表: /opt/project/traffic/output/json/summary_report.json            │
│    • CSV文件: /opt/project/traffic/output/csv/                                 │
│    • HDFS结果: /user/traffic/ads/spark_*                                       │
└────────────────────────────────────────────────────────────────────────────────┘
""")

print("\n" + "=" * 100)
print("✅ Spark深度分析完成！")
print("=" * 100)

spark.stop()
