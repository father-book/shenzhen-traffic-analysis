#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
深圳市交通数据获取脚本
"""

import requests
import time
import csv

APP_KEY = "c0512eeb0616479c9ec9ec1750a7d46c"
BASE_URL = "https://opendata.sz.gov.cn/api/29200_00403589/1/service.xhtml"

def fetch_page(page, rows=1000):
    params = {'appKey': APP_KEY, 'page': page, 'rows': rows}
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error: {e}")
    return None

def main():
    print("开始获取数据...")
    first = fetch_page(1, 1)
    if not first:
        print("获取失败")
        return
    
    total = int(first.get('total', 0))
    print(f"总数据量: {total:,} 条")
    
    with open('traffic_data.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['block_id', 'speed', 'golen', 'time_stamp', 'period', 'exponent', 'go_time'])
    
    rows_per_page = 1000
    total_pages = (total + rows_per_page - 1) // rows_per_page
    all_count = 0
    
    for page in range(1, total_pages + 1):
        data = fetch_page(page, rows_per_page)
        if data and 'data' in data:
            with open('traffic_data.csv', 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for item in data['data']:
                    writer.writerow([
                        item.get('BLOCKID', ''),
                        item.get('SPEED', ''),
                        item.get('GOLEN', ''),
                        item.get('TIME1', ''),
                        item.get('PERIOD', ''),
                        item.get('EXPONENT', ''),
                        item.get('GOTIME', '')
                    ])
            all_count += len(data['data'])
            if page % 10 == 0:
                print(f"进度: {page}/{total_pages} 页 | {all_count:,} 条")
        time.sleep(0.3)
    
    print(f"完成！共 {all_count:,} 条")

if __name__ == "__main__":
    main()