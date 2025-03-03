import os
import requests
import json
import pandas as pd
import datetime
from typing import Any, Dict, List



def gettoken():
        url="http://10.172.43.161:9080/internal/api/baidu_ad_access_token/7773286589768597505"
        all_rows_data: List[Dict[str, Any]] = []
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            print("\n=== 美化JSON ===")
            data=response.json()
            # pprint(response.json())
            

        except Exception as e:
            print(f"发生错误：{str(e)}")
        my_list=[]
        for key in data:
            my=[data[key]]
            my_list.extend(my)
        ky=my_list[3]['access_token']
        return ky









def get_search():



    # API 请求的基础信息
    now=datetime.datetime.now()
    yesterday = now - datetime.timedelta(days = 1)
    yesterday=yesterday.strftime("%Y-%m-%d")
    url = "https://api.baidu.com/json/sms/service/OpenApiReportService/getReportData"



    headers = {
        'Content-Type': 'application/json;charset=UTF-8'
    }

    # 构造请求数据的基础模板
    payload_template = {
        "body": {
            "reportType": 2307838,
            "startDate": yesterday,
            "endDate": yesterday,
            "timeUnit": "DAY",
            "columns": [
                "date",
                "campaignNameStatus",
                "adGroupNameStatus",
                "ideaInfo",
                "wInfoNameStatus",
                "queryWord",
                "queryStatusName",
                "wMatchId",
                "impression",
                "click",
                "ocpcConversionsDetail3",
                "ocpcConversionsDetail3Cost",
                "cost",
                "ctr",
                "cpc",
                "topPageViews",
                "ocpcConversionsDetail18",
                "ocpcConversionsDetail18Cost",
            ],
            "sorts": [],
            "filters": [],
            "startRow": 0,  # 起始行
            "rowCount": 200,  # 每次请求的行数
            "needSum": False
        },
        "header": {
            "userName": "武汉夜莺科技",
            "accessToken": gettoken()
        }
    }

    # 初始化存储所有行数据的列表
    all_rows_data: List[Dict[str, Any]] = []

    # 分页请求
    total_row_count = 1  # 初始化为大于 0 的值｜请求回来的总行数
    current_start_row = 0 # 当前的行数
    row_count_per_page = 200 #累计行数

    # 当前行数小于总行数时，就将循环运行
    while current_start_row < total_row_count:
        # 更新分页参数
        payload_template["body"]["startRow"] = current_start_row
        payload_template["body"]["rowCount"] = row_count_per_page

        # 发送请求
        response = requests.post(url, headers=headers, data=json.dumps(payload_template))
        data = response.json()

        # 提取返回的总行数（仅第一次请求时获取）
        # 当当前提取返回的总行数等于0时，
        if current_start_row == 0:
            total_row_count = data["body"]["data"][0]["totalRowCount"]
            print(f"总行数: {total_row_count}")

        # 提取当前页的数据
        rows_data = data["body"]["data"][0]["rows"]
        all_rows_data.extend(rows_data)  # 合并数据

        # 更新起始行数，准备请求下一页
        current_start_row += row_count_per_page

    # 打印总数据量，确认是否获取完整
    print(f"获取的总数据量: {len(all_rows_data)}")

    # 将所有数据转换为 DataFrame
    df = pd.DataFrame(all_rows_data)

    # 打印 DataFrame 的形状和前几行
    print(df.shape)
    print(df.head())
    # 导出到 Excel 文件
    output_file = "report_data.xlsx"

    # 检查文件是否已存在
    if os.path.exists(output_file):
        print(f"文件 {output_file} 已存在，将覆盖此文件")

    df.to_excel(output_file, index=False)
    print(f"数据已成功导出到 {output_file}")


get_search()
