from typing import Any, Dict, List
import requests
import json
import pandas as pd
import os

import tabulate


url = "https://api.baidu.com/json/sms/service/OpenApiReportService/getReportData"

# 构造请求数据
payload = json.dumps({
    "body": 
    {
        "reportType": 2307838,
        "startDate": "2025-01-02",
        "endDate": "2025-01-02",
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
        "startRow": 0,
        "rowCount": 200,
        "needSum": False
    },
    "header": {
        "userName": "武汉夜莺科技",
        "accessToken": "eyJhbGciOiJIUzM4NCJ9.eyJzdWIiOiJhY2MiLCJhdWQiOiLlo7nkvLTokKXplIDliqnmiYsiLCJ1aWQiOjMzMjAwMDE2LCJhcHBJZCI6ImU4MjZmOWJhMmQ4OGNmZjhhYzUzYWZlZDg1ZjVkNTdhIiwiaXNzIjoi5ZWG5Lia5byA5Y-R6ICF5Lit5b-DIiwicGxhdGZvcm1JZCI6IjQ5NjAzNDU5NjU5NTg1NjE3OTQiLCJleHAiOjE3NDA2NzU2MDEsImp0aSI6Ii05MDQwMDQ3NjQ1NjQ1ODk3NzI1In0.vSv3VrtXvWVNgKMzz1S_yPOUZZ9Iowbgoy43an80gW2bAhA1wAavwUS6o3zaJXQ3"
    }
})
headers = {
    'Content-Type': 'application/json;charset=UTF-8'
}

        # 发送请求
response = requests.request("POST", url, headers=headers, data=payload)
data = response.json()
rows_data: List[Dict[str, Any]] = data["body"]["data"][0]["rows"]
# print(rows_data)
total_row_count = data["body"]["data"][0]["totalRowCount"]
print(total_row_count)
current_page_row_count = len(rows_data)
print(current_page_row_count)
insert_data: List[Dict[str, Any]] = []
for row in rows_data:
    # print(row)
    gx_baidu_query_word_report = {
    "date": row["date"],
    "campaignNameStatus": row["campaignNameStatus"],
    "campaignId": row["campaignId"],
    "adGroupNameStatus": row["adGroupNameStatus"],
    "adGroupId": row["adGroupId"],
    "ideaInfo": row["ideaInfo"],
    "wInfoNameStatus": row["wInfoNameStatus"],
    "wInfoId": row["wInfoId"],
    "queryWord": row["queryWord"],
    "queryStatusName": row["queryStatusName"],
    "wMatchId": row["wMatchId"],
    "impression": row["impression"],
    "click": row["click"],
    "ocpcConversionsDetail3": row["ocpcConversionsDetail3"],
    "ocpcConversionsDetail3Cost": row.get("ocpcConversionsDetail3Cost", None),
    "cost": row["cost"],
    "ctr": row["ctr"],
    "cpc": row["cpc"],
    "topPageViews": row["topPageViews"],
    "ocpcConversionsDetail18": row["ocpcConversionsDetail18"],
    "ocpcConversionsDetail18Cost": row.get("ocpcConversionsDetail18Cost", None),
}
    insert_data.append(gx_baidu_query_word_report)
df=pd.DataFrame(gx_baidu_query_word_report)
print(df.shape)



