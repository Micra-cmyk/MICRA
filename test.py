# import requests 
# url="http://10.172.43.161:9080/internal/api/baidu_ad_access_token/7773286589768597505"



# # 基础形式
# requests.get('http://10.172.43.161:9080/internal/api/baidu_ad_access_token/7773286589768597505')
# print(requests.text)
# # 明确设置空请求体
# requests.post('https://api.example.com', data=None)

# # 带请求头但无实体内容
# requests.delete(
#     'https://api.example.com/resource/123',
#     headers={'Authorization': 'Bearer xyz'}
# )

from typing import Any, Dict, List
import requests
from pprint import pprint

def get_token():
   import datetime
import json
import warnings

import pandas
import pymysql
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

# 定义昨天的时间 来筛选昨日数据
group_chatid = ""

# 取当月和上月对比数据
sql = """
select
a.ta1,
a.合计绝对收益a,
(@bsum := @bsum + a.合计绝对收益a) AS a_pay,
a.合计安装数a,
(@csum := @csum + a.合计安装数a) AS a_num,
a.ta,
a.合计绝对收益,
(@dsum := @dsum + a.合计绝对收益) AS b_pay,
a.合计安装数,
(@esum := @esum + a.合计安装数) AS b_num
from
(
select
a.ta1,
a.合计绝对收益a,
a.合计安装数a,
b.ta,
b.合计绝对收益,
b.合计安装数
from
(
select
    a.ta as ta1,
    (a.360_income + a.baidu_income + a.sougou_income + a.360_web_income + a.baidu_web_income) as 合计绝对收益a,
    (a.360_num+a.sougou_num+a.baidu_num) as 合计安装数a
    from 
    (
    select
    a.ta,
    a.num as 360_num,
    a.cost as 360_cost,
    ifnull(a.pay, 0) as 360_pay,
    ifnull(a.pay, 0) - a.cost as 360_income,
    b.num as baidu_num,
    b.cost as baidu_cost,
    ifnull(b.pay, 0) as baidu_pay,
    ifnull(b.pay, 0) - b.cost as baidu_income,
    c.num as sougou_num,
    ifnull(c.cost,0) as sougou_cost,
    ifnull(c.pay,0) as sougou_pay,
    ifnull(c.pay,0) - ifnull(c.cost,0) as sougou_income,
    ifnull(d.cost,0) as 360_web_cost,
    ifnull(d.pay,0) as 360_web_pay,
    ifnull(d.pay,0) - ifnull(d.cost,0) as 360_web_income,
    ifnull(e.cost,0) as baidu_web_cost,
    ifnull(e.pay,0) as baidu_web_pay,
    ifnull(e.pay,0)- ifnull(e.cost,0) as baidu_web_income
    from
    -- ----------------------------------------------
    -- 360插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    date_format(created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(created_at) != current_date
    and extension_installed = 1
    and register_channel like '0214%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    from
    `360_sem_campaign_daily`
    where date_format(date,'%y-%m') = date_format(now(),'%y-%m')
    group by tb
    order by tb desc ) as b
    on a.ta =b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user` join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like '0214%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(new_order.created_at) != date(current_date)
    group by tc
    ) as c
    on b.tb= c.tc
    ) as a
    -- ----------------------------------------------
    -- 百度插件花销付费
    -- ----------------------------------------------
    left join 
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    date(created_at) != current_date
    and date_format(created_at,'%y-%m') = date_format(now(),'%y-%m')
    and extension_installed = 1
    and register_channel like 'baidu%'
    and `register_channel` not like'baidu_xmt%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(`baidu_sem_report`.report_time) as tb,
    ROUND(sum(cost),0) as cost
    from
    `baidu_sem_report`
    WHERE report_id=27461652 
    group by tb
    ) as b
    on a.ta = b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user` join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like 'baidu%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(new_order.created_at) != date(current_date)
    group by tc
    ) as c
    on  b.tb = c.tc
    ) as b
    on a.ta = b.ta
    left join 
    -- ----------------------------------------------
    -- 搜狗插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.num, 0) as num,
    ifnull(c.cost, 0) as cost,
    ifnull(d.pay, 0) as pay
    from
    (
    select
    date(`user`.created_at) as ta,
    count(id) 
    from `user` 
    where date_format(created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(created_at) != current_date
    group by ta 
    )as a 
    left join 
    (
    select
    date(user.created_at) as tb,
    count(1) as num
    from 
    `user`
    where register_channel like '%so%'
    and extension_installed = 1 
    and date_format(created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(created_at) != current_date
    group by tb
    ) as b 
    on a.ta =b.tb 
    left join 
    (
    select
    date as tc,
    ROUND(sum(cost),0) as cost
    from 
    `sougou_sem_campaign_report`
    group by tc) as c 
    on a.ta =c.tc
    left join 
    (
    select
    date(new_order.created_at) as td,
    ROUND(sum(new_order.price/100),0) as pay
    from (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order join `user`
    on new_order.user_id =`user`.id
    where paid=1
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    and user.register_channel like 'so%'
    group by td
    ) as d 
    on a.ta = d.td) as c
    on b.ta = c.ta
    left join 
    -- ----------------------------------------------
    -- baidu  web 插件花销付费
    -- ----------------------------------------------

    (
    SELECT
    a.ta,
    a.cost,
    b.pay
    from
    (
    SELECT
    date(report_time) as ta,
    ROUND(sum(cost),0) as cost
    FROM
    `baidu_web_edit_sem_report`
    where report_type ='2'
    -- and date(created_at) != date(current_date)
    and date_format(report_time,'%y-%m') = date_format(now(),'%y-%m')
    group by ta) AS a
    left join 
    (
    SELECT
    date( new_order.created_at ) AS tb,
    ROUND( SUM( price )/ 100 ) AS pay  
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    INNER JOIN `user` 
    WHERE
    `user`.`id` = new_order.`user_id` 
    AND new_order.`paid` = 1 
    and (register_channel like 'web_editor:baidu%'
    or register_channel like '%baidu_xmt%'
    or register_channel like '%xmt_bd%')
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(new_order.created_at) != date(current_date)
    GROUP BY tb 
    ) as b
    on a.ta = b.tb ) as e
    on c.ta = e.ta
    left join 
    -- ----------------------------------------------
    -- 360 web 插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(a.cost, 0) as cost,
    ifnull(b.pay, 0) as pay
    from
    (select
    date(date) as ta,
    ROUND(sum(total_cost),0) as cost
    FROM
    `360_sem_campaign_daily_wbio`
    where  date_format(date,'%y-%m') = date_format(now(),'%y-%m')
    -- date(created_at) != date(current_date)
    and (
    campaign_id =1791046127  -- 品牌词
    or campaign_id =1774274543  -- 产品词
    or campaign_id =1740757047   -- 公众号组
    or campaign_id=2361520432    -- 新媒体
    --          campaign_id = 1354879796 -- 365 
    --          or campaign_id = 1421935668 -- 秀米 
    --          or campaign_id = 1421941812 -- 135
    --          or campaign_id = 1421993012  -- 96
    --          or campaign_id = 1472315956  -- 小蚂蚁
    --          or campaign_id = 3301000759  -- 新榜
    --          or campaign_id = 4156630838  -- i排版
    )
    group by ta) as a
    left join 
    (
    SELECT
    date(new_order.created_at) AS tb,
    ROUND(SUM( price )/100,0)  AS pay
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    INNER JOIN `user` 
    WHERE
    `user`.`id` = new_order.`user_id` 
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    AND new_order.`paid` = 1 
    AND (`user`.register_channel like 'web_editor:360%'
    or `user`.register_channel like 'web_editor:1201%'
    or `user`.register_channel like 'web_editor:1130%'
    or `user`.register_channel like 'xmt%'
    or `user`.register_channel like '%gzh%')
    and `user`.register_channel not like '%baidu%'
    and `user`.register_channel not like '%so%'
    and `user`.register_channel not like '%0214%'
    and `user`.register_channel not like '%xmt_bd%'
    and date(new_order.created_at) != date(current_date)
    and  new_order.user_id not in (11,51276,17851815,61735143,16)
    GROUP BY tb
    ) as b
    on a.ta =b.tb) as d
    on e.ta =d.ta
    )as a
) as a
left join 
(
select
    a.ta,
    (a.360_income + a.baidu_income + a.sougou_income + a.360_web_income + a.baidu_web_income) as 合计绝对收益,
    (a.360_num+a.sougou_num+a.baidu_num) as 合计安装数
    from 
    (
    select
    a.ta,
    a.num as 360_num,
    a.cost as 360_cost,
    ifnull(a.pay, 0) as 360_pay,
    ifnull(a.pay, 0) - a.cost as 360_income,
    b.num as baidu_num,
    b.cost as baidu_cost,
    ifnull(b.pay, 0) as baidu_pay,
    ifnull(b.pay, 0) - b.cost as baidu_income,
    c.num as sougou_num,
    ifnull(c.cost,0) as sougou_cost,
    ifnull(c.pay,0) as sougou_pay,
    ifnull(c.pay,0) - ifnull(c.cost,0) as sougou_income,
    ifnull(d.cost,0) as 360_web_cost,
    ifnull(d.pay,0) as 360_web_pay,
    ifnull(d.pay,0) - ifnull(d.cost,0) as 360_web_income,
    ifnull(e.cost,0) as baidu_web_cost,
    ifnull(e.pay,0) as baidu_web_pay,
    ifnull(e.pay,0)- ifnull(e.cost,0) as baidu_web_income
    from
    -- ----------------------------------------------
    -- 360插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    and extension_installed = 1
    and register_channel like '0214%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    from
    `360_sem_campaign_daily`
    where PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(date,'%y%m') ) =1
    group by tb
    order by tb desc ) as b
    on a.ta =b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user` join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like '0214%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and
    PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    group by tc
    ) as c
    on b.tb= c.tc
    ) as a
    -- ----------------------------------------------
    -- 百度插件花销付费
    -- ----------------------------------------------
    left join 
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    date(created_at) != current_date
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    and extension_installed = 1
    and register_channel like 'baidu%'
    and `register_channel` not like'baidu_xmt%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(`baidu_sem_report`.report_time) as tb,
    ROUND(sum(cost),0) as cost
    from
    `baidu_sem_report`
    WHERE report_id=27461652 
    group by tb
    ) as b
    on a.ta = b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user` join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like 'baidu%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    group by tc
    ) as c
    on  b.tb = c.tc
    ) as b
    on a.ta = b.ta
    left join 
    -- ----------------------------------------------
    -- 搜狗插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.num,0)as num,
    ifnull(c.cost,0)as cost,
    ifnull(d.pay,0)as pay
    from
    (
    select
    date(user.created_at) as ta,
    count(1) as num
    from 
    `user`
    where PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    group by ta
    ) as a 
    left join 
    (
    select
    date(user.created_at) as tb,
    count(1) as num
    from `user`
    where register_channel like '%so%'
    and extension_installed = 1 
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    group by tb
    ) as b 
    on a.ta =b.tb 
    left join 
    (
    select
    date as tc,
    ROUND(sum(cost),0) as cost
    from 
    `sougou_sem_campaign_report`
    group by tc
    ) as c 
    on a.ta =c.tc
    left join 
    (
    select
    date(new_order.created_at) as td,
    ROUND(sum(new_order.price/100),0) as pay
    from (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    join `user`
    on new_order.user_id =`user`.id
    where paid=1
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    and user.register_channel like 'so%'
    group by td
    ) as d 
    on a.ta = d.td
    ) as c
    on b.ta = c.ta
    left join 
    -- ----------------------------------------------
    -- baidu  web 插件花销付费
    -- ----------------------------------------------

    (
    SELECT
    a.ta,
    a.cost,
    b.pay
    from
    (
    SELECT
    date(report_time) as ta,
    ROUND(sum(cost),0) as cost
    FROM
    `baidu_web_edit_sem_report`
    where report_type ='2'
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(report_time,'%y%m') ) =1
    group by ta) AS a
    left join 
    (
    SELECT
    date( new_order.created_at ) AS tb,
    ROUND( SUM( price )/ 100 ) AS pay  
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    INNER JOIN `user` 
    WHERE
    `user`.`id` = new_order.`user_id` 
    AND new_order.`paid` = 1 
    and (register_channel like 'web_editor:baidu%'
    or register_channel like '%baidu_xmt%'
    or register_channel like '%xmt_bd%')
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    GROUP BY tb 
    ) as b
    on a.ta = b.tb ) as e
    on c.ta = e.ta
    left join 
    -- ----------------------------------------------
    -- 360 web 插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.cost,0)as cost,
    ifnull(c.pay,0)as pay 
    from
    (
    select
    date(created_at)as ta,
    count(id)
    from `user`
    where  PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1 
    group by ta 
    )as a 
    left join 
    (
    select
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    FROM `360_sem_campaign_daily_wbio`
    where  PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(date,'%y%m') ) =1
    and (
    campaign_id =1791046127  -- 品牌词
    or campaign_id =1774274543  -- 产品词
    or campaign_id =1740757047   -- 公众号组
    or campaign_id=2361520432    -- 新媒体
    --          campaign_id = 1354879796 -- 365 
    --          or campaign_id = 1421935668 -- 秀米 
    --          or campaign_id = 1421941812 -- 135
    --          or campaign_id = 1421993012  -- 96
    --          or campaign_id = 1472315956  -- 小蚂蚁
    --          or campaign_id = 3301000759  -- 新榜
    --          or campaign_id = 4156630838  -- i排版
    )
    group by tb
    ) as b 
    on a.ta=b.tb 
    left join 
    (
    SELECT
    date(new_order.created_at) AS tc,
    ROUND(SUM( price )/100,0)  AS pay
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order INNER JOIN `user` 
    WHERE `user`.`id` = new_order.`user_id` 
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    AND new_order.`paid` = 1 
    AND (`user`.register_channel like 'web_editor:360%'
    or `user`.register_channel like 'web_editor:1201%'
    or `user`.register_channel like 'web_editor:1130%'
    or `user`.register_channel like 'xmt%'
    or `user`.register_channel like '%gzh%')
    and `user`.register_channel not like '%baidu%'
    and `user`.register_channel not like '%so%'
    and `user`.register_channel not like '%0214%'
    and `user`.register_channel not like '%xmt_bd%'
    and  new_order.user_id not in (11,51276,17851815,61735143,16)
    GROUP BY tc
    ) as c
    on a.ta =c.tc
    ) as d
    on e.ta =d.ta
    )as a
) as b
on a.ta1 = date_add(b.ta,interval 1 month) ) as a,
    (select @bsum:=0) as apay,
    (select @csum:=0) as anum,
    (select @dsum:=0) as bpay,
    (select @esum:=0) as bnum
"""
# 取上月和上上月对比数据
sqla = """
select
a.ta1,
a.合计绝对收益a,
(@bsum := @bsum + a.合计绝对收益a) AS a_pay,
a.合计安装数a,
(@csum := @csum + a.合计安装数a) AS a_num,
a.ta,
a.合计绝对收益,
(@dsum := @dsum + a.合计绝对收益) AS b_pay,
a.合计安装数,
(@esum := @esum + a.合计安装数) AS b_num
from
(
select
a.ta1,
a.合计绝对收益a,
a.合计安装数a,
b.ta,
b.合计绝对收益,
b.合计安装数
from
(
select
    a.ta as ta1,
    (a.360_income + a.baidu_income + a.sougou_income + a.360_web_income + a.baidu_web_income) as 合计绝对收益a,
    (a.360_num+a.sougou_num+a.baidu_num) as 合计安装数a
    from 
    (
    select
    a.ta,
    a.num as 360_num,
    a.cost as 360_cost,
    ifnull(a.pay, 0) as 360_pay,
    ifnull(a.pay, 0) - a.cost as 360_income,
    b.num as baidu_num,
    b.cost as baidu_cost,
    ifnull(b.pay, 0) as baidu_pay,
    ifnull(b.pay, 0) - b.cost as baidu_income,
    c.num as sougou_num,
    ifnull(c.cost,0) as sougou_cost,
    ifnull(c.pay,0) as sougou_pay,
    ifnull(c.pay,0) - ifnull(c.cost,0) as sougou_income,
    ifnull(d.cost,0) as 360_web_cost,
    ifnull(d.pay,0) as 360_web_pay,
    ifnull(d.pay,0) - ifnull(d.cost,0) as 360_web_income,
    ifnull(e.cost,0) as baidu_web_cost,
    ifnull(e.pay,0) as baidu_web_pay,
    ifnull(e.pay,0)- ifnull(e.cost,0) as baidu_web_income
    from
    -- ----------------------------------------------
    -- 360插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    and date(created_at) != current_date
    and extension_installed = 1
    and register_channel like '0214%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    from
    `360_sem_campaign_daily`
    where PERIOD_DIFF( date_format( now( )-1 , '%Y%m' ) , date_format(date,'%y%m') ) =1
    group by tb
    order by tb desc ) as b
    on a.ta =b.tb
    left join 
    (
     select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user` join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like '0214%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and PERIOD_DIFF( date_format( now( ), '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    group by tc
    ) as c
    on b.tb= c.tc
    ) as a
    -- ----------------------------------------------
    -- 百度插件花销付费
    -- ----------------------------------------------
    left join 
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    date(created_at) != current_date
     and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(`user`.created_at,'%y%m') ) =1
    and extension_installed = 1
    and register_channel like 'baidu%'
    and `register_channel` not like'baidu_xmt%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(`baidu_sem_report`.report_time) as tb,
    ROUND(sum(cost),0) as cost
    from
    `baidu_sem_report`
    WHERE report_id=27461652 
    group by tb
    ) as b
    on a.ta = b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user`join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like 'baidu%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    and date(new_order.created_at) != date(current_date)
    group by tc
    ) as c
    on  b.tb = c.tc
    ) as b
    on a.ta = b.ta
    left join 
    -- ----------------------------------------------
    -- 搜狗插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.num,0)as num,
    ifnull(c.cost,0)as cost,
    ifnull(d.pay,0)as pay
    from
    (
    select
    date(user.created_at) as ta,
    count(1) as num
    from 
    `user`
    where PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    group by ta
    ) as a 
    left join 
    (
    select
    date(user.created_at) as tb,
    count(1) as num
    from `user`
    where register_channel like '%so%'
    and extension_installed = 1 
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    group by tb
    ) as b 
    on a.ta =b.tb 
    left join 
    (
    select
    date as tc,
    ROUND(sum(cost),0) as cost
    from 
    `sougou_sem_campaign_report`
    group by tc
    ) as c 
    on a.ta =c.tc
    left join 
    (
    select
    date(new_order.created_at) as td,
    ROUND(sum(new_order.price/100),0) as pay
    from (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order join `user`
    on new_order.user_id =`user`.id
    where paid=1
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    and user.register_channel like 'so%'
    group by td
    ) as d 
    on a.ta = d.td
    ) as c
    on b.ta = c.ta
    left join 
    -- ----------------------------------------------
    -- baidu  web 插件花销付费
    -- ----------------------------------------------

    (
    SELECT
    a.ta,
    a.cost,
    b.pay
    from
    (
    SELECT
    date(report_time) as ta,
    ROUND(sum(cost),0) as cost
    FROM
    `baidu_web_edit_sem_report`
    where report_type ='2'
    -- and date(created_at) != date(current_date)
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(report_time,'%y%m') ) =1
    group by ta) AS a
    left join 
    (
    SELECT
    date( new_order.created_at ) AS tb,
    ROUND( SUM( price )/ 100 ) AS pay  
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    INNER JOIN `user` 
    WHERE
    `user`.`id` = new_order.`user_id` 
    AND new_order.`paid` = 1 
    and (register_channel like 'web_editor:baidu%'
    or register_channel like '%baidu_xmt%'
    or register_channel like '%xmt_bd%')
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    and date(new_order.created_at) != date(current_date)
    GROUP BY tb 
    ) as b
    on a.ta = b.tb ) as e
    on c.ta = e.ta
    left join 
    -- ----------------------------------------------
    -- 360 web 插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.cost,0)as cost,
    ifnull(c.pay,0)as pay 
    from
    (
    select
    date(created_at)as ta,
    count(id)
    from `user`
    where  PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1 
    group by ta 
    )as a 
    left join 
    (
    select
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    FROM `360_sem_campaign_daily_wbio`
    where  PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(date,'%y%m') ) =1
    and (
    campaign_id =1791046127  -- 品牌词
    or campaign_id =1774274543  -- 产品词
    or campaign_id =1740757047   -- 公众号组
    or campaign_id=2361520432    -- 新媒体
    --          campaign_id = 1354879796 -- 365 
    --          or campaign_id = 1421935668 -- 秀米 
    --          or campaign_id = 1421941812 -- 135
    --          or campaign_id = 1421993012  -- 96
    --          or campaign_id = 1472315956  -- 小蚂蚁
    --          or campaign_id = 3301000759  -- 新榜
    --          or campaign_id = 4156630838  -- i排版
    )
    group by tb
    ) as b 
    on a.ta=b.tb 
    left join 
    (
    SELECT
    date(new_order.created_at) AS tc,
    ROUND(SUM( price )/100,0)  AS pay
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order INNER JOIN `user` 
    WHERE `user`.`id` = new_order.`user_id` 
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    AND new_order.`paid` = 1 
    AND (`user`.register_channel like 'web_editor:360%'
    or `user`.register_channel like 'web_editor:1201%'
    or `user`.register_channel like 'web_editor:1130%'
    or `user`.register_channel like 'xmt%'
    or `user`.register_channel like '%gzh%')
    and `user`.register_channel not like '%baidu%'
    and `user`.register_channel not like '%so%'
    and `user`.register_channel not like '%0214%'
    and `user`.register_channel not like '%xmt_bd%'
    and  new_order.user_id not in (11,51276,17851815,61735143,16)
    GROUP BY tc
    ) as c
    on a.ta =c.tc
    ) as d
    on e.ta =d.ta
    )as a
) as a
left join 
(
select
    a.ta,
    (a.360_income + a.baidu_income + a.sougou_income + a.360_web_income + a.baidu_web_income) as 合计绝对收益,
    (a.360_num+a.sougou_num+a.baidu_num) as 合计安装数
    from 
    (
    select
    a.ta,
    a.num as 360_num,
    a.cost as 360_cost,
    ifnull(a.pay, 0) as 360_pay,
    ifnull(a.pay, 0) - a.cost as 360_income,
    b.num as baidu_num,
    b.cost as baidu_cost,
    ifnull(b.pay, 0) as baidu_pay,
    ifnull(b.pay, 0) - b.cost as baidu_income,
    c.num as sougou_num,
    ifnull(c.cost,0) as sougou_cost,
    ifnull(c.pay,0) as sougou_pay,
    ifnull(c.pay,0) - ifnull(c.cost,0) as sougou_income,
    ifnull(d.cost,0) as 360_web_cost,
    ifnull(d.pay,0) as 360_web_pay,
    ifnull(d.pay,0) - ifnull(d.cost,0) as 360_web_income,
    ifnull(e.cost,0) as baidu_web_cost,
    ifnull(e.pay,0) as baidu_web_pay,
    ifnull(e.pay,0)- ifnull(e.cost,0) as baidu_web_income
    from
    -- ----------------------------------------------
    -- 360插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =2
    and extension_installed = 1
    and register_channel like '0214%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    from
    `360_sem_campaign_daily`
    where PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(date,'%y%m') ) =2
    group by tb
    order by tb desc ) as b
    on a.ta =b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user` join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like '0214%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and PERIOD_DIFF( date_format( now( ), '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =2
    group by tc
    ) as c
    on b.tb= c.tc
    ) as a
    -- ----------------------------------------------
    -- 百度插件花销付费
    -- ----------------------------------------------
    left join 
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    date(created_at) != current_date
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =2
    and extension_installed = 1
    and register_channel like 'baidu%'
    and `register_channel` not like'baidu_xmt%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(`baidu_sem_report`.report_time) as tb,
    ROUND(sum(cost),0) as cost
    from
    `baidu_sem_report`
    WHERE report_id=27461652 
    group by tb
    ) as b
    on a.ta = b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user` join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like 'baidu%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =2
    group by tc
    ) as c
    on  b.tb = c.tc
    ) as b
    on a.ta = b.ta
    left join 
    -- ----------------------------------------------
    -- 搜狗插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.num,0)as num,
    ifnull(c.cost,0)as cost,
    ifnull(d.pay,0)as pay
    from
    (
    select
    date(user.created_at) as ta,
    count(1) as num
    from 
    `user`
    where PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =2
    group by ta
    ) as a 
    left join 
    (
    select
    date(user.created_at) as tb,
    count(1) as num
    from `user`
    where register_channel like '%so%'
    and extension_installed = 1 
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =2
    group by tb
    ) as b 
    on a.ta =b.tb 
    left join 
    (
    select
    date as tc,
    ROUND(sum(cost),0) as cost
    from 
    `sougou_sem_campaign_report`
    group by tc
    ) as c 
    on a.ta =c.tc
    left join 
    (
    select
    date(new_order.created_at) as td,
    ROUND(sum(new_order.price/100),0) as pay
    from (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order join `user`
    on new_order.user_id =`user`.id
    where paid=1
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =2
    and user.register_channel like 'so%'
    group by td
    ) as d 
    on a.ta = d.td
    ) as c
    on b.ta = c.ta
    left join 
    -- ----------------------------------------------
    -- baidu  web 插件花销付费
    -- ----------------------------------------------

    (
    SELECT
    a.ta,
    a.cost,
    b.pay
    from
    (
    SELECT
    date(report_time) as ta,
    ROUND(sum(cost),0) as cost
    FROM
    `baidu_web_edit_sem_report`
    where report_type ='2'
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(report_time,'%y%m') ) =2
    group by ta) AS a
    left join 
    (
    SELECT
    date( new_order.created_at ) AS tb,
    ROUND( SUM( price )/ 100 ) AS pay  
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    INNER JOIN `user` 
    WHERE
    `user`.`id` = new_order.`user_id` 
    AND new_order.`paid` = 1 
    and (register_channel like 'web_editor:baidu%'
    or register_channel like '%baidu_xmt%'
    or register_channel like '%xmt_bd%')
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =2361520432
    GROUP BY tb 
    ) as b
    on a.ta = b.tb ) as e
    on c.ta = e.ta
    left join 
    -- ----------------------------------------------
    -- 360 web 插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.cost,0)as cost,
    ifnull(c.pay,0)as pay 
    from
    (
    select
    date(created_at)as ta,
    count(id)
    from `user`
    where  PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =2
    group by ta 
    )as a 
    left join 
    (
    select
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    FROM `360_sem_campaign_daily_wbio`
    where  PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(date,'%y%m') ) =2
    and (
    campaign_id =1791046127  -- 品牌词
    or campaign_id =1774274543  -- 产品词
    or campaign_id =1740757047   -- 公众号组
    or campaign_id=2361520432    -- 新媒体
    --          campaign_id = 1354879796 -- 365 
    --          or campaign_id = 1421935668 -- 秀米 
    --          or campaign_id = 1421941812 -- 135
    --          or campaign_id = 1421993012  -- 96
    --          or campaign_id = 1472315956  -- 小蚂蚁
    --          or campaign_id = 3301000759  -- 新榜
    --          or campaign_id = 4156630838  -- i排版
    )
    group by tb
    ) as b 
    on a.ta=b.tb 
    left join 
    (
    SELECT
    date(new_order.created_at) AS tc,
    ROUND(SUM( price )/100,0)  AS pay
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order INNER JOIN `user` 
    WHERE `user`.`id` = new_order.`user_id` 
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =2
    AND new_order.`paid` = 1 
    AND (`user`.register_channel like 'web_editor:360%'
    or `user`.register_channel like 'web_editor:1201%'
    or `user`.register_channel like 'web_editor:1130%'
    or `user`.register_channel like 'xmt%'
    or `user`.register_channel like '%gzh%')
    and `user`.register_channel not like '%baidu%'
    and `user`.register_channel not like '%so%'
    and `user`.register_channel not like '%0214%'
    and `user`.register_channel not like '%xmt_bd%'
    and  new_order.user_id not in (11,51276,17851815,61735143,16)
    GROUP BY tc
    ) as c
    on a.ta =c.tc
    ) as d
    on e.ta =d.ta
    )as a
) as b
on a.ta1 = date_add(b.ta,interval 1 month) ) as a,
    (select @bsum:=0) as apay,
    (select @csum:=0) as anum,
    (select @dsum:=0) as bpay,
    (select @esum:=0) as bnum
"""


#  取当月每天数据
sql1 = """
    select
    a.ta,
    (a.360_income + a.baidu_income + a.sougou_income + a.360_web_income + a.baidu_web_income) as 合计绝对收益,
    (ifnull(a.360_num, 0)+ifnull(a.sougou_num, 0)+ifnull(a.baidu_num, 0)) as 合计安装数,
    (a.360_cost+a.baidu_cost+a.sougou_cost+a.360_web_cost+a.baidu_web_cost) as 合计花费,
    (a.360_pay+a.baidu_pay+a.sougou_pay+a.360_web_pay+a.baidu_web_pay) as 合计付费,
    a.360_num as 360安装,
    a.360_cost as 360花费,
    a.360_pay as 360付费,
    a.360_income 360绝对收益,
    a.baidu_num 百度安装,
    a.baidu_cost 百度花费,
    a.baidu_pay 百度付费,
    a.baidu_income 百度绝对收益,
    a.sougou_num 搜狗安装,
    a.sougou_cost 搜狗花费,
    a.sougou_pay 搜狗付费,
    a.sougou_income 搜狗绝对收益,
    a.360_web_cost as web360花费,
    a.360_web_pay as web360付费,
    a.360_web_income as web360绝对收益,
    a.baidu_web_cost as web百度花费,
    a.baidu_web_pay as web百度付费,
    a.baidu_web_income as web百度绝对收益
    from 
    (
    select
    a.ta,
    a.num as 360_num,
    a.cost as 360_cost,
    ifnull(a.pay, 0) as 360_pay,
    ifnull(a.pay, 0) - a.cost as 360_income,
    b.num as baidu_num,
    b.cost as baidu_cost,
    ifnull(b.pay, 0) as baidu_pay,
    ifnull(b.pay, 0) - b.cost as baidu_income,
    c.num as sougou_num,
    ifnull(c.cost,0) as sougou_cost,
    ifnull(c.pay,0) as sougou_pay,
    ifnull(c.pay,0) - ifnull(c.cost,0) as sougou_income,
    ifnull(d.cost,0) as 360_web_cost,
    ifnull(d.pay,0) as 360_web_pay,
    ifnull(d.pay,0) - ifnull(d.cost,0) as 360_web_income,
    ifnull(e.cost,0) as baidu_web_cost,
    ifnull(e.pay,0) as baidu_web_pay,
    ifnull(e.pay,0)- ifnull(e.cost,0) as baidu_web_income
    from
    -- ----------------------------------------------
    -- 360插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    date_format(created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(created_at) != current_date
    and extension_installed = 1
    and register_channel like '0214%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    from
    `360_sem_campaign_daily`
    where date_format(date,'%y-%m') = date_format(now(),'%y-%m')
    group by tb
    order by tb desc ) as b
    on a.ta =b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user`join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like '0214%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(new_order.created_at) != date(current_date)
    group by tc
    ) as c
    on b.tb= c.tc
    ) as a
    -- ----------------------------------------------
    -- 百度插件花销付费
    -- ----------------------------------------------
    left join 
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    date(created_at) != current_date
    and date_format(created_at,'%y-%m') = date_format(now(),'%y-%m')
    and extension_installed = 1
    and register_channel like 'baidu%'
    and `register_channel` not like'baidu_xmt%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(`baidu_sem_report`.report_time) as tb,
    ROUND(sum(cost),0) as cost
    from
    `baidu_sem_report`
    WHERE report_id=27461652 
    group by tb
    ) as b
    on a.ta = b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user`join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like 'baidu%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(new_order.created_at) != date(current_date)
    group by tc
    ) as c
    on  b.tb = c.tc
    ) as b
    on a.ta = b.ta
    left join 
    -- ----------------------------------------------
    -- 搜狗插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.num, 0) as num,
    ifnull(c.cost, 0) as cost,
    ifnull(d.pay, 0) as pay
    from
    (
    select
    date(`user`.created_at) as ta,
    count(id) 
    from `user` 
    where date_format(created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(created_at) != current_date
    group by ta 
    )as a 
    left join 
    (
    select
    date(user.created_at) as tb,
    count(1) as num
    from 
    `user`
    where register_channel like '%so%'
    and extension_installed = 1 
    and date_format(created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(created_at) != current_date
    group by tb
    ) as b 
    on a.ta =b.tb 
    left join 
    (
    select
    date as tc,
    ROUND(sum(cost),0) as cost
    from 
    `sougou_sem_campaign_report`
    group by tc) as c 
    on a.ta =c.tc
    left join 
    (
    select
    date(new_order.created_at) as td,
    ROUND(sum(new_order.price/100),0) as pay
    from (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order join `user`
    on new_order.user_id =`user`.id
    where paid=1
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    and user.register_channel like 'so%'
    group by td
    ) as d 
    on a.ta = d.td) as c
    on b.ta = c.ta
    left join 
    -- ----------------------------------------------
    -- baidu  web 插件花销付费
    -- ----------------------------------------------

    (
    SELECT
    a.ta,
    a.cost,
    b.pay
    from
    (
    SELECT
    date(report_time) as ta,
    ROUND(sum(cost),0) as cost
    FROM
    `baidu_web_edit_sem_report`
    where report_type ='2'
    -- and date(created_at) != date(current_date)
    and date_format(report_time,'%y-%m') = date_format(now(),'%y-%m')
    group by ta) AS a
    left join 
    (
    SELECT
    date( new_order.created_at ) AS tb,
    ROUND( SUM( price )/ 100 ) AS pay  
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    INNER JOIN `user` 
    WHERE
    `user`.`id` = new_order.`user_id` 
    AND new_order.`paid` = 1 
    and (register_channel like 'web_editor:baidu%'
    or register_channel like '%baidu_xmt%'
    or register_channel like '%xmt_bd%')
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    and date(new_order.created_at) != date(current_date)
    GROUP BY tb 
    ) as b
    on a.ta = b.tb ) as e
    on c.ta = e.ta
    left join 
    -- ----------------------------------------------
    -- 360 web 插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(a.cost, 0) as cost,
    ifnull(b.pay, 0) as pay
    from
    (select
    date(date) as ta,
    ROUND(sum(total_cost),0) as cost
    FROM
    `360_sem_campaign_daily_wbio`
    where  date_format(date,'%y-%m') = date_format(now(),'%y-%m')
    -- date(created_at) != date(current_date)
    and (
    campaign_id =1791046127  -- 品牌词
    or campaign_id =1774274543  -- 产品词
    or campaign_id =1740757047   -- 公众号组
    or campaign_id=2361520432    -- 新媒体
    --          campaign_id = 1354879796 -- 365 
    --          or campaign_id = 1421935668 -- 秀米 
    --          or campaign_id = 1421941812 -- 135
    --          or campaign_id = 1421993012  -- 96
    --          or campaign_id = 1472315956  -- 小蚂蚁
    --          or campaign_id = 3301000759  -- 新榜
    --          or campaign_id = 4156630838  -- i排版
    )
    group by ta) as a
    left join 
    (
    SELECT
    date(new_order.created_at) AS tb,
    ROUND(SUM( price )/100,0)  AS pay
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    INNER JOIN `user` 
    WHERE
    `user`.`id` = new_order.`user_id` 
    and date_format(new_order.created_at,'%y-%m') = date_format(now(),'%y-%m')
    AND new_order.`paid` = 1 
    AND (`user`.register_channel like 'web_editor:360%'
    or `user`.register_channel like 'web_editor:1201%'
    or `user`.register_channel like 'web_editor:1130%'
    or `user`.register_channel like 'xmt%'
    or `user`.register_channel like '%gzh%')
    and `user`.register_channel not like '%baidu%'
    and `user`.register_channel not like '%so%'
    and `user`.register_channel not like '%0214%'
    and `user`.register_channel not like '%xmt_bd%'
    and date(new_order.created_at) != date(current_date)
    and  new_order.user_id not in (11,51276,17851815,61735143,16)
    GROUP BY tb
    ) as b
    on a.ta =b.tb) as d
    on e.ta =d.ta
    )as a
"""
sqlb = """
 select
    a.ta,
    (a.360_income + a.baidu_income + a.sougou_income + a.360_web_income + a.baidu_web_income) as 合计绝对收益,
    (ifnull(a.360_num, 0)+ifnull(a.sougou_num, 0)+ifnull(a.baidu_num, 0)) as 合计安装数,
    (a.360_cost+a.baidu_cost+a.sougou_cost+a.360_web_cost+a.baidu_web_cost) as 合计花费,
    (a.360_pay+a.baidu_pay+a.sougou_pay+a.360_web_pay+a.baidu_web_pay) as 合计付费,
    a.360_num as 360安装,
    a.360_cost as 360花费,
    a.360_pay as 360付费,
    a.360_income 360绝对收益,
    a.baidu_num 百度安装,
    a.baidu_cost 百度花费,
    a.baidu_pay 百度付费,
    a.baidu_income 百度绝对收益,
    a.sougou_num 搜狗安装,
    a.sougou_cost 搜狗花费,
    a.sougou_pay 搜狗付费,
    a.sougou_income 搜狗绝对收益,
    a.360_web_cost as web360花费,
    a.360_web_pay as web360付费,
    a.360_web_income as web360绝对收益,
    a.baidu_web_cost as web百度花费,
    a.baidu_web_pay as web百度付费,
    a.baidu_web_income as web百度绝对收益
    from 
    (
     select
    a.ta,
    a.num as 360_num,
    a.cost as 360_cost,
    ifnull(a.pay, 0) as 360_pay,
    ifnull(a.pay, 0) - a.cost as 360_income,
    b.num as baidu_num,
    b.cost as baidu_cost,
    ifnull(b.pay, 0) as baidu_pay,
    ifnull(b.pay, 0) - b.cost as baidu_income,
    c.num as sougou_num,
    ifnull(c.cost,0) as sougou_cost,
    ifnull(c.pay,0) as sougou_pay,
    ifnull(c.pay,0) - ifnull(c.cost,0) as sougou_income,
    ifnull(d.cost,0) as 360_web_cost,
    ifnull(d.pay,0) as 360_web_pay,
    ifnull(d.pay,0) - ifnull(d.cost,0) as 360_web_income,
    ifnull(e.cost,0) as baidu_web_cost,
    ifnull(e.pay,0) as baidu_web_pay,
    ifnull(e.pay,0)- ifnull(e.cost,0) as baidu_web_income
    from
    -- ----------------------------------------------
    -- 360插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    and date(created_at) != current_date
    and extension_installed = 1
    and register_channel like '0214%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    from
    `360_sem_campaign_daily`
    where PERIOD_DIFF( date_format( now( )-1 , '%Y%m' ) , date_format(date,'%y%m') ) =1
    group by tb
    order by tb desc ) as b
    on a.ta =b.tb
    left join 
    (
     select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user` join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like '0214%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and PERIOD_DIFF( date_format( now( ), '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    group by tc
    ) as c
    on b.tb= c.tc
    ) as a
    -- ----------------------------------------------
    -- 百度插件花销付费
    -- ----------------------------------------------
    left join 
    (
    select
    a.ta,
    a.num,
    ifnull(b.cost, 0) as cost,
    ifnull(c.pay, 0) as pay
    from
    (
    select
    date(created_at) as ta,
    count(1) as num
    from 
    `user`
    where 
    date(created_at) != current_date
     and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(`user`.created_at,'%y%m') ) =1
    and extension_installed = 1
    and register_channel like 'baidu%'
    and `register_channel` not like'baidu_xmt%'
    group by ta 
    ) as a
    left join 
    (
    select 
    date(`baidu_sem_report`.report_time) as tb,
    ROUND(sum(cost),0) as cost
    from
    `baidu_sem_report`
    WHERE report_id=27461652 
    group by tb
    ) as b
    on a.ta = b.tb
    left join 
    (
    select
    date(new_order.created_at) as tc,
    ROUND(sum(price/100),0) as pay
    from
    `user`join (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    where 
    `user`.id=new_order.user_id
    and `user`.register_channel like 'baidu%'
    and `user`.register_channel not like '%xmt%'
    and new_order.paid=1
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    and date(new_order.created_at) != date(current_date)
    group by tc
    ) as c
    on  b.tb = c.tc
    ) as b
    on a.ta = b.ta
    left join 
    -- ----------------------------------------------
    -- 搜狗插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.num,0)as num,
    ifnull(c.cost,0)as cost,
    ifnull(d.pay,0)as pay
    from
    (
    select
    date(user.created_at) as ta,
    count(1) as num
    from 
    `user`
    where PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    group by ta
    ) as a 
    left join 
    (
    select
    date(user.created_at) as tb,
    count(1) as num
    from `user`
    where register_channel like '%so%'
    and extension_installed = 1 
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1
    group by tb
    ) as b 
    on a.ta =b.tb 
    left join 
    (
    select
    date as tc,
    ROUND(sum(cost),0) as cost
    from 
    `sougou_sem_campaign_report`
    group by tc
    ) as c 
    on a.ta =c.tc
    left join 
    (
    select
    date(new_order.created_at) as td,
    ROUND(sum(new_order.price/100),0) as pay
    from (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order join `user`
    on new_order.user_id =`user`.id
    where paid=1
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    and user.register_channel like 'so%'
    group by td
    ) as d 
    on a.ta = d.td
    ) as c
    on b.ta = c.ta
    left join 
    -- ----------------------------------------------
    -- baidu  web 插件花销付费
    -- ----------------------------------------------

    (
    SELECT
    a.ta,
    a.cost,
    b.pay
    from
    (
    SELECT
    date(report_time) as ta,
    ROUND(sum(cost),0) as cost
    FROM
    `baidu_web_edit_sem_report`
    where report_type ='2'
    -- and date(created_at) != date(current_date)
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(report_time,'%y%m') ) =1
    group by ta) AS a
    left join 
    (
    SELECT
    date( new_order.created_at ) AS tb,
    ROUND( SUM( price )/ 100 ) AS pay  
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order
    INNER JOIN `user` 
    WHERE
    `user`.`id` = new_order.`user_id` 
    AND new_order.`paid` = 1 
    and (register_channel like 'web_editor:baidu%'
    or register_channel like '%baidu_xmt%'
    or register_channel like '%xmt_bd%')
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    and date(new_order.created_at) != date(current_date)
    GROUP BY tb 
    ) as b
    on a.ta = b.tb ) as e
    on c.ta = e.ta
    left join 
    -- ----------------------------------------------
    -- 360 web 插件花销付费
    -- ----------------------------------------------
    (
    select
    a.ta,
    ifnull(b.cost,0)as cost,
    ifnull(c.pay,0)as pay 
    from
    (
    select
    date(created_at)as ta,
    count(id)
    from `user`
    where  PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(created_at,'%y%m') ) =1 
    group by ta 
    )as a 
    left join 
    (
    select
    date(date) as tb,
    ROUND(sum(total_cost),0) as cost
    FROM `360_sem_campaign_daily_wbio`
    where  PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(date,'%y%m') ) =1
    and (
    campaign_id =1791046127  -- 品牌词
    or campaign_id =1774274543  -- 产品词
    or campaign_id =1740757047   -- 公众号组
    or campaign_id=2361520432    -- 新媒体
    --          campaign_id = 1354879796 -- 365 
    --          or campaign_id = 1421935668 -- 秀米 
    --          or campaign_id = 1421941812 -- 135
    --          or campaign_id = 1421993012  -- 96
    --          or campaign_id = 1472315956  -- 小蚂蚁
    --          or campaign_id = 3301000759  -- 新榜
    --          or campaign_id = 4156630838  -- i排版
    )
    group by tb
    ) as b 
    on a.ta=b.tb 
    left join 
    (
    SELECT
    date(new_order.created_at) AS tc,
    ROUND(SUM( price )/100,0)  AS pay
    FROM (
        select 
            awuo.user_id, awuo.price, awuo.paid, awuo.paid_at as created_at 
        from ai_writing_user_order awuo 
        where awuo.paid = 1 and awuo.user_id not in (
            1407108502, 52605109, 17851815, 51276, 
            968253725, 1251436642, 56855150
        )
        
        union all
        
        select 
            o.user_id, o.price, o.paid, o.created_at 
        from `order` o 
        where o.paid = 1
    ) new_order INNER JOIN `user` 
    WHERE `user`.`id` = new_order.`user_id` 
    and PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format(new_order.created_at,'%y%m') ) =1
    AND new_order.`paid` = 1 
    AND (`user`.register_channel like 'web_editor:360%'
    or `user`.register_channel like 'web_editor:1201%'
    or `user`.register_channel like 'web_editor:1130%'
    or `user`.register_channel like 'xmt%'
    or `user`.register_channel like '%gzh%')
    and `user`.register_channel not like '%baidu%'
    and `user`.register_channel not like '%so%'
    and `user`.register_channel not like '%0214%'
    and `user`.register_channel not like '%xmt_bd%'
    and  new_order.user_id not in (11,51276,17851815,61735143,16)
    GROUP BY tc
    ) as c
    on a.ta =c.tc
    ) as d
    on e.ta =d.ta
    )as a
    """


# 付费sql
paid_sql = f"""
    select 
		sum(price)/100 as total,
		date(o.created_at) as paid_date
	from `order` o 
	where o.paid = 1
	group by date(o.created_at)
"""

# 安装sql
installed_sql = f"""
    select
        count(1) as installed_count,
        date(u.created_at) as installed_date
    from `user` u 
    where extension_installed = 1
    group by date(u.created_at)
"""

# 消耗sql
cost_sql = f"""
    select 
		sum(sem.cost) as total,
		date(sem.cost_date) as cost_date
	from (
		# 360插件花销付费
		select 
			date(scd.`date`) as cost_date,
			round(sum(scd.total_cost), 2) as cost,
			'360' as platform
		from `360_sem_campaign_daily` scd 
		group by date(scd.`date`)
		
		union all
		
		# 360 web 插件花销付费
		select 
			date(scdw.`date`) as cost_date,
			round(sum(scdw.total_cost), 2) as cost,
			'360 web' as platform
		from `360_sem_campaign_daily_wbio` scdw 
		where scdw.campaign_id =1791046127  -- 品牌词
		 	or scdw.campaign_id =1774274543  -- 产品词
		 	or scdw.campaign_id =1740757047   -- 公众号组
		 	or scdw.campaign_id=2361520432    -- 新媒体
		group by date(scdw.`date`)
		
		union all
		
		# 百度插件花销付费
		select 
			date(bsr.report_time) as cost_date,
			round(sum(bsr.cost), 2) as cost,
			'百度' as platform
		from baidu_sem_report bsr 
		where bsr.report_id=27461652 
		group by date(bsr.report_time)
		
		union all
		
		# baidu  web 插件花销付费
		select 
			date(bwesr.report_time) as cost_date,
			round(sum(bwesr.cost), 2) as cost,
			'baidu web' as platform
		from baidu_web_edit_sem_report bwesr 
		where report_type ="2"
		group by date(bwesr.report_time)
		
		union all
		
		# 搜狗插件花销付费
		select 
			date(sscr.`date`) as cost_date,
			round(sum(sscr.cost), 2) as cost,
			'搜狗' as platform
		from sougou_sem_campaign_report sscr 
		group by date(sscr.`date`)
	) as sem
	group by sem.cost_date
"""

# 收益
profit_sql = f"""
    select 
        paid.total - cost.total as profit,
        paid.paid_date as profit_date
    from (
        {paid_sql}
    ) paid
    join (
        {cost_sql}
    ) cost on cost.cost_date = paid.paid_date
"""

ai_writing_sql = f"""
    select 
        sum(awuo.price) / 100 as total,
        date(awuo.paid_at) as paid_date
    from ai_writing_user_order awuo
    where awuo.paid = 1 and awuo.user_id not in (
        1407108502, 52605109, 17851815, 51276, 
        968253725, 1251436642, 56855150
    )
    group by date(awuo.paid_at)
"""


def data_daily_statics(cursor):
    yes_total_paid_sql = f"""
        select 
            round(sum(paid.total), 2)
        from (
            {paid_sql}
        ) paid
        where paid.paid_date = date_sub(current_date, interval 1 day)
    """
    cursor.execute(yes_total_paid_sql)
    yes_total_paid = cursor.fetchone()[0]

    current_month_paid_sql = f"""
        select 
            round(sum(paid.total), 2)
        from (
            {paid_sql}
        ) paid
        where paid.paid_date >= date(concat(left(date_sub(current_date, interval 1 day), 8), '01'))
            and paid.paid_date < current_date
    """
    cursor.execute(current_month_paid_sql)
    current_month_paid = cursor.fetchone()[0]

    last_month_paid_sql = f"""
        select 
            round(sum(paid.total), 2)
        from (
            {paid_sql}
        ) paid
        where paid.paid_date >= date_sub(date(concat(left(date_sub(current_date, interval 1 day), 8), '01')), interval 1 month)
            and paid.paid_date < date_sub(current_date, interval 1 month)
    """
    cursor.execute(last_month_paid_sql)
    last_month_paid = cursor.fetchone()[0]

    yes_installed_sql = f"""
        select 
            sum(installed.installed_count)
        from (
            {installed_sql}
        ) installed
        where installed.installed_date = date_sub(current_date, interval 1 day)
    """
    cursor.execute(yes_installed_sql)
    yes_installed = cursor.fetchone()[0]

    current_month_installed_sql = f"""
        select 
            sum(installed.installed_count)
        from (
            {installed_sql}
        ) installed
        where installed.installed_date  >= date(concat(left(date_sub(current_date, interval 1 day), 8), '01'))
            and installed.installed_date < current_date
    """
    cursor.execute(current_month_installed_sql)
    current_month_installed = cursor.fetchone()[0]

    last_month_installed_sql = f"""
        select 
            sum(installed.installed_count)
        from (
            {installed_sql}
        ) installed
        where installed.installed_date  >= date_sub(date(concat(left(date_sub(current_date, interval 1 day), 8), '01')), interval 1 month)
            and installed.installed_date < date_sub(current_date, interval 1 month)
    """
    cursor.execute(last_month_installed_sql)
    last_month_installed = cursor.fetchone()[0]

    yes_profit_sql = f"""
        select 
            sum(profit.profit)
        from (
            {profit_sql}
        ) profit
        where profit.profit_date = date_sub(current_date, interval 1 day)
    """
    cursor.execute(yes_profit_sql)
    yes_profit = cursor.fetchone()[0]

    current_month_profit_sql = f"""
        select 
            sum(profit.profit)
        from (
            {profit_sql}
        ) profit
        where profit.profit_date  >= date(concat(left(date_sub(current_date, interval 1 day), 8), '01'))
            and profit.profit_date < current_date
    """
    cursor.execute(current_month_profit_sql)
    current_month_profit = cursor.fetchone()[0]

    last_month_profit_sql = f"""
        select 
            sum(profit.profit)
        from (
            {profit_sql}
        ) profit
        where profit.profit_date  >= date_sub(date(concat(left(date_sub(current_date, interval 1 day), 8), '01')), interval 1 month)
            and profit.profit_date < date_sub(current_date, interval 1 month)
    """
    cursor.execute(last_month_profit_sql)
    last_month_profit = cursor.fetchone()[0]

    yes_total_repaid_sql = f"""
        select 
            round(sum(repaid.price), 2)
        from (
            select
                date(created_at) as paid_date,
                sum(price/100) as price
            from (
                select
                    distinct o1.id,
                    o1.created_at,
                    o1.price
                from `order` o1
                join `order` o2 on o1.user_id = o2.user_id and o2.created_at < o1.created_at
                where o1.paid = 1 and o2.paid = 1
            ) as f group by paid_date 
            order by paid_date desc 
        ) repaid
        where repaid.paid_date = date_sub(current_date, interval 1 day)
    """
    cursor.execute(yes_total_repaid_sql)
    yes_total_repaid = cursor.fetchone()[0]

    yes_total_new_paid_sql = f"""
        select 
            round(sum(new_paid.price), 2)
        from (
            select
                date(created_at) as paid_date,
                sum(price/100) as price
            from (
                select
                    distinct o1.id,
                    o1.created_at,
                    o1.price
                from `order` o1
                left join `order` o2 on o1.user_id = o2.user_id and o2.paid = 1 and o2.created_at < o1.created_at
                where o1.paid = 1 and o2.id is null
            ) as f group by paid_date 
            order by paid_date desc 
        ) new_paid
        where new_paid.paid_date = date_sub(current_date, interval 1 day)
    """
    cursor.execute(yes_total_new_paid_sql)
    yes_total_new_paid = cursor.fetchone()[0]

    yes_new_user_total_repaid_sql = f"""
        select 
            round(sum(repaid.price), 2)
        from (
            select 
                date(f.order_created_at) as paid_date,
                sum(f.price/100) as price
            from (
                select 
                    o.price, o.created_at as order_created_at
                from `order` o 
                join `user` u on u.id = o.user_id and datediff(o.created_at, u.created_at) between 0 and 7
                where o.paid = 1
            ) as f group by paid_date
            order by paid_date desc 
        ) repaid
        where repaid.paid_date = date_sub(current_date, interval 1 day)
    """
    cursor.execute(yes_new_user_total_repaid_sql)
    yes_new_user_total_repaid = cursor.fetchone()
    yes_new_user_total_repaid = (
        yes_new_user_total_repaid[0] if yes_new_user_total_repaid else 0
    )

    yes_ai_writing_paid_sql = f"""
        select 
            sum(ai_writing.total)
        from (
            {ai_writing_sql}
        ) ai_writing
        where ai_writing.paid_date = date_sub(current_date, interval 1 day)
    """
    cursor.execute(yes_ai_writing_paid_sql)
    yes_ai_writing_paid = cursor.fetchone()[0]

    current_month_ai_writing_paid_sql = f"""
        select 
            sum(ai_writing.total)
        from (
            {ai_writing_sql}
        ) ai_writing
        where ai_writing.paid_date >= date(concat(left(date_sub(current_date, interval 1 day), 8), '01'))
            and ai_writing.paid_date < current_date
    """
    cursor.execute(current_month_ai_writing_paid_sql)
    current_month_ai_writing_paid = cursor.fetchone()[0]

    last_month_ai_writing_paid_sql = f"""
        select 
            sum(ai_writing.total)
        from (
            {ai_writing_sql}
        ) ai_writing
        where ai_writing.paid_date >= date_sub(date(concat(left(date_sub(current_date, interval 1 day), 8), '01')), interval 1 month)
            and ai_writing.paid_date < date_sub(current_date, interval 1 month)
    """
    cursor.execute(last_month_ai_writing_paid_sql)
    last_month_ai_writing_paid = cursor.fetchone()[0]

    msg = f""">昨日总付费： {round(yes_total_paid + yes_ai_writing_paid, 2)}
>昨日AI写作付费： {round(yes_ai_writing_paid, 2)}
>复购用户付费金额： {round(yes_total_repaid, 2)}
>新购用户付费金额：{round(yes_total_new_paid, 2)}
>新用户付费金额：{round(yes_new_user_total_repaid, 2)}
>当月累计付费：<font color="info">{round(current_month_paid + current_month_ai_writing_paid, 2)}</font>
>同比上月同期：<font color="comment">{round((
    (
        (current_month_paid + current_month_ai_writing_paid) - (last_month_paid + last_month_ai_writing_paid) 
    ) / (last_month_paid + last_month_ai_writing_paid)
) * 100, 2)}%</font>
\n<font color="info"> </font>\n
>昨日总安装：{yes_installed}
>当月累计安装：<font color="info">{current_month_installed}</font>
>同比上月同期：<font color="comment">{round(((current_month_installed-last_month_installed)/last_month_installed)*100, 2)}%</font>
\n<font color="info"> </font>\n
>昨日总收益：{round(yes_profit + float(yes_ai_writing_paid), 2)}
>当月累计收益：<font color="info">{round(current_month_profit + float(current_month_ai_writing_paid), 2)}</font>
>同比上月同期：<font color="comment">{round((
    (
        (current_month_profit + float(current_month_ai_writing_paid)) - (last_month_profit + float(last_month_ai_writing_paid))
    ) / (last_month_profit + float(last_month_ai_writing_paid))
) * 100, 2)}%</font>"""

    return msg


def send_yiban_notify():
    date_today = datetime.datetime.today()
    today = (
        str(date_today.year)
        + "年"
        + str(date_today.month)
        + "月"
        + str(date_today.day)
        + "日"
    )
    now = datetime.datetime.today()
    yesterday = (date_today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    if now.day == 1:
        conn = pymysql.connect(
            port=61694,  # 连接服务器的端口名
            host="",  # 连接服务器的地址
            user="",  # 连接服务器的用户名
            password="",  # 连接用户的密码
            db="yiban",  # 连接的库名
            charset="utf8",  # 连接编码)
        )
        print("数据库连接成功")
        cursor = conn.cursor()
        cursor1 = conn.cursor()

        cursor.execute(sqla)
        cursor1.execute(sqlb)
        data = cursor.fetchall()
        data1 = cursor1.fetchall()

        data_daily_statics_cursor = conn.cursor()
        data_daily_statics_msg = data_daily_statics(data_daily_statics_cursor)
        data_daily_statics_cursor.close()

        # conn.commit()
        cursor.close()
        cursor1.close()
        conn.close()
        df = pandas.DataFrame(
            list(data1),
            columns=[
                "日期",
                "合计绝对收益",
                "合计安装数",
                "合计花费",
                "合计付费",
                "360安装",
                "360花费",
                "360付费",
                "360绝对收益",
                "百度安装",
                "百度花费",
                "百度付费",
                "百度绝对收益",
                "搜狗安装",
                "搜狗花费",
                "搜狗付费",
                "搜狗绝对收益",
                "web360花费",
                "web360付费",
                "web360绝对收益",
                "web百度花费",
                "web百度付费",
                "web百度绝对收益",
            ],
        )
        print("本月数据转换成功")
        df_all = pandas.DataFrame(
            list(data),
            columns=[
                "ta",
                "aincome",
                "a_pay",
                "num",
                "a_num",
                "tb",
                "bincome",
                "b_pay",
                "num",
                "b_num",
            ],
        )
        df = df.replace({None: 0})
        df_all = df_all.replace({None: 0})
        print("同比数据转换成功")
        df_all["ta"] = pandas.to_datetime(df_all["ta"])
        # 将日期设置为索引 去掉默认索引
        df_all = df_all.set_index("ta")
        # 将上月数据以最后一条记录向下填充 保证同比计算的准确延续性
        df_all["b_pay"] = df_all["b_pay"].fillna(method="ffill")
        df_all["b_num"] = df_all["b_num"].fillna(method="ffill")
        # 计算累计付费、累计安装同比上月同期
        df_all["pay_rate"] = round(
            ((df_all["a_pay"] - df_all["b_pay"]) / abs(df_all["b_pay"])) * 100, 2
        )
        df_all["num_rate"] = round(
            ((df_all["a_num"] - df_all["b_num"]) / df_all["b_num"]) * 100, 2
        )
        # 添加百分号
        df_all.pay_rate = df_all["pay_rate"].astype(str).add("%")
        df_all.num_rate = df_all["num_rate"].astype(str).add("%")
        # print(df_all.to_string())
        warnings.simplefilter("ignore")
        df_all = df_all[["pay_rate", "num_rate"]].copy()
        # print(df_all.to_string())
        # 用index == 防止报错keyerror 转换为列表取数
        df_all = df_all[df_all.index == yesterday].values.tolist()
        print("同比数据已采集")
        # 添加累计计算列
        df.insert(1, "累计花费", "", allow_duplicates=False)
        df.insert(2, "累计安装", "", allow_duplicates=False)
        df.insert(3, "累计付费", "", allow_duplicates=False)
        df.insert(4, "月度收益", "", allow_duplicates=False)
        # 对其他字段 累加计算
        df["累计花费"] = df["合计花费"].cumsum()
        df["累计安装"] = df["合计安装数"].cumsum()
        df["累计付费"] = df["合计付费"].cumsum()
        df["月度收益"] = df["合计绝对收益"].cumsum()
        # 将日期转换为时间类型
        df["日期"] = pandas.to_datetime(df["日期"])
        # 将日期设置为索引 去掉默认索引
        df = df.set_index("日期")
        # 整体结果倒序 「只有倒序后 才能用yesterday传值匹配查找昨天的数据」
        df = df.iloc[::-1]

        # 取易亏账户绝对收益
        df1 = df[["搜狗绝对收益", "web360绝对收益", "web百度绝对收益"]].copy()
        # 转换数据类型为int，让数据避免小数点
        df1["搜狗绝对收益"] = df1["搜狗绝对收益"].astype("int64")
        df1["web360绝对收益"] = df1["web360绝对收益"].astype("int64")
        df1["web百度绝对收益"] = df1["web百度绝对收益"].astype("int64")
        warnings.simplefilter("ignore")
        # 限定时间为昨天 并转换为列表套字典
        df1 = df1[df1.index == yesterday]
        # df1 = df1[yesterday]
        df1 = df1.to_dict("records")
        # 定义一个列表接收数据
        li_1 = []
        # 遍历数据 数据结构是list套字典
        for i in df1:
            # 遍历字典 将字典转换为元祖形式 方便后续用下标传递数据
            for val in i.items():
                # 将元祖添加进列表
                li_1.append(val)
        # 将数据传入map 方便content调用
        record_map = {
            "搜狗": ("收益", li_1[0][1]),
            "web百度": ("收益", li_1[2][1]),
            "web360": ("收益", li_1[1][1]),
        }
        print("各渠道收益数据已采集")
        # 取所需字段
        df2 = df[["合计绝对收益", "月度收益", "合计安装数", "累计安装"]].copy()
        # 转换为整数类型 避免错误数据类型出现
        df2["合计绝对收益"] = df2["合计绝对收益"].astype("int64")
        df2["月度收益"] = df2["月度收益"].astype("int64")
        warnings.simplefilter("ignore")
        # 转换成列表
        df2 = df2[df2.index == yesterday].values.tolist()  # 取所需字段
        # 用变量接收数据
        a = df2[0][0]
        b = df2[0][1]
        c = df2[0][2]
        d = df2[0][3]
        e = df_all[0][0]
        f = df_all[0][1]
        print("数据采集完毕")
        # 文本格式 不能缩近 缩近格式会错误
        content = f"""**{today}壹伴收益播报**
{data_daily_statics_msg}
\n<font color="info"> </font>\n
>昨日sem绝对收益： {a}
>当月累计sem收益：<font color=\"info\">{b}</font>
>同比上月同期：<font color=\"comment\">{e}</font>
\n<font color="info"> </font>\n
>昨日sem安装数：{c}
>当月累计sem安装：<font color=\"info\">{d}</font>
>同比上月同期：<font color=\"comment\">{f}</font>
\n<font color="info"> </font>\n
>易亏损渠道收益情况：搜狗:<font color=\"warning\">{record_map.get("搜狗")[1]}</font>元、web百度:<font color=\"warning\">{record_map.get("web百度")[1]}</font>元、web360:<font color=\"warning\">{record_map.get("web360")[1]}</font>元
[如需了解详情，请点击：[metabase表单地址](http://129.204.167.254:31234/question/1073)]\n"""
        msg_data = {"content": content}
        print("开始发送数据")
        print(msg_data)
        send_message(group_chatid, msg_data)
        print("消息已发送，程序已执行完毕")

    else:
        conn = pymysql.connect(
            port=61694,  # 连接服务器的端口名
            host="",  # 连接服务器的地址
            user="",  # 连接服务器的用户名
            password="",  # 连接用户的密码
            db="yiban",  # 连接的库名
            charset="utf8",  # 连接编码)
        )
        print("数据库连接成功")
        cursor = conn.cursor()
        cursor1 = conn.cursor()

        cursor.execute(sql)
        cursor1.execute(sql1)
        data = cursor.fetchall()
        data1 = cursor1.fetchall()

        data_daily_statics_cursor = conn.cursor()
        data_daily_statics_msg = data_daily_statics(data_daily_statics_cursor)
        data_daily_statics_cursor.close()

        # conn.commit()
        cursor.close()
        cursor1.close()
        conn.close()
        df = pandas.DataFrame(
            list(data1),
            columns=[
                "日期",
                "合计绝对收益",
                "合计安装数",
                "合计花费",
                "合计付费",
                "360安装",
                "360花费",
                "360付费",
                "360绝对收益",
                "百度安装",
                "百度花费",
                "百度付费",
                "百度绝对收益",
                "搜狗安装",
                "搜狗花费",
                "搜狗付费",
                "搜狗绝对收益",
                "web360花费",
                "web360付费",
                "web360绝对收益",
                "web百度花费",
                "web百度付费",
                "web百度绝对收益",
            ],
        )
        print("本月数据转换成功")
        df_all = pandas.DataFrame(
            list(data),
            columns=[
                "ta",
                "aincome",
                "a_pay",
                "num",
                "a_num",
                "tb",
                "bincome",
                "b_pay",
                "num",
                "b_num",
            ],
        )
        df = df.replace({None: 0})
        df_all = df_all.replace({None: 0})
        print("同比数据转换成功")
        df_all["ta"] = pandas.to_datetime(df_all["ta"])
        # 将日期设置为索引 去掉默认索引
        df_all = df_all.set_index("ta")
        # 将上月数据以最后一条记录向下填充 保证同比计算的准确延续性
        df_all["b_pay"] = df_all["b_pay"].fillna(method="ffill")
        df_all["b_num"] = df_all["b_num"].fillna(method="ffill")
        # 计算累计付费、累计安装同比上月同期
        df_all["pay_rate"] = round(
            ((df_all["a_pay"] - df_all["b_pay"]) / abs(df_all["b_pay"])) * 100, 2
        )
        df_all["num_rate"] = round(
            ((df_all["a_num"] - df_all["b_num"]) / df_all["b_num"]) * 100, 2
        )
        # 添加百分号
        df_all.pay_rate = df_all["pay_rate"].astype(str).add("%")
        df_all.num_rate = df_all["num_rate"].astype(str).add("%")
        # print(df_all.to_string())
        warnings.simplefilter("ignore")
        df_all = df_all[["pay_rate", "num_rate"]].copy()
        # print(df_all.to_string())
        # 用index == 防止报错keyerror 转换为列表取数
        df_all = df_all[df_all.index == yesterday].values.tolist()
        print("同比数据已采集")
        # 添加累计计算列
        df.insert(1, "累计花费", "", allow_duplicates=False)
        df.insert(2, "累计安装", "", allow_duplicates=False)
        df.insert(3, "累计付费", "", allow_duplicates=False)
        df.insert(4, "月度收益", "", allow_duplicates=False)
        # 对其他字段 累加计算
        df["累计花费"] = df["合计花费"].cumsum()
        df["累计安装"] = df["合计安装数"].cumsum()
        df["累计付费"] = df["合计付费"].cumsum()
        df["月度收益"] = df["合计绝对收益"].cumsum()
        # 将日期转换为时间类型
        df["日期"] = pandas.to_datetime(df["日期"])
        # 将日期设置为索引 去掉默认索引
        df = df.set_index("日期")
        # 整体结果倒序 「只有倒序后 才能用yesterday传值匹配查找昨天的数据」
        df = df.iloc[::-1]
        # 取易亏账户绝对收益
        df1 = df[["搜狗绝对收益", "web360绝对收益", "web百度绝对收益"]].copy()
        # 转换数据类型为int，让数据避免小数点
        df1["搜狗绝对收益"] = df1["搜狗绝对收益"].astype("int64")
        df1["web360绝对收益"] = df1["web360绝对收益"].astype("int64")
        df1["web百度绝对收益"] = df1["web百度绝对收益"].astype("int64")
        warnings.simplefilter("ignore")
        # 限定时间为昨天 并转换为列表套字典
        df1 = df1[df1.index == yesterday]
        # df1 = df1[yesterday]
        df1 = df1.to_dict("records")
        # 定义一个列表接收数据
        li_1 = []
        # 遍历数据 数据结构是list套字典
        for i in df1:
            # 遍历字典 将字典转换为元祖形式 方便后续用下标传递数据
            for val in i.items():
                # 将元祖添加进列表
                li_1.append(val)
        # 将数据传入map 方便content调用
        record_map = {
            "搜狗": ("收益", li_1[0][1]),
            "web百度": ("收益", li_1[2][1]),
            "web360": ("收益", li_1[1][1]),
        }
        print("各渠道收益数据已采集")
        # 取所需字段
        df2 = df[["合计绝对收益", "月度收益", "合计安装数", "累计安装"]].copy()
        # 转换为整数类型 避免错误数据类型出现
        df2["合计绝对收益"] = df2["合计绝对收益"].astype("int64")
        df2["月度收益"] = df2["月度收益"].astype("int64")
        warnings.simplefilter("ignore")
        # 转换成列表
        # df2 = df2[yesterday].values.tolist()  # 取所需字段
        df2 = df2[df2.index == yesterday].values.tolist()  # 取所需字段
        # 用变量接收数据
        a = df2[0][0]
        b = df2[0][1]
        c = df2[0][2]
        d = df2[0][3]
        e = df_all[0][0]
        f = df_all[0][1]
        print("数据采集完毕")
        # 文本格式 不能缩近 缩近格式会错误
        content = f"""**{today}壹伴收益播报**
{data_daily_statics_msg}
\n<font color="info"> </font>\n
>昨日sem绝对收益： {a}
>当月累计sem收益：<font color=\"info\">{b}</font>
>同比上月同期：<font color=\"comment\">{e}</font>
\n<font color="info"> </font>\n
>昨日sem安装数：{c}
>当月累计sem安装：<font color=\"info\">{d}</font>
>同比上月同期：<font color=\"comment\">{f}</font>
\n<font color="info"> </font>\n
>易亏损渠道收益情况：搜狗:<font color=\"warning\">{record_map.get("搜狗")[1]}</font>元、web百度:<font color=\"warning\">{record_map.get("web百度")[1]}</font>元、web360:<font color=\"warning\">{record_map.get("web360")[1]}</font>元
[如需了解详情，请点击：[metabase表单地址](http://129.204.167.254:31234/question/1073)]\n"""
        msg_data = {"content": content}
        print("开始发送数据")
        print(msg_data)
        send_message(group_chatid, msg_data)
        print("消息已发送，程序已执行完毕")


# 拿微伴token
def get_weiban_token():
    url = "https://open.weibanzhushou.com/open-api/access_token/get"

    payload = json.dumps(
        {"corp_id": "1653633638901744641", "secret": ""}
    )
    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)
    token = response.json()
    token = token["access_token"]
    return token


# 发送消息
def send_message(chatid, msg_data):
    url = (
        "https://open.weibanzhushou.com/open-api/group_chat/send_message?access_token="
        + get_weiban_token()
    )
    payload = {
        "chatid": chatid,
        "msgtype": "markdown",
        # 注意点：传参的msg_data里本身为 key：values
        # 如果没去掉{}符号，那么就等于{msg_data:{{"content":content}}} 这个数据类型就是错误的 缺少了key
        # 现在的数据类型为{msg_data:{"content":content}} 这个是对的
        "msg_data": msg_data,
    }
    headers = {"Content-Type": "application/json"}

    response = requests.request(
        "POST", url, headers=headers, data=json.dumps(payload), verify=False
    )
    data_type = response.json()
    data_type = data_type["errmsg"]
    print(f"发送状态：{data_type}")


if __name__ == "__main__":
    cron = BlockingScheduler(timezone="Asia/Shanghai")
    cron.add_job(
        send_yiban_notify, trigger="cron", hour="10", minute="23", max_instances=100
    )
    cron.start()
