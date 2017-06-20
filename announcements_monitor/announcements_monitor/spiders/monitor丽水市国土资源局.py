# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: proxy_spider.py
    @time: 2017/3/9 16:27
--------------------------------
"""
import sys
import os

import numpy as np
import pandas as pd
import scrapy
import announcements_monitor.items
import re
import traceback
import datetime
import bs4
import json
log_path = r'%s/log/spider_DEBUG(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
import csv_report

log_obj = set_log.Logger(log_path, set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup(log_path, if_cleanup=False)  # 是否需要在每次运行程序前清空Log文件
csv_report = csv_report.csv_report()

"""
bs = bs4.BeautifulSoup(s,'html.parser')
e_trs = bs.find_all('tr')
e_trs[0].get_text()
"""
with open(os.getcwd() + r'\announcements_monitor\spiders\needed_data.txt', 'r') as f:
    s = f.read()
    needed_data = s.split(',')
needed_data = [s.encode('utf8') for s in needed_data]

re_type = {
    'parcel_location':ur'土地坐落：.+?。',
    'purpose':ur'土地用途：.+?。',
    '出让年限':ur'出让年限：.+?。'
}
title_type1 = {7:['parcel_location', 'offer_area_m2', 'plot_ratio', '建筑密度（%）'
               '绿地率（%）', 'starting_price_sum', '保证金(万元)'],
               9:['parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度（%）'
               '绿地率（%）', '出让年限(年)', 'starting_price_sum', '保证金(万元)'],
               11:['序号', 'parcel_location', 'parcel_name', 'offer_area_m2', 'purpose',
                   '出让年限(年)', 'plot_ratio', '建筑密度（%）', '绿地率（%）',  'starting_price_sum',
                   '保证金(万元)'],
               12:['parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度（%）'
               '绿地率（%）', '出让年限(年)', '投资强度（万元/亩）', '土地产出（万元/亩）',
                '土地税收(万元)', 'starting_price_sum', '保证金(万元)'],
               }
title_height1 = {7:1, 9:2, 11:1, 12:2}
class Spider(scrapy.Spider):
    name = "511712"

    def start_requests(self):
        # 台州相应网址的index的系数，index_1代表第二页
        self.urls1 = ["http://www.lssgtzyj.gov.cn/ArticleList/Index/284?pageIndex=%s&title=" %i for i in xrange(3) if i > 0]
        #self.urls2 = ["http://www.zjtzgtj.gov.cn/scxx/tdsc/tdcrcj/index_%s.html" %i for i in xrange(3) if i > 0]

        for url in self.urls1:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
            导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        #e_table = bs_obj.find('div', class_='txtlist')
        e_row = bs_obj.find_all('div', class_='Lcon02R-2-01')
        for e_tr in e_row:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '丽水'
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tr.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_tr.find('p', class_='p21').get_text(strip=True) # 成交日期
                item['monitor_url'] = "http://www.lssgtzyj.gov.cn" + e_tr.a.get('href')

                yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
            except:
                log_obj.error(u"%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        try:
            e_page = bs_obj.find('div', attrs={'id':'infoContent', 'class':'SconC'})
            # 处理网页文字

            e_ps = e_page.find_all('p')
            row_ps = [e_p.get_text(strip=True) for e_p in e_ps]
            d = {}
            for row_s in row_ps:
                for rs in re_type:
                    m = re.search(re_type[rs], row_s)
                    if m:
                        d[rs] = m.group()

            # 处理网页中的表格
            e_table = e_page.table
            e_trs = e_table.find_all('tr')
            test_row = e_trs[-1].find_all('td')
            #if len(test_row) not in title_type1:
            #    raise
            e_trs = e_trs[title_height1[len(test_row)]:]
            for e_tr in e_trs:
                e_tds = e_tr.find_all('td')
                title = title_type1[len(e_tds)]
                row = [e_td.get_text(strip=True) for e_td in e_tds]

                detail = dict(zip(title,row))
                detail.update(d)
                content_detail = {'addition':{}}
                for key in detail:
                    if key in needed_data:
                        content_detail[key] = detail[key]
                    else:
                        content_detail['addition'][key] = detail[key]

                content_detail['parcel_no'] = re.search(ur'丽土.+?号', item['monitor_title']).group()
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error("%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass