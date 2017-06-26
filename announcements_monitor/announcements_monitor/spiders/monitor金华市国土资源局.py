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

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########

log_obj = spider_log.spider_log() #########

needed_data = ['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', 'starting_price_sum', 'starting_price']
title_type1 = [[u'\n\n\n序号\n\n\n\n\n土地位置\n\n\n\n\n土地面积（m2）\n\n\n\n\n土地 用途\n\n\n\n\n规划指标要求\n\n\n\n\n出让年限（年）\n\n\n\n\n起始（叫）价(元/ m2)\n\n\n\n\n竞买保证金(万元)\n\n\n',
               u'\n\n\n容积率\n\n\n\n\n建筑密度\n\n\n\n\n绿地率\n\n\n\n\n建筑限高（米）\n\n\n'],
               ['parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度', '绿地率',
                '出让年限(年)', 'starting_price', '竞买保证金 (万元)']]
title_type2 = [[[u'\n\n\n宗地\n\n\n\n\n土地位置\n\n\n\n\n用地面积(㎡)\n\n\n\n\n土地用途\n\n\n\n\n规划指标要求\n\n\n\n\n出让年限（年）\n\n\n\n\n起始价\n\n\n（元/㎡）\n\n\n\n\n竞买保证金（万元）\n\n\n',
               u'\n\n\n容积率\n\n\n\n\n建筑密度\n\n\n\n\n绿地率\n\n\n']],
               ['parcel_name', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度', '绿地率',
                '出让年限(年)', 'starting_price', '竞买保证金 (万元)']]

class Spider(scrapy.Spider):
    name = "511709"

    def start_requests(self):
        self.urls1 = ["http://www.jhdlr.gov.cn/ywgg/news_7_%s.htm" %i for i in xrange(3)]
        self.urls2 = ["http://www.jhdlr.gov.cn/ywgg/news_8_%s.htm" %i for i in xrange(3)]

        for url in self.urls1 + self.urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
            导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        e_table = bs_obj.find('div', class_='ssbody_right_body')
        e_row = e_table.find_all('li')
        for e_li in e_row:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '金华'
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_li.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_li.span.get_text(strip=True) # 成交日期 site.xpath('td[3]/text()').extract_first()
                item['monitor_url'] = "http://www.jhdlr.gov.cn/" + e_li.a.get('href').replace('../../','')

                yield item
            except:
                log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'


    def parse2(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'

if __name__ == '__main__':
    pass
"""
bs = bs4.BeautifulSoup(s,'html.parser')
bs.find_all('tr')
e_trs = bs.find_all('tr')
[e_trs[0].get_text(), e_trs[1].get_text()]

"""