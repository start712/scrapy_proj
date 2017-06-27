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

needed_data = ['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', 'starting_price_sum']
title_type1 = [[u'\n\n编号\n\n\n土地位置\n\n\n土地\n面积 （㎡）\n\n\n用途\n\n\n规划指标\n\n\n出让年限（年）\n\n\n挂牌起始价\n（元/㎡）\n\n\n保证金\n（万元）\n\n',
               u'\n\n容积率\n\n\n建筑密度\n\n\n绿地率\n\n'],
               ['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度',
                '绿地率', '出让年限', 'starting_price_sum', '保证金']]
title_type2 = [[u'\n\n编号\n\n\n土地位置\n\n\n土地\n面积 (m2)\n\n\n用途\n\n\n规划指标\n\n\n出让年限年\n\n\n挂牌起始价(元/M2)\n\n\n保证金(*屏蔽的关键字*万元)\n\n',
               u'\n\n容积率\n\n\n建筑密度\n\n'],
               ['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度',
                '出让年限', 'starting_price_sum', '保证金']]
title_type3 = [[u'\n\n编号\n\n\n界址（空间范围）\n\n\n土地\n面积 (m2)\n\n\n用途\n\n\n规划指标\n\n\n出让年 限\n\n\n挂牌起始价(元/M2)\n\n\n\xa0\n竞买保证金(万元)\n\n',
               u'\n\n容积率\n\n\n建筑密度\n\n'],
               ['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度',
                '出让年限', 'starting_price_sum', '保证金']]

title_type4 = [[u'\n\n编号\n\n\n土地位置\n\n\n土地\n面积 (m2)\n\n\n用途\n\n\n规划指标\n\n\n出让年限年\n\n\n挂牌起始价(元/M2)\n\n\n保证金(人民币万元)\n\n',
                 u'\n\n容积率\n\n\n建筑密度\n\n'],
                ['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度',
                '出让年限', 'starting_price_sum', '保证金']]


class Spider(scrapy.Spider):
    name = "511708"

    def start_requests(self):
        # 嘉兴相应网址的index的系数，index_1代表第二页
        self.urls1 = ["http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdzpgxxgg/index.html", ] + ["http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdzpgxxgg/index_%s.html" %i for i in xrange(3) if i > 0]
        self.urls2 = ["http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdcrjggs/index.html", ] + ["http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdcrjggs/index_%s.html" % i for i in xrange(3) if i > 0]

        for url in self.urls1 + self.urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
            导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        e_row = bs_obj.find_all('table', style='line-height:20pt;border-bottom:1px dashed #b0b6de')
        for e_tr in e_row:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '嘉兴'
            e_tds = e_tr.find_all('td')
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tds[0].get_text(strip=True) # 标题
                item['monitor_date'] = e_tds[1].get_text(strip=True) # 成交日期 site.xpath('td[3]/text()').extract_first()
                if response.url in self.urls1:
                    item['monitor_url'] = "http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdzpgxxgg" + re.sub(ur'\./', '/',e_tds[0].a.get('href'))
                    yield scrapy.Request(item['monitor_url'],meta={'item':item},callback=self.parse1, dont_filter=True)
                elif response.url in self.urls2:
                    item['monitor_url'] = "http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdcrjggs" + re.sub(ur'\./', '/',e_tds[0].a.get('href'))  # 链接
                    yield scrapy.Request(item['monitor_url'],meta={'item':item},callback=self.parse2, dont_filter=True)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        try:
            e_table = bs_obj.find('div', align='center')
            if not e_table:
                e_table = bs_obj.find('table', attrs={'cellspacing':'0', 'cellpadding':'0', 'border':'1'})
            e_trs = e_table.find_all('tr')[2:]
            for e_tr in e_trs:
                title = [row.get_text() for row in e_table.find_all('tr')[0:2]]
                if title == title_type1[0]:
                    title = title_type1[1]
                elif title == title_type2[0]:
                    title = title_type2[1]
                elif title == title_type3[0]:
                    title = title_type3[1]
                elif title == title_type4[0]:
                    title = title_type4[1]
                else:
                    raise

                e_tds = e_tr.find_all('td')
                row = [e_td.get_text(strip=True) for e_td in e_tds]

                # 个别行提供的信息没有用，需要过滤掉
                if len(row) < 3:
                    continue

                detail = dict(zip(title,row))
                content_detail = {'addition':{}}
                for key in detail:
                    if key in needed_data:
                        content_detail[key] = detail[key]
                    else:
                        content_detail['addition'][key] = detail[key]

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
        try:
            e_table = bs_obj.find('table', class_='MsoNormalTable')
            e_trs = e_table.find_all('tr')
            for e_tr in e_trs:
                row = [e_td.get_text(strip=True) for e_td in e_tr.find_all('td')]
                # 去掉不必要的数据
                if len(row) < 3 or row[0] == u'公告号':
                    continue

                if re.search(ur'嘉土南.*|嘉土告字经开.*', item['monitor_title']):
                    item['monitor_re'] = ur'嘉土南.*|嘉土告字经开.*'
                    if len(row) == 10:
                        parcel_no = row[0]
                        row = row[1:]
                    content_detail = {
                        'parcel_name':row[0],
                        'offer_area_m2':row[1],
                        'purpose':row[2],
                        'competitive_person':row[3],
                        'parcel_location':row[4],
                        'starting_price':row[6],
                        'transaction_price':row[7],
                        'transaction_price_sum':row[8],
                        'addition':{'出让年限':row[5],
                                    }
                    }

                elif re.search(ur'嘉土秀洲.*', item['monitor_title']):
                    m = re.search(ur'嘉土秀洲.+?号', item['monitor_title'])
                    item['monitor_re'] = ur'嘉土秀洲.+?号'
                    if m:
                        parcel_no = m.group()
                    content_detail = {
                        'parcel_name': row[0],
                        'competitive_person': row[1],
                        'parcel_location': row[2],
                        'offer_area_m2': row[3],
                        'transaction_price_sum': row[4],
                        'starting_price': row[5],
                        'purpose':row[8],
                        'addition':{'出让年限':row[6],
                                    '供地方式':row[9]
                                    }
                    }

                content_detail['parcel_no'] = parcel_no
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass