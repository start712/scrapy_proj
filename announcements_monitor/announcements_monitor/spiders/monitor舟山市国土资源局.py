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

with open(os.getcwd() + r'\announcements_monitor\spiders\needed_data.txt', 'r') as f:
    s = f.read()
    needed_data = s.split(',')
needed_data = [s.encode('utf8') for s in needed_data]

title_match = {
    u'地块编号': 'parcel_no',
    u'地块位置': 'parcel_location',
    u'土地面积': 'offer_area_m2',
    u'面积': 'offer_area_m2',
    u'土地用途': 'purpose',
    u'最大容积率': 'plot_ratio',
    u'土地竞得人': 'competitive_person',
    u'成交价': 'transaction_price_sum',
    u'受让单位': 'competitive_person',
    u'宗地编号': 'parcel_no',
    u'宗地面积': 'offer_area_m2',
    u'宗地坐落': 'parcel_location',
    u'容积率': 'plot_ratio',
    u'起始价': 'starting_price_sum',
    u'编号': 'parcel_no',
    '编号': 'parcel_no',
}
title_type = {
    14:['编号', 'parcel_location', '土地总面积', 'offer_area_m2', '退让面积', 'purpose', 'starting_price_sum',
        'plot_ratio', '建筑密度', '绿地率', '出让年限', '竞买保证金', '开工保证金', '竣工保证金'],
    16:['编号', 'parcel_location', '土地总面积', 'offer_area_m2', '退让面积', 'purpose', 'starting_price_sum',
        'plot_ratio', '建筑密度', '绿地率', '出让年限', '投资总额', '竞买保证金', '开工保证金', '竣工保证金', '履约保证金']
}
title_height = {14:2, 16:2}

class Spider(scrapy.Spider):
    name = "511713"

    def start_requests(self):
        # 舟山相应网址的index的系数，index_2代表第二页
        self.urls1 = ["http://www.zsblr.gov.cn/mlx/tdsc/tdzpgxxgg/index.html",] + ["http://www.zsblr.gov.cn/mlx/tdsc/tdzpgxxgg/index_%s.html" %i for i in xrange(3) if i > 1]
        self.urls2 = ["http://www.zsblr.gov.cn/mlx/tdsc/tdcrjg/index.html",] + ["http://www.zsblr.gov.cn/mlx/tdsc/tdcrjg/index_%s.html" %i for i in xrange(3) if i > 1]

        for url in self.urls1 + self.urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
            导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        #e_table = bs_obj.find('div', class_='txtlist')
        e_table = bs_obj.find('div', id='mlx_list')
        e_row = e_table.find_all('li')
        for e_tr in e_row:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '舟山'
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tr.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_tr.span.get_text(strip=True) # 成交日期
                item['monitor_url'] = "http://www.zsblr.gov.cn" + e_tr.a.get('href')

                if response.url in self.urls1 and not re.search(ur'海域', item['monitor_title']):
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                elif response.url in self.urls2:
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse2, dont_filter=True)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        try:
            # 在整页范围内找地块编号
            m = re.search(ur'.土[^土]+?号', bs_obj.get_text())
            parcel_no = ''
            if m:
                parcel_no = m.group()

            e_table = bs_obj.table
            while e_table.table:
                e_table = e_table.table

            e_trs = e_table.find_all('tr')
            title_row = e_trs[0].find_all('td')
            # 有6列的表格标题列不在前两行
            if len(title_row) == 6:
                df = pd.read_html(e_table.prettify(encoding='utf8'), encoding='utf8')[0]
                df = df.fillna('')  # 替换缺失值
                arr = np.reshape(np.array(df),(-1,2))
                # 去除key中的空格和冒号
                data_dict = dict(arr)
                r = re.compile(ur'\s+|:|：')
                data_dict = {r.sub('', key): data_dict[key] for key in data_dict if
                             (type(key) == type(u'') or type(key) == type('')) and key != 'nan'}

                content_detail = {'addition': {}}
                for key in data_dict:
                    if key in title_match:
                        content_detail[title_match[key]] = data_dict[key]
                    else:
                        content_detail['addition'][key] = data_dict[key]
                if parcel_no:
                    content_detail['addition']['地块编号（副）'] = content_detail['parcel_no']
                    content_detail['parcel_no'] = parcel_no

                item['content_detail'] = content_detail
                yield item
            else:
                test_row = e_trs[-1].find_all('td')
                if len(test_row) not in title_type:
                    log_obj.error(item['monitor_url'], "%s（%s）中存在不规则数据，标题列数为%s，数据列数为%s\n"
                                         "标题列：%s\n数据列：%s" % (self.name, response.url, len(title_row),
                                                            len(test_row), ','.join([e_td.get_text(strip=True) for e_td in title_row]),
                                                            ','.join([e_td.get_text(strip=True) for e_td in test_row])))
                    yield response.meta['item']
                else:
                    title = title_type[len(test_row)]
                    e_trs = e_trs[title_height[len(test_row)]:]
                    for e_tr in e_trs:
                        e_tds = e_tr.find_all('td')
                        row = [e_td.get_text(strip=True) for e_td in e_tds]

                        detail = dict(zip(title, row))
                        content_detail = {'addition': {}}
                        for key in detail:
                            if key in needed_data:
                                content_detail[key] = detail[key]
                            else:
                                content_detail['addition'][key] = detail[key]

                        item['content_detail'] = content_detail
                        yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
        try:
            e_page = bs_obj.find('div', class_='acontent')

            # 处理网页文字
            s_page = e_page.get_text() + item['monitor_title']
            m = re.search(ur'.土公示.+?号', s_page)
            if m:
                parcel_no = m.group()
            else:
                parcel_no = ''

            # 处理网页中的表格
            e_table = e_page.table
            while e_table.table:
                e_table = e_table.table
            e_trs = e_table.find_all('tr')
            title_row = e_trs[0].find_all('td')
            # 标题行必然大于2列
            while len(title_row) < 3 or re.search(ur'单位:.+、.+、.+', ''.join([e_td.get_text(strip=True) for e_td in title_row])):
                e_trs = e_trs[1:]
                title_row = e_trs[0].find_all('td')

            title_addition = []
            title_new = []
            # 去除标题中包含括号的字段名，免得妨碍之后的字段名匹配
            for e_td in title_row:
                s = e_td.get_text(strip=True)
                r = re.compile(ur'\(.+?\)|（.+?）')
                m = r.search(s)
                if m:
                    s = r.sub('', s)
                    title_addition.append(u'%s:%s' %(s, m.group()))
                title_new.append(s)

            # 处理标题以外的数据
            for e_tr in e_trs[1:]:
                e_tds = e_tr.find_all('td')
                row = [e_td.get_text(strip=True) for e_td in e_tds]

                if len(title_new) != len(row):
                    if len(row) > 2 and ''.join(row) and not re.search(ur'舟山市国土资源局.+分局|^\d+年\d+月\d+日',''.join(row)):
                        log_obj.error(item['monitor_url'], "%s（%s）中存在不规则数据，标题列数为%s，数据列数为%s\n标题列：%s\n数据列：%s" % (self.name,
                                      response.url, len(title_new), len(row), ','.join(title_new), ','.join(row)))
                        yield response.meta['item']
                else:
                    detail = dict(zip(title_new,row))

                    content_detail = {'addition':{}}
                    for key0 in detail:
                        key = re.sub(r'\s+', '', key0)
                        if key in title_match:
                            content_detail[title_match[key]] = detail[key0]
                        else:
                            content_detail['addition'][key] = detail[key0]

                    # 规范输出的地块编号
                    if parcel_no:
                        if 'parcel_no' in content_detail:
                            content_detail['addition']['地块编号（副）'] = content_detail['parcel_no']

                        content_detail['parcel_no'] = parcel_no

                    """
                    if 'parcel_no' in content_detail:
                        m0 = re.search(ur'.土公示.+?号', content_detail['parcel_no'])
                        if not m0:
                            content_detail['addition']['地块编号（副）'] = content_detail['parcel_no']
                            content_detail['parcel_no'] = ''
                    """

                    item['content_detail'] = content_detail
                    yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass

"""
bs = bs4.BeautifulSoup(s,'html.parser')
e_table = bs.table
while e_table.table:
    e_table = e_table.table
"""