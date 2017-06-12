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
import traceback
import bs4
import pandas as pd
import scrapy
import announcements_monitor.items
import re
import datetime
import requests
import numpy as np
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

replacement = {u'地块 名称':'parcel_no',
               u'土地 用途':'purpose',
               u'土地 面积（㎡）':'offer_area_m2',
               u'容积率':'plot_ratio',
               u'土地座落':'parcel_location',
               u'建筑面积（㎡）':'building_area',
               u'起拍价（万元）':'starting_price_sum'}

class Spider(scrapy.Spider):
    name = "511706"
    allowed_domains = ["www.sxztb.gov.cn"]

    def start_requests(self):
        self.url1 = "http://www.sxztb.gov.cn/sxweb/tdjy/005006/005006004/005006004001/MoreInfo.aspx?CategoryNum=005006004001"
        self.url2 = "http://www.sxztb.gov.cn/sxweb/tdjy/005006/005006005/005006005001/MoreInfo.aspx?CategoryNum=005006005001"
        yield scrapy.Request(url=self.url1, callback=self.parse)
        yield scrapy.Request(url=self.url2, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        e_table = bs_obj.find('table', id='MoreInfoList1_DataGrid1')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        for e_tr in e_table.find_all('tr'):
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '绍兴'
            e_tds = e_tr.find_all('td')
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tds[1].get_text(strip=True) # 标题
                item['monitor_date'] = e_tds[2].get_text(strip=True) # 发布日期
                item['monitor_url'] = 'http://www.sxztb.gov.cn' + e_tds[1].a.get('href') # 链接

                if re.search(ur'绍兴市国土资源局国有建设用地使用权.*', item['monitor_title']):
                    item['monitor_re'] = '绍兴市国土资源局国有建设用地使用权.*'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                elif response.url == self.url2:
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse2, dont_filter=True)
                else:
                    yield item
            except:
                log_obj.debug("%s中存在无法解析的xpath：%s\n原因：%s" %(self.name, e_tds, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        e_table = bs_obj.find("table", class_="MsoNormalTable")
        if not e_table:
            log_obj.debug(u"%s(%s)没有检测到更多detail" %(self.name, response.url))
        df = ''
        try:
            df = pd.read_html(str(e_table), encoding='utf8')[0]
            df = pd.DataFrame(np.array(df.loc[1:,:]), columns=list(df.loc[0]))
            for i in xrange(len(df.index)):
                content_detail = {'addition':{}}
                d = dict(zip(df.columns, np.array(df.loc[i, :]).tolist()))
                for key in d:
                    if key in replacement:
                        content_detail[replacement[key]] = re.sub(ur'\s+', '', d[key])
                    else:
                        content_detail['addition'][re.sub(ur'\s+', '', key)] = re.sub(ur'\s+', '', d[key])
                if 'building_area' in content_detail:
                    content_detail['building_area'] = re.split(ur'～|-',content_detail['building_area'])[-1]
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error("%s（%s）中无法解析:\n%s\n%s" %(self.name, response.url, df, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
        e_page = bs_obj.find('td', attrs={'id':'TDContent', 'class':'infodetail'})
        try:
            e_table = e_page.find('table', class_='MsoNormalTable')
            e_trs = e_table.find_all('tr')[1:]
            for e_tr in e_trs:
                e_tds = e_tr.find_all('td')[1:]
                content_detail = {
                    'parcel_no':e_tds[0].get_text(strip=True),
                    'offer_area_m2':e_tds[1].get_text(strip=True),
                    'purpose':e_tds[3].get_text(strip=True),
                    'transaction_price_sum':e_tds[4].get_text(strip=True),
                    'competitive_person':e_tds[5].get_text(strip=True),
                    'addition':{'出让方式':e_tds[2].get_text(strip=True)}
                }
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error("%s（%s）中无法解析:\n%s\n%s" % (self.name, response.url, e_page, traceback.format_exc()))
            yield response.meta['item']



if __name__ == '__main__':
    pass

"""
	
ASP.NET_SessionId=nyfwwyjh2ul0et550monou45	
ASP.NET_SessionId=nyfwwyjh2ul0et550monou45
"""