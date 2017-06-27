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

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########

log_obj = spider_log.spider_log() #########

replacement = {u'地块 名称':'parcel_no',
               u'土地 用途':'purpose',
               u'土地 面积（㎡）':'offer_area_m2',
               u'容积率':'plot_ratio',
               u'土地座落':'parcel_location',
               u'建筑面积（㎡）':'building_area',
               u'起拍价（万元）':'starting_price_sum'}

class Spider(scrapy.Spider):
    name = "511707"
    allowed_domains = ["www.sxztb.gov.cn"]

    def start_requests(self):
        self.url1 = "http://www.huzgt.gov.cn/GTInfoMoreList.aspx?ModuleID=203&PageID=3"
        self.url2 = "http://www.huzgt.gov.cn/GTInfoMoreList.aspx?ModuleID=202&PageID=3"
        yield scrapy.Request(url=self.url1, callback=self.parse)
        yield scrapy.Request(url=self.url2, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        e_table = bs_obj.find('div', class_='list2')
        for e_p in e_table.find_all('p'):
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '湖州'
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_p.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_p.span.get_text(strip=True) # 发布日期
                item['monitor_url'] = 'http://www.huzgt.gov.cn/' + e_p.a.get('href') # 链接

                if response.url == self.url1 and re.search(ur'.*拍卖公告.*', item['monitor_title']):
                    item['monitor_re'] = '.*拍卖公告.*'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                elif response.url == self.url1 and re.search(ur'.*拍卖变更公告.*', item['monitor_title']):
                    item['monitor_re'] = '.*拍卖变更公告.*'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse2, dont_filter=True)
                #elif response.url == self.url2:
                #    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse3, dont_filter=True)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        e_table = bs_obj.table
        while e_table.table:
            e_table = e_table.table

        try:
            for e_tr in e_table.find_all('tr')[2:]:
                e_tds = e_tr.find_all('td')
                content_detail = {
                    'parcel_no':e_tds[0].get_text(strip=True),
                    'parcel_location':e_tds[1].get_text(strip=True),
                    'purpose': e_tds[2].get_text(strip=True),
                    'offer_area_m2':e_tds[3].get_text(strip=True),
                    'plot_ratio':e_tds[4].get_text(strip=True),
                    'starting_price_sum':e_tds[10].get_text(strip=True),
                    'addition':{
                        '建筑密度(%)':e_tds[5].get_text(strip=True),
                        '绿地率(%)':e_tds[6].get_text(strip=True),
                        '投资强度(≥万元/亩)':e_tds[7].get_text(strip=True),
                        '投产初始运行期满亩均产值(≥万元/亩)':e_tds[8].get_text(strip=True),
                        '投产初始运行期满亩均税收(≥万元/亩)':e_tds[9].get_text(strip=True),
                        '保证金(万元)':e_tds[11].get_text(strip=True)
                    }
                }
                content_detail['plot_ratio'] = re.split(r'-', content_detail['plot_ratio'])[-1]
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析:\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'update'
        yield item

    """
    def parse3(self, response):
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
    """


if __name__ == '__main__':
    pass