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

import bs4
import requests
import scrapy
import announcements_monitor.items
import re
import datetime
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



class Spider(scrapy.Spider):
    name = "511701"
    allowed_domains = ["www.zjdlr.gov.cn"]

    def start_requests(self):
        urls1 =  ["http://www.zjdlr.gov.cn/col/col1071192/index.html?uid=4228212&pageNum=%s" %i for i in xrange(6) if i > 0]
        urls2 =  ["http://www.zjdlr.gov.cn/col/col1071194/index.html?uid=4228212&pageNum=%s" %i for i in xrange(6) if i > 0]
        for url in urls1 + urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """结构不同，使用正则表达式直接读取"""
        #log_obj.debug(u"准备分析内容：%s" %response.url)
        root_site = "http://www.zjdlr.gov.cn"
        items = []
        rows = re.findall(r"(?<=<record><!\[CDATA\[).*?(?=</record>)", response.text, re.S)

        for row in rows:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            if row:
                try:
                    item['monitor_id'] = self.name
                    item['monitor_title'] = re.search(r"(?<=title=').*?(?=' target=)", row).group(0) # 出让公告标题
                    item['monitor_date'] = re.search(r'(?<=class="bt_time" style="font-size:16px;border-bottom:dashed 1px #ccc">).*?(?=</td>)', row).group(0) # 发布日期
                    item['monitor_url'] = root_site + re.search(r"(?<=href=').*?(?=' class)", row).group(0) # 链接
                    item['monitor_content'] = ""

                except:
                    info = sys.exc_info()
                    log_obj.debug(u"%s中存在无法解析的xpath：%s\n原因：%s%s%s" %(self.name, row, info[0], ":", info[1]))

                #csv_report.output_data(items, "result", method = 'a')
                if re.search(r'.*公告.*', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'.*公告.*'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=False)
                elif re.search(r'.*公示.*', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'.*公示.*'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse2, dont_filter=False)
                else:
                    yield item

    def parse1(self, response):
        """关键词：.*公告.*"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        parcel_data = []
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find_all('table', style='border-collapse:collapse; border-color:#333333;font-size:12px;')
        if not sites:
            log_obj.debug(u"%s(%s)没有检测到更多detail" %(self.name, response.url))

        try:
            for i in xrange(len(sites)):
                site = sites[i]  # 一个对应网页中一个地块信息的表格
                table0 = [tr0.find_all('td') for tr0 in site.find_all('tr')] #把每个tr中的所有td放入一个列表内
                table0 = [s.get_text(strip=True) for l in table0 for s in l if len(l)%2==0] #先将所有一行中偶数个单元格的数据放在一个列表
                data_dict = {table0[i]:table0[i+1] for i in xrange(len(table0)) if i%2==0} # 将所有数据的标题与之对应写成字典
                #for key in data_dict:
                #    print key, ':', data_dict[key]
                parcel_data.append(data_dict)

            item['content_detail'] = {'parcel_no': str(item['monitor_title']),
                                      'addition': {'所有数据':parcel_data}}

            yield item
        except:
            info = sys.exc_info()
            log_obj.error(u"%s（%s）中无法解析%s\n原因：%s%s%s" % (self.name, response.url, item['monitor_title'], info[0], ":", info[1]))
            yield response.meta['item']

    def parse2(self, response):
        """关键词：.*公示.*"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        parcel_data = []
        #item['content_html'] = bs_obj.prettify()
        try:
            sites = bs_obj.find('table', style='border-collapse:collapse; border-color:#333333; font-size:12px;').find_all('tr')
            sites = [site.find_all('td') for site in sites]  # [@id="list"] [@class="padding10"][position()>1]
            title = sites.pop(0)

            if not sites:
                log_obj.debug(u"%s(%s)没有检测到更多detail" % (self.name, response.url))
        except:
            log_obj.debug(u"问题网页：%s(%s)" % (self.name, response.url))

        parcel_data = []
        try:
            for i in xrange(len(sites)):
                site = sites[i]
                data_dict = {}
                #print "!!!!!!!!", len(title), '  ', len(site)
                for j in xrange(len(site)):
                    if len(title) == len(site):
                        data_dict[title[j].get_text()] = site[j].get_text()
                    else:
                        # 偶尔会有不规整的表格
                        if '额外数据' not in data_dict:
                            data_dict['额外数据'] = []
                        data_dict['额外数据'].append(site[j].get_text())

                parcel_data.append(data_dict)
            item['parcel_no'] = item['monitor_title']
            item['content_detail'] = {'addition':{'所有数据':parcel_data}}
        except:
            info = sys.exc_info()
            log_obj.error(u"%s（%s）中无法解析%s\n原因：%s%s%s" % (self.name, response.url, item['monitor_title'], info[0], ":", info[1]))
            yield response.meta['item']

if __name__ == '__main__':
    pass