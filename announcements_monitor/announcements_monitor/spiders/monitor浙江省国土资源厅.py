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
import numpy
import requests
import scrapy
import announcements_monitor.items
import re
import datetime
import pandas as pd
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

key_dict = {
    '宗地坐落':'parcel_location',
    '宗地编号':'parcel_no',
    '宗地面积':'offer_area_m2',
    '容积率':'plot_ratio',
    '土地用途':'purpose',
    '起始价':'starting_price_sum',
    '地块编号':'parcel_no',
    '地块位置':'parcel_location',
    '成交价(万元)':'transaction_price_sum'
}

class Spider(scrapy.Spider):
    name = "511701"
    allowed_domains = ["www.zjdlr.gov.cn"]

    def start_requests(self):
        urls1 =  ["http://www.zjdlr.gov.cn/col/col1071192/index.html?uid=4228212&pageNum=%s" %i for i in xrange(6) if i > 0]
        urls2 =  []#["http://www.zjdlr.gov.cn/col/col1071194/index.html?uid=4228212&pageNum=%s" %i for i in xrange(6) if i > 0]
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
            yield response.meta['item']
            
        try:
            for site in sites:
                content_detail = {'addition':{}}
                data_frame = pd.read_html(str(site), encoding='gbk')[0] #1
                col_count = len(data_frame.columns)
                if col_count % 2 == 0:
                    # 一列标题，下一列为数据
                    # 先将数据线data frame数据转化为numpy数组，然后将数组reshape改成2列
                    arr = numpy.reshape(numpy.array(data_frame), (-1, 2))
                    # 去除key中的空格和冒号
                    data_dict = dict(arr)
                    data_dict = {re.sub(r'\s+|:|：', '', key): data_dict[key] for key in data_dict if key != 'nan'}
                    for key in data_dict:
                        if key in key_dict:
                            # key_dict[key]将中文键名改成英文的
                            content_detail[key_dict[key]] = data_dict[key]
                        else:
                            content_detail['addition'][key] = data_dict[key]

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error("%s（%s）中无法解析%s\n%s" % (self.name, response.url, item['monitor_title'], traceback.format_exc().decode('gbk')))
            yield response.meta['item']

    def parse2(self, response):
        """关键词：.*公示.*"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_no'] = re.search(r'(?<=\().*(?=\))', item['monitor_title']).group()

        site = bs_obj.find('table', style='border-collapse:collapse; border-color:#333333; font-size:12px;')
        if not site:
            log_obj.debug(u"%s(%s)没有检测到更多detail" % (self.name, response.url))
            yield response.meta['item']

        parcel_data = []
        try:
            data_frame = pd.read_html(str(site), encoding='gbk')[0] #2
            a = numpy.array(data_frame)
            title = a[0]
            data_list = a[1:]
            for i in xrange(1,len(a)):
                content_detail = {'addition': {}}
                # 将第一行标题跟每一列的数据组成一个字典
                d0 = dict(a[[0, i], :].T)
                for key in d0:
                    if key in key_dict:
                        # key_dict[key]将中文键名改成英文的
                        content_detail[key_dict[key]] = d0[key]
                    else:
                        content_detail['addition'][key] = d0[key]
                # 若有多行数据，则表示一个地块编号下有多块地，需要在取不同的名字
                # 另外，网页中表格内的地块编号不对，需要更改
                if len(a) == 2:
                    content_detail['parcel_no'] = item['parcel_no']
                else:
                    item['parcel_no'] = '%s(%s)' %(item['parcel_no'], content_detail['parcel_no'])
                    content_detail['parcel_no'] = item['parcel_no']

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error("%s（%s）中无法解析%s\n%s" % (self.name, response.url, item['monitor_title'], traceback.format_exc().decode('gbk')))
            yield response.meta['item']

if __name__ == '__main__':
    pass
