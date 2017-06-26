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

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########

log_obj = spider_log.spider_log() #########

key_dict = {
    u'宗地坐落':'parcel_location',
    u'宗地编号':'parcel_no',
    u'宗地面积':'offer_area_m2',
    u'容积率':'plot_ratio',
    u'土地用途':'purpose',
    u'起始价':'starting_price_sum',
    u'地块编号':'parcel_no',
    u'地块位置':'parcel_location',
    u'成交价(万元)':'transaction_price_sum'
}

class Spider(scrapy.Spider):
    name = "511701"
    allowed_domains = ["www.zjdlr.gov.cn"]

    def start_requests(self):
        urls1 =  ["http://www.zjdlr.gov.cn/col/col1071192/index.html?uid=4228212&pageNum=%s" %i for i in xrange(9) if i > 0]
        urls2 =  ["http://www.zjdlr.gov.cn/col/col1071194/index.html?uid=4228212&pageNum=%s" %i for i in xrange(9) if i > 0]
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
            item['monitor_city'] = '浙江'
            if row:
                try:
                    item['monitor_id'] = self.name
                    item['monitor_title'] = re.search(r"(?<=title=').*?(?=' target=)", row).group(0) # 出让公告标题
                    item['monitor_date'] = re.search(r'(?<=class="bt_time" style="font-size:16px;border-bottom:dashed 1px #ccc">).*?(?=</td>)', row).group(0) # 发布日期
                    item['monitor_url'] = root_site + re.search(r"(?<=href=').*?(?=' class)", row).group(0) # 链接
                    item['monitor_content'] = ""

                    if re.search(r'.*公告.*', item['monitor_title'].encode('utf8')):
                        item['monitor_re'] = r'.*公告.*'
                        yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1,
                                             dont_filter=False)
                    elif re.search(r'.*公示.*', item['monitor_title'].encode('utf8')):
                        item['monitor_re'] = r'.*公示.*'
                        yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse2,
                                             dont_filter=False)
                    else:
                        yield item
                except:
                    log_obj.update_error("%s中无法解析%s\n原因：%s" % (self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        """关键词：.*公告.*"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        parcel_data = []
        item['parcel_status'] = 'onsell'
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find_all('table', style='border-collapse:collapse; border-color:#333333;font-size:12px;')
            
        try:
            for site in sites:
                item['parcel_no'] = re.search(r'(?<=\().*(?=\))', item['monitor_title']).group()
                content_detail = {'addition':{}}
                
                if not site:
                    log_obj.update_debug(u"%s(%s)没有检测到更多detail" %(self.name, response.url))
                    
                data_frame = pd.read_html(str(site), encoding='utf8')[0] #1
                data_frame = data_frame.fillna('') # 替换缺失值
                col_count = len(data_frame.columns)
                if col_count % 2 == 0:
                    # 一列标题，下一列为数据
                    # 先将数据线data frame数据转化为numpy数组，然后将数组reshape改成2列
                    arr = numpy.reshape(numpy.array(data_frame), (-1, 2))
                    # 去除key中的空格和冒号
                    data_dict = dict(arr)
                    r = re.compile(ur'\s+|:|：')
                    data_dict = {r.sub('', key): data_dict[key] for key in data_dict if (type(key) == type(u'') or type(key) == type('')) and key != 'nan' }
                    for key in data_dict:
                        if key in key_dict:
                            # key_dict[key]将中文键名改成英文的
                            content_detail[key_dict[key]] = data_dict[key]
                        else:
                            content_detail['addition'][key] = data_dict[key]

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc().decode('gbk').encode('utf8')))
            yield response.meta['item']

    def parse2(self, response):
        """关键词：.*公示.*"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
        site = bs_obj.find('table', style='border-collapse:collapse; border-color:#333333; font-size:12px;')
        parcel_data = []
        try:
            data_frame = pd.read_html(str(site), encoding='utf8')[0] #2
            data_frame = data_frame.fillna('')  # 替换缺失值
            arr = numpy.array(data_frame)
            for i in xrange(1,len(arr)):
                # 从标题中取出地块编号
                m = re.search(r'(?<=\().*(?=\))', item['monitor_title'])
                if m:
                    item['parcel_no'] = m.group()
                    
                content_detail = {'addition': {}}
                # 将第一行标题跟每一列的数据组成一个字典
                d0 = dict(arr[[0, i], :].T)
                for key in d0:
                    if key in key_dict:
                        # key_dict[key]将中文键名改成英文的
                        content_detail[key_dict[key]] = d0[key]
                    else:
                        content_detail['addition'][key] = d0[key]
                # 若有多行数据，则表示一个地块编号下有多块地，需要在取不同的名字
                # 另外，网页中表格内的地块编号不对，需要更改
                if len(arr) == 2:
                    content_detail['parcel_no'] = item['parcel_no']
                else:
                    if 'parcel_no' not in content_detail:
                        content_detail['parcel_no'] = '表格中无地块编号'
                        
                    item['parcel_no'] = '%s(%s)' %(item['parcel_no'], content_detail['parcel_no'])
                    content_detail['parcel_no'] = item['parcel_no']

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc().decode('gbk').encode('utf8')))
            yield response.meta['item']

if __name__ == '__main__':
    pass
