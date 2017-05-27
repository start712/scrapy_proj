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
# 对应于数据库中字段名的标题
needed_item = ['parcel_no', 'offer_area_m2', 'purpose', 'plot_ratio', 'starting_price_sum', 'parcel_location',
               'starting_price', 'starting_price_sum', 'parcel_name', 'floor_starting_price']

# 地域名对应的列表中，有三空要填
# 第一个是网页表格中标题的行数
# 第二个是网页表格的标题内容，needed_item以外的标题寸存在addition中，
#       这里可以只写需要的前N个标题，后面代码里会按这里的标题顺序存储数据，此外的标题对应的数据不予爬取
# 第三个是之前N个标题中不需要爬取的标题，可以为空但是不能不填，为了防止有较大垃圾数据，必须要为列表
title_structure = {
    '挂牌出让公告':{
        '市局':[2, ['parcel_no', 'parcel_location', 'purpose', 'offer_area_m2', 'starting_price_sum',
                  '保证金（万元）', 'plot_ratio', '绿地率（%）', '建筑密度（%）', '建筑限高（米）'], []],
        '奉化':[1, ['parcel_no', 'offer_area_m2', 'purpose', '出让年限（年）', 'plot_ratio',
                  '建筑密度', '绿地率', '投资强度（≥万元／公顷）', 'starting_price_sum', '保证金（万元）'],[]],
        '余姚':[2, ['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', '出让年限（年）',
                  'plot_ratio', '建筑密度', '绿地率', 'starting_price', 'starting_price_sum', '竞买保证金(万元）'], []],
        '江北':[1, ['parcel_no', 'parcel_name', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio',
                  '建筑密度(≤)', '绿地率', '建筑高度(≤)', '出让年限', '竞买保证金(万元)', '出让起始价(万元/亩)'], []],
        '慈溪':[2, ['序号', 'parcel_no', 'parcel_location', 'purpose', 'offer_area_m2', '竞买保证金（万元）',
                  'starting_price_sum', 'plot_ratio', '建筑密度（%）', '绿地率（%）', '建筑限高（米）'], ['序号',]],
        '北仑':[2, ['序号', 'parcel_no', 'parcel_location', 'purpose', 'offer_area_m2', 'plot_ratio',
                  '绿地率（%）', '建筑密度（%）', '建筑高度(米)', '出让年限（年）', '保证金（万元）', 'floor_starting_price'], ['序号',]],
        '象山':[0], # 表示需要单独设置一个解析方法
        '鄞州':[1, ['parcel_no', 'parcel_name', 'offer_area_m2', 'purpose', '建筑密度', 'plot_ratio', '绿地率',
                  '建筑高度', '出让年限（年）', '竞买保证金(万元)', 'floor_starting_price'], ['序号',]]
        '保税区':[2, ['序号', 'parcel_name', 'offer_area_m2', 'purpose', 'purpose', '绿地率', '建筑系数',
                   '限高（m）', '出让年限', '投资强度(万元/亩）', '竞买保证金（万元）', 'starting_price_sum'], []],
        '镇海':[1, ['parcel_name', 'parcel_location', 'offer_area_m2', 'purpose', '出让年限', '建筑密度',
                  'plot_ratio', '建筑高度', '绿地率'], []]
        '宁海':[0] # 表示需要单独设置一个解析方法
              },
    '出让结果公告':{}
}


class Spider(scrapy.Spider):
    name = "511697"
    allowed_domains = ["www.yhland.gov.cn"]

    def start_requests(self):
        # http://www.nblr.gov.cn/showpage2/pubchief.jsp?type=tdcrgg
        self.urls1 =  ["http://www.nblr.gov.cn/show3.do?method=getSomeInfo_list&name=kyqgpcrgg&page=%s" % i for i in xrange(5) if i > 0]
        self.urls2 =  ["http://www.nblr.gov.cn/show3.do?method=getSomeInfo_list&name=kyqgpcrgg&page=%s" % i for i in xrange(5) if i > 0]

        for url in self.urls1 + self.urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        sites = bs_obj.find_all('table', id="table85")
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""

        for site0 in sites:
            site = site0.find_all('td')
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '宁波'
            try:
                item['monitor_id'] = self.name
                area = site[1].find('a', style='color: #615b97').get('title')
                item['monitor_title'] = site[1].find('a', target='_blank').get('title')
                item['monitor_date'] = site[2].get_text(strip=True)
                item['monitor_url'] = 'http://122.224.205.74:33898/ProArticle/' + site[1].find('a', target='_blank').get('href') # 链接
                item["monitor_content"] = area

                if response.url in self.urls1:
                    item['parcel_status'] = 'sold'
                elif response.url in self.urls2:
                    item['parcel_status'] = 'sold'
                else:
                    yield item
                yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse0, dont_filter=True)
            except:
                log_obj.debug("%s中存在无法解析的xpath：%s\n原因：%s" %(self.name, site, traceback.format_exc()))


    def parse0(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find("table", style="HEIGHT: 459px; WIDTH: 862px; BORDER-COLLAPSE: collapse").find_all('tr')
        if not sites:
            log_obj.debug(u"%s(%s)没有检测到更多detail" %(self.name, response.url))
        else:
            sites = sites[1:]

        for i in xrange(len(sites)):
            site = sites[i].find_all('td')
            try:
                content_detail =\
                            {'parcel_no': site[0].get_text(strip=True),
                             'offer_area_m2': site[1].get_text(strip=True),
                             'purpose': site[2].get_text(strip=True),
                             'plot_ratio': site[4].get_text(strip=True),
                             'starting_price_sum': site[9].get_text(strip=True),
                             'addition': {u'出让年限（年）': site[3].get_text(strip=True),
                                          u'建筑密度': site[5].get_text(strip=True),
                                          u'绿地率': site[6].get_text(strip=True),
                                          u'保证金（万元）': site[10].get_text(strip=True)}
                            }
                if '≤' in content_detail['plot_ratio']:
                    content_detail['plot_ratio'] = str(content_detail['plot_ratio']).split('-')[-1]

                item['content_detail'] = content_detail
                yield item
            except:
                log_obj.error("%s（%s）中无法解析%s\n%s" %(self.name, response.url, site, traceback.format_exc()))
                yield response.meta['item']

if __name__ == '__main__':
    pass