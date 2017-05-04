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

class Spider(scrapy.Spider):
    name = "511703" #出让信息
    allowed_domains = ["www.hzgtj.gov.cn"]

    def start_requests(self):
        urls = ["http://www.hzgtj.gov.cn/fore/portal/infos/toList?parentIdStr=1-32-6686-&id=6686",]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = scrapy.Selector(response)
        root_path = '/html/body/div[4]/div[2]/form/div[3]/table/tbody/tr/td/table[1]/tbody/tr'
        sites = sel.xpath(root_path)  # [@id="list"] [@class="padding10"][position()>1]
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        if not sites:
            sites = sel.xpath(root_path.replace("/tbody",""))

        for site in sites:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = re.sub(r"[ \s]", "", site.xpath('td[2]/a/@title').extract_first()) # 标题
                item['monitor_date'] = re.sub(r"[ \s]", "", site.xpath('td[3]/text()').extract_first()) # 成交日期 site.xpath('td[3]/text()').extract_first()
                item['monitor_url'] = "http://www.hzgtj.gov.cn" + site.xpath('td[2]/a/@href').extract_first() # 链接

            except:
                info = sys.exc_info()
                log_obj.debug(u"%s中无法解析%s\n原因：%s%s%s" %(self.name, site, info[0], ":", info[1]))

            #log_obj.debug("!!!!item['monitor_title']:%s" % type(item['monitor_title']))
            if re.match(r'杭政工出.*', item['monitor_title'].encode('utf8')):
                item['monitor_re'] = r'杭政工出.*'
                yield scrapy.Request(url=item['monitor_url'],meta={'item':item},callback=self.parse1, dont_filter=False)
            elif re.match(r'杭政储出.*', item['monitor_title'].encode('utf8')):
                item['monitor_re'] = r'杭政储出.*'
                yield scrapy.Request(item['monitor_url'],meta={'item':item},callback=self.parse2, dont_filter=False)
            else:
                yield item

    def parse1(self, response):
        """关键词：杭政工出"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find_all('tr')
        sites = [site.find_all('td') for site in sites if site.b == None and sites.index(site)>=1]  # [@id="list"] [@class="padding10"][position()>1]
        if not sites:
            log_obj.debug(u"%s(%s)没有检测到更多detail" %(self.name, response.url))

        #log_obj.debug(u"sites: %s" %str([site[0].get_text(strip=True) for site in sites]))
        for i in xrange(len(sites)):
            site = sites[i]
            try:
                content_detail =\
                            {'parcel_no': site[0].get_text(strip=True),
                            'parcel_location': site[1].get_text(strip=True),
                            'offer_area_m2': site[2].get_text(strip=True),
                            'purpose': site[3].get_text(strip=True),
                            'plot_ratio': site[5].get_text(strip=True).split('-')[-1],
                            'starting_price_sum': site[6].get_text(strip=True),
                            'fixture_date': item['monitor_date'],
                            'addition':{u'用途':[site[2].get_text(strip=True),site[3].get_text(strip=True)]}
                            }

                # 有一些条目有合并单元格的情况,一般集中于杭政储出
                if site[0].has_attr('rowspan'):
                    length = site[0]['rowspan'] - 1
                    # 填入合并的单元格的第一行数据
                    content_detail['addition'] = {u'用途':[site[0].get_text(strip=True),site[1].get_text(strip=True)]}
                    for j in xrange(length):
                        i += 1 # 从合并的单元格的第二行开始增加数据
                        site = sites[i]
                        content_detail['offer_area_m2'] += site[0].get_text(strip=True)
                        content_detail['addition'][u'用途'].append([site[0].get_text(strip=True),site[1].get_text(strip=True)])

                item['content_detail'] = content_detail
                yield item
            except:
                log_obj.error("%s（%s）中无法解析%s\n%s" %(self.name, response.url, site, traceback.format_exc()))
                yield response.meta['item']

    def parse2(self, response):
        """关键词：杭政储出"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find_all('tr')
        sites = [site.find_all('td') for site in sites if site.b == None and sites.index(site)>=1]  # [@id="list"] [@class="padding10"][position()>1]
        if not sites:
            log_obj.debug(u"%s(%s)没有检测到更多detail" %(self.name, response.url))

        for i in xrange(len(sites)):
            site = sites[i]
            try:
                content_detail =\
                            {'parcel_no': site[0].get_text(strip=True),
                            'parcel_location': site[1].get_text(strip=True),
                            'offer_area_m2': site[2].get_text(strip=True),
                            'purpose': site[3].get_text(strip=True),
                            'building_area':site[4].get_text(strip=True),
                            'fixture_date': item['monitor_date']
                            }

                # 建筑面积有时会杂有文字
                if '地上' in content_detail['building_area']:
                    content_detail['building_area'] = re.search(r'(?<=地上建筑面积)\d+(?=平方米)|(?<=地上)\d+(?=平方米)', site[4].get_text(strip=True)).group(0)
                    content_detail['addition'] = {u'建筑面积（M2）': site[4].get_text(strip=True)}
                elif '地下' in content_detail['building_area']:
                    content_detail['building_area'] =''
                    content_detail['addition'] = {u'建筑面积（M2）':site[4].get_text(strip=True)}

                # 有一些条目有合并单元格的情况
                if site[0].has_attr('rowspan'):
                    length = site[0]['rowspan'] - 1
                    # 填入合并的单元格的第一行数据
                    content_detail['addition'] = {u'建筑面积（M2）':[site[0].get_text(strip=True),site[1].get_text(strip=True)]}
                    for j in xrange(length):
                        i += 1 # 从合并的单元格的第二行开始增加数据
                        site = sites[i]
                        content_detail['offer_area_m2'] += site[0].get_text(strip=True)
                        content_detail['addition'][u'建筑面积（M2）'].append([site[0].get_text(strip=True),site[1].get_text(strip=True)])

                item['content_detail'] = content_detail
                #log_obj.debug("Bitem['monitor_title']:%s\n%s" % (item['monitor_title'], item['monitor_url']))
                #log_obj.debug("item:%s" % str(item))
                yield item
            except:
                log_obj.error("%s（%s）中无法解析%s\n%s" %(self.name, response.url, site, traceback.format_exc()))
                yield response.meta['item']

if __name__ == '__main__':
    pass