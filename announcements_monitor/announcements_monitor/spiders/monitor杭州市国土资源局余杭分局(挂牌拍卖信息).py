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
import re

import bs4
import scrapy
import announcements_monitor.items
import datetime

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########

log_obj = spider_log.spider_log() #########

class Spider(scrapy.Spider):
    name = "511699"
    allowed_domains = ["www.yhland.gov.cn"]

    def start_requests(self):
        urls =  ["http://www.yhland.gov.cn/newsGTSC.aspx?tag=1&classid=27&page=%s" % i for i in xrange(2) if i > 0]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = scrapy.Selector(response)
        root_path = '//table/tbody/tr/td[1]/table[2]/tbody/tr/td/table[1]/tbody/tr'
        sites = sel.xpath(root_path)
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        if not sites:
            sites = sel.xpath(root_path.replace("/tbody",""))

        for site in sites:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = re.sub(r'·', '', site.xpath('td[1]/a[@target="_blank"]/@title').extract_first()) # 宗地名称
                item['monitor_date'] = re.sub(r'\s+', '', site.xpath('td[2]/text()').extract_first()) # 成交日期
                item['monitor_url'] = "http://www.yhland.gov.cn/" + site.xpath('td[1]/a[@target="_blank"]/@href').extract_first() # 链接
                item['monitor_content'] = ""

                if re.search(r'.*余政工出.*出让公告', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = '.*余政工出.*出让公告'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1,
                                         dont_filter=False)
                elif re.search(r'.*余政储出.*出让公告', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'.*余政储出.*出让公告'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse2,
                                         dont_filter=False)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        """关键字：.*余政工出.*出让公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find("table", class_="MsoNormalTable").find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 1] #标题高度

        try:
            for i in xrange(len(sites)):
                site = sites[i]
                content_detail =\
                            {'parcel_no': site[0].get_text(strip=True),
                            'parcel_location': site[1].get_text(strip=True),
                            'offer_area_m2': site[2].get_text(strip=True),
                            'purpose': site[3].get_text(strip=True),
                            'plot_ratio': site[4].get_text(strip=True),
                            'starting_price_sum': site[7].get_text(strip=True),
                            'addition': {u'保证金': site[8].get_text(strip=True)}
                            }
                if '-' in content_detail['plot_ratio']:
                    content_detail['plot_ratio'] = str(content_detail['plot_ratio']).split('-')[-1]

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        """关键词：.*余政储出.*出让公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find("table", class_="MsoNormalTable").find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 1] #标题高度

        try:
            for i in xrange(len(sites)):
                site = sites[i]
                content_detail =\
                            {'parcel_no': site[0].get_text(strip=True),
                            'parcel_location': site[1].get_text(strip=True),
                            'offer_area_m2': site[2].get_text(strip=True),
                            'purpose': site[3].get_text(strip=True),
                            'building_area': site[4].get_text(strip=True)
                            }

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass