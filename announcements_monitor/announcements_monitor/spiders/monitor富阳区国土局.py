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



class Spider(scrapy.Spider):
    name = "511695"
    allowed_domains = ["www.fuyang.gov.cn"]

    def start_requests(self):
        urls1 = ["http://www.fuyang.gov.cn/fy/gtj/crxx/index_%s.jhtml" % i for i in xrange(2) if i > 0]
        urls2 = ["http://www.fuyang.gov.cn/fy/gtj/cjxx/index_%s.jhtml" % i for i in xrange(2) if i > 0]

        for url in urls1+urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        #log_obj.debug(u"准备分析内容：%s" %response.url)
        sel = scrapy.Selector(response)
        items = []
        root_path = '/html/body/div[6]/div[2]/div/div[2]/div/ul/li'
        sites = sel.xpath(root_path)  # [@id="list"]
        if not sites:
            sites = sel.xpath(root_path.replace("/tbody",""))

        for site in sites:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = site.xpath('a/text()').extract_first()
                item['monitor_date'] = site.xpath('span/text()').extract_first()
                item['monitor_url'] = site.xpath('a/@href').extract_first()
                item['monitor_content'] = ""

            except:
                info = sys.exc_info()
                log_obj.debug(u"%s中无法解析%s\n原因：%s%s%s" %(self.name, site, info[0], ":", info[1]))

        #csv_report.output_data(items, "result", method='a')
        #return items
            if re.search(r'^(?!.*富阳区).*出让公告', item['monitor_title'].encode('utf8')):
                item['monitor_re'] = r'^(?!.*富阳区).*出让公告'
                yield scrapy.Request(url=item['monitor_url'],meta={'item':item},callback=self.parse1, dont_filter=False)
            elif re.search(r'.*富阳区.*出让公告', item['monitor_title'].encode('utf8')):
                item['monitor_re'] = r'.*富阳区.*出让公告'
                yield scrapy.Request(item['monitor_url'],meta={'item':item},callback=self.parse2, dont_filter=False)
            elif re.search(r'.*富阳区.*拍卖公告|.*富阳区.*挂牌公告', item['monitor_title'].encode('utf8')):
                item['monitor_re'] = r'.*富阳区.*拍卖公告|.*富阳区.*挂牌公告'
                yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse3, dont_filter=False)
            elif re.search(r'.*使用权结果公示.*', item['monitor_title'].encode('utf8')):
                item['monitor_re'] = r'.*使用权结果公示.*'
                yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse4, dont_filter=False)
            else:
                yield item

    def parse1(self, response):
        """关键词：^(富阳区).+出让公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 1] #标题高度
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
                            'building_area': site[4].get_text(strip=True)
                            }

                # 地上建筑面积（M2）  ≥4991且≤23034
                if '≤' in content_detail['building_area']:
                    content_detail['building_area'] = content_detail['building_area'].split('≤')[-1]

                item['content_detail'] = content_detail
                yield item
            except:
                log_obj.error("%s（%s）中无法解析%s\n%s" %(self.name, response.url, site, traceback.format_exc()))
                yield response.meta['item']

    def parse2(self, response):
        """关键词：.*富阳区.*出让公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 1] #标题高度
        max_len = max([len(site) for site in sites])
        if not sites:
            log_obj.debug(u"%s(%s)没有检测到更多detail" % (self.name, response.url))

        for i in xrange(len(sites)):
            site = sites[i]
            try:
                if len(site) != max_len:
                    log_obj.debug(u"%s(%s)出现不规则数据%s" % (self.name, response.url,[s.get_text(strip=True) for s in site]))
                    continue
                content_detail = \
                    {'parcel_no': site[0].get_text(strip=True),
                     'parcel_location': site[1].get_text(strip=True),
                     'offer_area_m2': site[2].get_text(strip=True),
                     'purpose': site[3].get_text(strip=True),
                     'plot_ratio': site[4].get_text(strip=True),
                     'starting_price_sum': site[7].get_text(strip=True),
                     'addition': {'保证金': site[8].get_text(strip=True)}
                     }
                if '≤' in content_detail['plot_ratio']:
                    content_detail['plot_ratio'] = content_detail['plot_ratio'].split('≤')[-1]

                item['content_detail'] = content_detail
                yield item
            except:
                log_obj.error("%s（%s）中无法解析%s\n%s" %(self.name, response.url, site, traceback.format_exc()))
                yield response.meta['item']

    def parse3(self, response):
        """关键词：.*富阳区.*拍卖公告|.*富阳区.*挂牌公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 2] #标题高度
        max_len = max([len(site) for site in sites])
        if not sites:
            log_obj.debug(u"%s(%s)没有检测到更多detail" % (self.name, response.url))

        for i in xrange(len(sites)):
            site = sites[i]
            try:
                content_detail = \
                    {'parcel_no': site[0].get_text(strip=True),
                     'parcel_location': site[1].get_text(strip=True),
                     'offer_area_m2': site[2].get_text(strip=True),
                     'purpose': site[3].get_text(strip=True),
                     'plot_ratio': site[4].get_text(strip=True),
                     'starting_price_sum': site[8].get_text(strip=True)
                     }
                if '≤' in content_detail['plot_ratio']:
                    content_detail['plot_ratio'] = content_detail['plot_ratio'].split('≤')[-1]

                item['content_detail'] = content_detail
                yield item
            except:
                log_obj.error("%s（%s）中无法解析%s\n%s" %(self.name, response.url, site, traceback.format_exc()))
                yield response.meta['item']

    def parse4(self, response):
        """关键词：.*使用权结果公示.*"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.table.find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 2] #标题高度
        if not sites:
            log_obj.debug(u"%s(%s)没有检测到更多detail" % (self.name, response.url))

        for i in xrange(len(sites)):
            site = sites[i]
            try:
                # 过滤空白表格
                if not site[0].get_text(strip=True):
                    continue

                content_detail = \
                    {'parcel_no': site[0].get_text(strip=True),
                     'parcel_location': site[1].get_text(strip=True),
                     'offer_area_m2': site[2].get_text(strip=True),
                     'purpose': site[3].get_text(strip=True),
                     'plot_ratio': site[4].get_text(strip=True),
                     'starting_price_sum': site[6].get_text(strip=True),
                     'transaction_price_sum': site[7].get_text(strip=True),
                     'competitive_person': site[8].get_text(strip=True)
                     }
                if '≤' in content_detail['plot_ratio']:
                    content_detail['plot_ratio'] = content_detail['plot_ratio'].split('≤')[-1]

                item['content_detail'] = content_detail
                yield item
            except:
                log_obj.error("%s（%s）中无法解析%s\n%s" %(self.name, response.url, site, traceback.format_exc()))
                yield response.meta['item']

if __name__ == '__main__':
    pass