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

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########

log_obj = spider_log.spider_log() #########

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

                if re.search(r'^(?!.*富阳区).*出让公告', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'^(?!.*富阳区).*出让公告'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1,
                                         dont_filter=False)
                elif re.search(r'.*富阳区.*出让公告', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'.*富阳区.*出让公告'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse2,
                                         dont_filter=False)
                elif re.search(r'.*富阳区.*拍卖公告|.*富阳区.*挂牌公告', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'.*富阳区.*拍卖公告|.*富阳区.*挂牌公告'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse3,
                                         dont_filter=False)
                elif re.search(r'.*使用权结果公示.*', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'.*使用权结果公示.*'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse4,
                                         dont_filter=False)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        """关键词：^(富阳区).+出让公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        sites = bs_obj.find_all('tr')
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

                # 地上建筑面积（M2）  ≥4991且≤23034
                if '≤' in content_detail['building_area']:
                    content_detail['building_area'] = content_detail['building_area'].split('≤')[-1]

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        """关键词：.*富阳区.*出让公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        sites = bs_obj.find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 1] #标题高度
        max_len = max([len(site) for site in sites])

        try:
            # 有合并单元格的现象
            cell_len = [len(site) for site in sites]
            l0 = [num - max(cell_len) for num in cell_len]
            normal_row = [i for i in xrange(len(l0)) if l0[i] == 0] # 找出哪几行是正常行
            short_len = map(lambda x:x[0]-x[1]-1, zip(normal_row[1:],normal_row[:-1])) # 一列中相邻两数相减
            short_len.append(0) #让这个列表的长度与normal_row一样

            for i in normal_row:
                site = sites[i]
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

                # 短小列的数据加进正常列里，需要根据具体数据格式修改
                if short_len[normal_row.index(i)] != 0:
                    row_no = i
                    content_detail['addition']['土地面积情况'] = "%s{%s},%s" %("土地面积",content_detail['purpose'],content_detail['offer_area_m2'])
                    for j in xrange(row_no + 1, row_no + short_len[normal_row.index(i)] + 1):
                        site = sites[j]
                        content_detail['purpose'] = "%s,%s" %(content_detail['purpose'], site[0].get_text(strip=True))
                        content_detail['offer_area_m2'] = float(content_detail['offer_area_m2']) + float(site[1].get_text(strip=True))
                        content_detail['offer_area_mu'] = float(content_detail['offer_area_mu']) + float(site[2].get_text(strip=True))
                        content_detail['plot_ratio'] = float(content_detail['building_area']) / float(content_detail['offer_area_m2'])
                        content_detail['addition']['土地面积情况'] = content_detail['addition']['土地面积情况'] + "%s{%s},%s" %("土地面积",site[0].get_text(strip=True),site[1].get_text(strip=True))

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse3(self, response):
        """关键词：.*富阳区.*拍卖公告|.*富阳区.*挂牌公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        sites = bs_obj.find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 2] #标题高度
        max_len = max([len(site) for site in sites])

        try:
            for i in xrange(len(sites)):
                site = sites[i]

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
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse4(self, response):
        """关键词：.*使用权结果公示.*"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.table.find_all('tr')
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 2] #标题高度

        try:
            for i in xrange(len(sites)):
                site = sites[i]
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
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass