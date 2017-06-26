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
import datetime
import bs4
import traceback

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########

log_obj = spider_log.spider_log() #########

class Spider(scrapy.Spider):
    name = "511702" #成交信息
    allowed_domains = ["www.hzgtj.gov.cn"]

    def start_requests(self):
        urls =  ["http://www.hzgtj.gov.cn/fore/portal/infos/toList?parentIdStr=1-32-6285-&id=6285",]

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
                item['monitor_content'] = ""

                if re.match(r'(杭政工出.*)|(杭政储出.*)', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'(杭政工出.*)|(杭政储出.*)'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=False)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        """关键词：(杭政工出.*)|(杭政储出.*)"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find_all('tr')
        sites = [site.find_all('td') for site in sites if site.b == None and 2<=sites.index(site)<len(sites)-1]  # [@id="list"] [@class="padding10"][position()>1]

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
                     'parcel_name': site[1].get_text(strip=True),
                     'purpose': site[2].get_text(strip=True),
                     'competitive_person': site[3].get_text(strip=True),
                     'offer_area_m2': site[4].get_text(strip=True),
                     'offer_area_mu': site[5].get_text(strip=True),
                     'plot_ratio': str(site[6].get_text(strip=True)).split('～')[-1],
                     'building_area': str(site[7].get_text(strip=True)).split('～')[-1],
                     'starting_price_sum': site[8].get_text(strip=True),
                     'transaction_price_sum': site[9].get_text(strip=True),
                     'transaction_price': site[10].get_text(strip=True),
                     'floor_transaction_price': str(site[11].get_text(strip=True)).split('～')[-1],
                     'addition': {'报名及竞价情况': site[-1].get_text(strip=True)}
                     }

                # 若没有数据，去除 / 符号
                content_detail['plot_ratio'] = re.sub(r'/', '', content_detail['plot_ratio'])
                content_detail['transaction_price'] = re.sub(r'/', '', content_detail['transaction_price'])

                # 去除一些没用的括号
                content_detail['floor_transaction_price'] = re.sub(r'[\(（].+[）\)]|\s+', '', content_detail['floor_transaction_price'])

                # 建筑面积有时会杂有文字
                if '地上' in content_detail['building_area']:
                    m = re.search(ur'(?<=地上建筑面积)\d+(?=平方米)|(?<=地上)\d+(?=平方米)', site[4].get_text(strip=True))
                    if m:
                        content_detail['building_area'] = m.group(0)
                    content_detail['addition'][u'建筑面积（M2）'] = site[4].get_text(strip=True)
                elif '地下' in content_detail['building_area']:
                    content_detail['building_area'] =''
                    content_detail['addition'][u'建筑面积（M2）'] = site[4].get_text(strip=True)

                # 短小列的数据加进正常列里
                if short_len[normal_row.index(i)] != 0:
                    row_no = i
                    content_detail['addition']['土地面积情况'] = "%s(%s),%s" %("土地面积",content_detail['purpose'],content_detail['offer_area_m2'])
                    for j in xrange(row_no + 1, row_no + short_len[normal_row.index(i)] + 1):
                        site = sites[j]
                        content_detail['purpose'] = "%s,%s" %(content_detail['purpose'], site[0].get_text(strip=True))
                        content_detail['offer_area_m2'] = float(content_detail['offer_area_m2']) + float(site[1].get_text(strip=True))
                        content_detail['offer_area_mu'] = float(content_detail['offer_area_mu']) + float(site[2].get_text(strip=True))
                        content_detail['plot_ratio'] = float(content_detail['building_area']) / float(content_detail['offer_area_m2'])
                        content_detail['addition']['土地面积情况'] = content_detail['addition']['土地面积情况'] + "%s(%s),%s" %("土地面积",site[0].get_text(strip=True),site[1].get_text(strip=True))

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass