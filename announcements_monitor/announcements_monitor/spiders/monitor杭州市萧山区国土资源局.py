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
    name = "511700"
    allowed_domains = ["115.236.5.251"]

    def start_requests(self):
        urls =  ["http://115.236.5.251/Bulletin/BulletinList.aspx?ProType=13&AfficheType=617&Class=227&ViewID=112",
                 "http://115.236.5.251/Bulletin/BulletinList.aspx?ProType=13&AfficheType=619&Class=227&ViewID=241"]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        #log_obj.debug(u"准备分析内容：%s" %response.url)
        sel = scrapy.Selector(response)
        items = []
        root_path = '//tbody/tr[@class="Row"]'
        sites = sel.xpath(root_path)  # [@id="list"] [@class="padding10"][position()>1]
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        if not sites:
            sites = sel.xpath(root_path.replace("/tbody",""))

        for site in sites:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            try:
                #print dir(site.xpath('td[@align="left"]/a/text()'))
                item['monitor_id'] = self.name
                id = site.xpath('td[@align="left"]/a[@target="_blank"]/text()').extract_first() # 招标编号
                title = site.xpath('td[contains(@class,"DispLimitColumn")]/div/a[@target="_blank"]/text()').extract_first() # 标题
                item['monitor_title'] = id + title

                item['monitor_date'] = site.xpath('td[contains(@class,"DateColumn")]/text()').extract_first() # 发布日期
                item['monitor_url'] = 'http://115.236.5.251/Bulletin/' + site.xpath('td[@align="left"]/a/@href').extract_first() # 链接
                item['monitor_content'] = ""


                if re.match(r'^(?!.*萧山区).*挂牌出让公告', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'^(?!.*富阳区).*挂牌出让公告'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=False)
                elif re.match(r'.*萧山区.*挂牌出让公告', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'.*萧山区.*挂牌出让公告'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse2, dont_filter=False)
                elif re.match(r'.*萧政储出.*出让成交公示', item['monitor_title'].encode('utf8')):
                    item['monitor_re'] = r'.*萧政储出.*出让成交公示'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse3, dont_filter=False)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        """关键词：^(?!.*萧山区).*挂牌出让公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
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

    def parse2(self, response):
        """关键词：.*萧山区.*挂牌出让公告"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find("table", class_="MsoNormalTable").find_all('tr')
        title = [s.get_text(strip=True) for s in sites[0].find_all('td')]
        # 去掉标题
        sites = [site.find_all('td') for site in sites if sites.index(site) >= 1] #标题高度
        max_len = max([len(site) for site in sites])

        try:
            for i in xrange(len(sites)):
                site = sites[i]
                if len(site) != max_len:
                    log_obj.update_debug(u"%s{%s}出现不规则数据%s" % (self.name, response.url,[s.get_text(strip=True) for s in site]))
                    continue
                # 存在http://115.236.5.251/Bulletin/BulletinBrowse.aspx?id=25126中这种变态的表格
                content_detail =\
                            {'parcel_no': site[0].get_text(strip=True),
                            'parcel_location': site[1].get_text(strip=True),
                            #'offer_area_m2': site[-7].get_text(strip=True),
                            'purpose': site[-6].get_text(strip=True),
                            'plot_ratio': site[-5].find('span', lang='EN-US').get_text(strip=True),
                            'starting_price_sum': site[-1].get_text(strip=True),
                            }
                addition_key = u'无指配key'
                if re.search(r'.*建筑.*', title[2]):
                    content_detail['building_area'] = site[-7].get_text(strip=True)
                    addition_key = '建设用地面积/容积率/用途'
                    content_detail['addition'] = {addition_key: [[site[-7].get_text(strip=True), site[-5].find('span', lang='EN-US').get_text(strip=True),site[-6].get_text(strip=True)],]}
                if re.search(r'.*出让.*', title[2]):
                    content_detail['offer_area_m2'] = site[-7].get_text(strip=True)
                    addition_key = '出让土地面积/容积率/用途'
                    content_detail['addition'] = {addition_key: [[site[-7].get_text(strip=True), site[-5].find('span', lang='EN-US').get_text(strip=True),site[-6].get_text(strip=True)], ]}

                # 处理合并单元格的问题
                while True:
                    if i >= len(sites)-1: #后面没有数据了
                        break
                    if len(sites[i+1]) == max_len:
                        content_detail['addition'] = ''
                        break
                    i += 1
                    site = sites[i]
                    content_detail['plot_ratio'] = ''
                    content_detail['purpose'] = ''
                    if 'offer_area_m2' in content_detail:
                        content_detail['offer_area_m2'] = float(content_detail['offer_area_m2']) + float(site[-6].get_text(strip=True))
                    if 'building_area' in content_detail:
                        content_detail['building_area'] = float(content_detail['building_area']) + float(site[-6].get_text(strip=True))

                    content_detail['addition'][addition_key].append([site[-6].get_text(strip=True),site[-4].find('span', lang='EN-US').get_text(strip=True),site[-5].get_text(strip=True)])
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse3(self, response):
        """关键词：.*萧政储出.*出让成交公示"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find("table", class_="MsoNormalTable").find_all('tr')
        # 去掉标题
        sites = [site.table.find_all('td') for site in sites if sites.index(site) >= 1] #标题高度
        max_len = max([len(site) for site in sites])

        try:
            for i in xrange(len(sites)):
                site = sites[i]
                if len(site) != max_len:
                    log_obj.update_debug(u"%s{%s}出现不规则数据%s" % (self.name, response.url,[s.get_text(strip=True) for s in site]))
                    continue
                content_detail =\
                            {'parcel_no': site[0].get_text(strip=True),
                            'parcel_location': site[1].get_text(strip=True),
                            'offer_area_m2': site[-7].get_text(strip=True),
                            'purpose': site[-6].get_text(strip=True),
                            'starting_price_sum': site[-5].get_text(strip=True),
                            'transaction_price_sum': site[-4].get_text(strip=True),
                            'competitive_person': site[-3].get_text(strip=True),
                            'addition': {'土地面积/用途':[[site[-7].get_text(strip=True),site[-6].get_text(strip=True)],]}
                            }

                # 处理合并单元格的问题
                while True:
                    if i >= len(sites)-1: #后面没有数据了
                        break
                    if len(sites[i+1]) == max_len:
                        content_detail['addition'] = ''
                        break
                    i += 1
                    site = sites[i]
                    content_detail['purpose'] = ''
                    content_detail['offer_area_m2'] = float(content_detail['offer_area_m2']) + float(site[-3].get_text(strip=True))
                    content_detail['addition']['土地面积/用途'].append([site[-3].get_text(strip=True),site[-2].get_text(strip=True)])


                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']


if __name__ == '__main__':
    pass