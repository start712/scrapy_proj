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
    name = "511704"
    allowed_domains = ["122.224.205.74:33898"]

    def start_requests(self):
        self.url1 = "http://122.224.205.74:33898/ProArticle/ProArticleList.aspx?ViewID=329"
        self.url2 = "http://122.224.205.74:33898/ProArticle/ProArticleList.aspx?ViewID=332"

        for page_num in xrange(5,6):
            meta = {'GridViewer1$ctl18$BtnGoto':u'Go', 'GridViewer1$ctl18$NumGoto': page_num}
            yield scrapy.Request(url=self.url1, method="POST", meta = meta, callback=self.parse, dont_filter=False)
            #yield scrapy.Request(url=self.url2, method="POST", meta = meta, callback=self.parse, dont_filter=False)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        sites = bs_obj.find('table', id="GridViewer1").find_all('tr', class_="Row")
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""

        for site0 in sites:
            site = site0.find_all('td')
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            try:
                # 有“政府采购项目”的条目不是需要的数据
                buyer = site[2].get_text(strip=True) # 采购单位
                #print 'buyer:', buyer
                if buyer:
                    continue

                item['monitor_id'] = self.name
                id = site[0].get_text(strip=True) # 招标编号
                title = site[1].get_text(strip=True) # 标题
                item['monitor_title'] = id + title
                item['monitor_date'] = site[3].get_text(strip=True) # 发布日期
                item['monitor_url'] = 'http://122.224.205.74:33898/ProArticle/' + site[1].a.get('href') # 链接

                if response.url == self.url1:
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                elif response.url == self.url2:
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse2, dont_filter=True)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        #item['content_html'] = bs_obj.prettify()
        sites = bs_obj.find("table", class_="MsoNormalTable").find_all('tr', style = 'height:114.7500pt;')

        try:
            for i in xrange(len(sites)):
                site = sites[i].find_all('td')
                content_detail =\
                            {'parcel_no': site[0].get_text(strip=True),
                             'parcel_location': site[1].get_text(strip=True),
                             'purpose': site[2].get_text(strip=True),
                             'offer_area_m2': site[3].get_text(strip=True),
                             'plot_ratio': site[4].get_text(strip=True),
                             'addition':{'土地出让年限':site[5].get_text(strip=True)}
                            }
                print content_detail
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
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

"""
	
ASP.NET_SessionId=nyfwwyjh2ul0et550monou45	
ASP.NET_SessionId=nyfwwyjh2ul0et550monou45
"""