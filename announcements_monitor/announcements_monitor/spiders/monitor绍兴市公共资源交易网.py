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
import pandas as pd
import scrapy
import announcements_monitor.items
import re
import datetime
import requests
import numpy as np

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########

log_obj = spider_log.spider_log() #########

re_table = {u'地块名称':'parcel_no',
            u'土地用途':'purpose',
            u'土地面积':'offer_area_m2',
            u'容积率':'plot_ratio',
            u'土地坐落':'parcel_location',
            u'土地座落':'parcel_location',
            u'建筑面积':'building_area',
            u'起拍价':'starting_price_sum',
            u'起始价':'starting_price_sum',
            }

class Spider(scrapy.Spider):
    name = "511706"
    allowed_domains = ["www.sxztb.gov.cn"]

    def start_requests(self):
        self.url1 = "http://www.sxztb.gov.cn/sxweb/tdjy/005006/005006004/005006004001/MoreInfo.aspx?CategoryNum=005006004001"
        self.url2 = "http://www.sxztb.gov.cn/sxweb/tdjy/005006/005006005/005006005001/MoreInfo.aspx?CategoryNum=005006005001"
        yield scrapy.Request(url=self.url1, callback=self.parse)
        yield scrapy.Request(url=self.url2, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        e_table = bs_obj.find('table', id='MoreInfoList1_DataGrid1')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        for e_tr in e_table.find_all('tr'):
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '绍兴'
            e_tds = e_tr.find_all('td')
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tds[1].get_text(strip=True) # 标题
                item['monitor_date'] = e_tds[2].get_text(strip=True) # 发布日期
                item['monitor_url'] = 'http://www.sxztb.gov.cn' + e_tds[1].a.get('href') # 链接

                if re.search(ur'绍兴市国土资源局国有建设用地使用权.*|绍兴市国土资源局上虞区分局国有建设用地使用权出让公告.*', item['monitor_title']):
                    item['monitor_re'] = '绍兴市国土资源局国有建设用地使用权.*'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                elif response.url == self.url2:
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse2, dont_filter=True)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        e_table = bs_obj.find("table", class_="MsoNormalTable")
        df = ''
        try:
            # 绍兴国土局，成交信息与挂牌信息的地块要对应起来的话，得用地块名称，所以先把地块编号保存在addition中
            m = re.search(ur'虞土.+?号|工业.+?|绍市.+?号', bs_obj.prettify(encoding='utf8'))
            parcel_no = ''
            if m:
                parcel_no = m.group()

            # 先读取成为dataframe，然后转换成dict
            df = pd.read_html(e_table.prettify(encoding='utf8'), encoding='utf8')[0]
            df = pd.DataFrame(np.array(df.loc[1:,:]), columns=list(df.loc[0]))
            for k in re_table:
                df.columns = map(lambda x:re_table[re.search(ur'%s' %k, x).group()] if re.search(ur'%s' %k, x)
                                 else x, df.columns)
            for i in xrange(len(df.index)):
                content_detail = {'addition':{}}
                d = dict(zip(df.columns, np.array(df.loc[i, :]).tolist()))
                d = {re.sub(r'\s+','',key):re.sub(ur'\s+', '', d[key]) for key in d}
                for key in d:
                    if key in re_table.viewvalues():
                        content_detail[key] = d[key]
                    else:
                        content_detail['addition'][key] = d[key]

                if parcel_no:
                    content_detail['addition']['地块编号备用'] = parcel_no
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析:\n%s\n%s" %(self.name, response.url, df, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'sold'
        e_page = bs_obj.find('td', attrs={'id':'TDContent', 'class':'infodetail'})
        try:
            e_table = e_page.find('table', class_='MsoNormalTable')
            e_trs = e_table.find_all('tr')[1:]
            for e_tr in e_trs:
                e_tds = e_tr.find_all('td')
                content_detail = {
                    'parcel_no':e_tds[0].get_text(strip=True),
                    'offer_area_m2':e_tds[1].get_text(strip=True),
                    'purpose':e_tds[3].get_text(strip=True),
                    'transaction_price_sum':e_tds[4].get_text(strip=True),
                    'competitive_person':e_tds[5].get_text(strip=True),
                    'addition':{'出让方式':e_tds[2].get_text(strip=True)}
                }
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析:\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass

"""
	
ASP.NET_SessionId=nyfwwyjh2ul0et550monou45	
ASP.NET_SessionId=nyfwwyjh2ul0et550monou45
"""