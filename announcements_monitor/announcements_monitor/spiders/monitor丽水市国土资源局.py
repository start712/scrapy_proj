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
sys.path.append(os.getcwd())
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import html_table_reader
html_table_reader = html_table_reader.html_table_reader()
log_obj = spider_log.spider_log() #########

with open(os.getcwd() + r'\announcements_monitor\spiders\needed_data.txt', 'r') as f:
    s = f.read()
    needed_data = s.split(',')
needed_data = [s.encode('utf8') for s in needed_data]

re_text = {
    'parcel_location':ur'土地坐落：.+?。',
    'purpose':ur'土地用途：.+?。',
    '出让年限':ur'出让年限：.+?。'
}

re_table = {
    u'土地坐落':'parcel_location',
    u'地块位置':'parcel_location',
    u'土地用途':'purpose',
    u'土地面积':'offer_area_m2',
    u'容积率':'plot_ratio',
    u'起始价':'starting_price_sum',
    u'建筑面积':'building_area'
}

class Spider(scrapy.Spider):
    name = "511712"

    def start_requests(self):
        # 丽水相应网址的index的系数，index_1代表第二页
        self.urls1 = ["http://www.lssgtzyj.gov.cn/ArticleList/Index/284?pageIndex=%s&title=" %i for i in xrange(3) if i > 0]
        #self.urls2 = ["http://www.zjtzgtj.gov.cn/scxx/tdsc/tdcrcj/index_%s.html" %i for i in xrange(3) if i > 0]

        for url in self.urls1:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
            导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        #e_table = bs_obj.find('div', class_='txtlist')
        e_row = bs_obj.find_all('div', class_='Lcon02R-2-01')
        for e_tr in e_row:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '丽水'
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tr.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_tr.find('p', class_='p21').get_text(strip=True) # 成交日期
                item['monitor_url'] = "http://www.lssgtzyj.gov.cn" + e_tr.a.get('href')

                yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
            except:
                log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        try:
            e_page = bs_obj.find('div', attrs={'id':'infoContent', 'class':'SconC'})
            # 处理网页文字
            extra_data = False
            e_ps = e_page.find_all('p')
            row_ps = [e_p.get_text(strip=True) for e_p in e_ps]
            d = {rs:[] for rs in re_text}
            for row_s in row_ps:
                for rs in re_text:
                    m = re.search(re_text[rs], row_s)
                    if m:
                        d[rs].append(m.group())
                        extra_data = True

            # 处理网页中的表格
            e_table = e_page.table
            df = html_table_reader.title_standardize(html_table_reader.table_tr_td(e_table), delimiter=r'=>')
            #log_obj.update_error(df.to_string())
            for k in re_table:
                df.columns = map(lambda x:re_table[re.search(ur'%s' %k, x).group()] if re.search(ur'%s' %k, x)
                                 else x, df.columns)
            log_obj.update_error(df.to_string().encode('utf8'))
            for i in xrange(len(df.index)):
                detail = df.iloc[i,:].to_dict()
                if extra_data:
                    d0 = {key:d[key][i] for key in d}
                    detail.update(d0)
                content_detail = {'addition':{}}
                for key in detail:
                    if key in re_table.viewvalues():
                        content_detail[key] = detail[key]
                    else:
                        content_detail['addition'][key] = detail[key]

                item['parcel_no'] = re.search(ur'丽土.+?号', item['monitor_title']).group()
                if len(df.index) > 1:
                    if 'parcel_name' not in content_detail:
                        item['parcel_no'] = item['parcel_no'] + ('|%s|' %i)
                    else:
                        item['parcel_no'] = item['parcel_no'] + ('|%s|' % content_detail['parcel_name'])
                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass