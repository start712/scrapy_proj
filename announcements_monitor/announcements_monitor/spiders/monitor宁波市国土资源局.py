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

# 对应于数据库中字段名的标题
needed_item = ['parcel_no', 'offer_area_m2', 'purpose', 'plot_ratio', 'starting_price_sum', 'parcel_location',
               'starting_price', 'starting_price_sum', 'parcel_name', 'floor_starting_price', 'transaction_price_sum',
               'transaction_price']

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
        '象山':[0,[],[]], # 表示需要单独设置一个解析方法
        '鄞州':[1, ['parcel_no', 'parcel_name', 'offer_area_m2', 'purpose', '建筑密度', 'plot_ratio', '绿地率',
                  '建筑高度', '出让年限（年）', '竞买保证金(万元)', 'floor_starting_price'], ['序号',]],
        '保税区':[2, ['序号', 'parcel_name', 'offer_area_m2', 'purpose', 'purpose', '绿地率', '建筑系数',
                   '限高（m）', '出让年限', '投资强度(万元/亩）', '竞买保证金（万元）', 'starting_price_sum'], []],
        '镇海':[1, ['parcel_name', 'parcel_location', 'offer_area_m2', 'purpose', '出让年限', '建筑密度',
                  'plot_ratio', '建筑高度', '绿地率'], []],
        '宁海':[0,[],[]], # 表示需要单独设置一个解析方法
    },
    '出让结果公告':{
        '市局':[1, ['序号', 'parcel_name', 'parcel_location', 'offer_area_m2', 'purpose', '土地级别', 'plot_ratio',
                  '出让年限（年）', '供地方式', 'competitive_person', 'transaction_price_sum', '确认时间'], ['序号',]],
        '北仑':[1, ['序号', 'parcel_no', 'competitive_person', 'offer_area_m2', 'plot_ratio', 'purpose', '使用年限',
                  'floor_transaction_price', '竞得时间', '备注'], ['序号', ]],
        '象山':[0,[],[]],
        '慈溪':[2, ['序号', 'parcel_no', 'offer_area_m2', 'purpose', 'plot_ratio', '建筑密度(%)', '绿地率(%)', '绿地率(%)',
                  'starting_price_sum', 'transaction_price_sum', '成交日期', 'competitive_person'], ['序号', ]],
        '奉化':[1, ['parcel_no', 'transaction_price_sum', 'competitive_person', '出让方式', '出让日期', 'offer_area_m2',
                  'purpose', 'plot_ratio'], []],
        '余姚':[2, ['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', '出让年限', 'transaction_price',
                  'transaction_price_sum', 'floor_transaction_price', 'competitive_person'], []],
        '镇海':[1, ['parcel_no', 'parcel_name', 'offer_area_m2', 'purpose', 'plot_ratio', 'floor_starting_price',
                  'floor_transaction_price', 'transaction_price_sum', 'competitive_person', '成交日期'], []],
        '鄞州':[1, ['序号', 'parcel_name', 'parcel_location', 'offer_area_m2', 'purpose', '土地级别', 'plot_ratio',
                  '出让年限（年）', '供地方式', 'competitive_person', 'floor_transaction_price', '成交时间'], ['序号',]],
        '保税区':[1, ['地块编号', 'parcel_location', '土地面积(公顷)', 'purpose', '出让年限', 'transaction_price_sum',
                   'competitive_person'], []],
        '宁海':[0,[],[]],
        '江北':[0,[],[]]
    }
}

class Spider(scrapy.Spider):
    def start_requests(self):
        # http://www.nblr.gov.cn/showpage2/pubchief.jsp?type=tdcrgg
        #               http://www.nblr.gov.cn/show3.do?method=getSomeInfo_list&name=gpcrgg&page=
        self.urls1 =  ["http://www.nblr.gov.cn/show3.do?method=getSomeInfo_list&name=gpcrgg&page=%s" % i for i in xrange(2) if i > 0]
        self.urls2 =  ["http://www.nblr.gov.cn/show3.do?method=getSomeInfo_list&name=crjggg&page=%s" % i for i in xrange(2) if i > 0]

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
                area = site[1].find('a', style='color: #615b97').get_text(strip=True)
                item['monitor_title'] = site[1].find('a', target='_blank').get('title')
                item['monitor_date'] = site[2].get_text(strip=True)
                item['monitor_url'] = 'http://www.nblr.gov.cn/' + site[1].find('a', target='_blank').get('href') # 链接
                item["monitor_content"] = re.sub(r'(\[)|(\])', '', area).encode('utf8')

                if response.url in self.urls1:
                    item['parcel_status'] = 'onsell'
                elif response.url in self.urls2:
                    item['parcel_status'] = 'sold'
                else:
                    yield item
                yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse0, dont_filter=True)
            except:
                log_obj.update_error("%s中存在无法解析的xpath：%s\n原因：%s" %(self.name, site, traceback.format_exc()))


    def parse0(self, response):
        status = {'onsell':'挂牌出让公告', 'sold': '出让结果公告'}
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        area = item["monitor_content"]

        try:
            title_row_count, titles, title_bans= title_structure[status[item['parcel_status']]][area]
            #item['content_html'] = bs_obj.prettify()
            sites = bs_obj.find("table", id='table125').table

            # 若标题行数标记为0，则说明有多种网页形式
            if not title_row_count:
                raise

            sites = sites.find_all('tr')
            # 删除标题行
            sites = sites[title_row_count:]

            for i in xrange(len(sites)):
                site = sites[i].find_all('td')
                content_detail = {}
                for j in xrange(len(titles)):
                    # 去掉一些不需要的标题
                    if titles[j] in title_bans:
                        continue
                    # 填入对应标题的数据
                    content_detail[titles[j]] = site[j].get_text(strip=True)

                # 忽略合计行
                if 'parcel_no' in content_detail and re.sub(r'\s+','',content_detail['parcel_no']) == u'合计':
                    continue

                m = re.search(ur'余?.土[^土]+号', item['monitor_title'])
                if m:
                    if 'parcel_no' in content_detail and content_detail['parcel_no']:
                        content_detail['parcel_no'] = re.sub(r'\s+','',"%s{%s}" %(m.group(), content_detail['parcel_no']))
                    else:
                        content_detail['parcel_no'] = re.sub(r'\s+', '', m.group())

                if 'plot_ratio' in content_detail:
                    content_detail['addition']['容积率区间'] = re.split(r'[^\d]+', content_detail['plot_ratio'])
                    content_detail['plot_ratio'] = re.split(r'[^\d]+', content_detail['plot_ratio'])[-1]

                item['content_detail'] = content_detail
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass