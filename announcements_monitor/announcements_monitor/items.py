# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import sys
sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')

import mysql_connecter
mysql_connecter = mysql_connecter.mysql_connecter()

class AnnouncementsMonitorItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    #title = scrapy.Field()

    monitor_id = scrapy.Field()
    monitor_title = scrapy.Field()
    monitor_key = scrapy.Field()
    monitor_date = scrapy.Field()
    monitor_url = scrapy.Field()
    monitor_content = scrapy.Field()
    monitor_re = scrapy.Field()
    # 土地信息表中的内容
    content_html = scrapy.Field()
    content_detail = scrapy.Field()
    parcel_no = scrapy.Field()
    parcel_status = scrapy.Field()

