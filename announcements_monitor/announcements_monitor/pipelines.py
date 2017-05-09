# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import MySQLdb.cursors
import re
from twisted.enterprise import adbapi
import sys
import datetime
import os
import json
import copy
import traceback
log_path = r'%s/log/sql_update(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
import csv_report
csv_report = csv_report.csv_report()

log_obj = set_log.Logger(log_path, set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup(log_path, if_cleanup=False)  # 是否需要在每次运行程序前清空Log文件

class AnnouncementsMonitorPipeline(object):
    def __init__(self,dbpool):
        self.dbpool=dbpool
        #self.ban_keys = []

    @classmethod
    def from_settings(cls,settings):
        '''1、@classmethod声明一个类方法，而对于平常我们见到的则叫做实例方法。 
           2、类方法的第一个参数cls（class的缩写，指这个类本身），而实例方法的第一个参数是self，表示该类的一个实例
           3、可以通过类来调用，就像C.f()，相当于java中的静态方法'''
        dbparams=dict(
            host=settings['MYSQL_HOST'],#读取settings中的配置
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8',#编码要加上，否则可能出现中文乱码问题
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=False,
        )
        dbpool=adbapi.ConnectionPool('MySQLdb',**dbparams)#**表示将字典扩展为关键字参数,相当于host=xxx,db=yyy....
        return cls(dbpool)#相当于dbpool付给了这个类，self中可以得到

    # 备用
#    def get_keys(self, settings):
#        self.ban_keys = settings['BAN_KEYS']

    #pipeline默认调用
    def process_item(self, item0, spider):
        """
其原因是由于Spider的速率比较快，而scapy操作数据库操作比较慢，导致pipeline中的方法调用较慢，这样当一个变量正在处理的时候，一个新的变量过来，之前的变量的值就会被覆盖
，比如pipline的速率是1TPS，而spider的速率是5TPS，那么数据库应该会有5条重复数据。

解决方案是对变量进行保存，在保存的变量进行操作，通过互斥确保变量不被修改
        """
        item = copy.deepcopy(item0)
        item_list = ['monitor_id', 'monitor_title', 'monitor_key', 'monitor_date', 'monitor_url', 'monitor_content',
                     'content_html', 'content_detail', 'parcel_no', 'monitor_re']
        s_list = []
        for s in item_list:
            if s not in item:
                item[s] = ''
                s_list.append(s)
        #if s_list:
        #    log_obj.debug(u'%s中为空字符串的字段为%s' %(item['monitor_title'], s_list))

        if type(item['content_detail']) == type({}):
            item["parcel_no"] = item['content_detail']["parcel_no"]#re.sub(r'\s+', '', item['content_detail']["parcel_no"])
        if item["parcel_no"]:
            item["monitor_key"] = "%s/%s/%s" % (item["monitor_id"], item["monitor_date"], item["parcel_no"])#re.sub(r'\s+', '', "%s/%s/%s" % (item["monitor_id"], item["monitor_date"], item["parcel_no"]))
        else:
            item["monitor_key"] = "raw_page/%s/%s/%s" % (item["monitor_id"], item["monitor_date"], item["monitor_title"])#re.sub(r'\s+', '', "raw_page/%s/%s/%s" % (item["monitor_id"], item["monitor_date"], item["monitor_title"]))

        item['content_detail'] = json.dumps(item['content_detail'])#json.dumps({key: re.sub(r'\s+', '', str(item['content_detail'][key])) for key in item['content_detail']})

        query=self.dbpool.runInteraction(self._conditional_insert,item)#调用插入的方法
        #query.addErrback(self._handle_error,asynItem,spider)#调用异常处理方法
        return item

    #写入数据库中
    def _conditional_insert(self,tx,item):
        sql = "INSERT INTO monitor(`crawler_id`, `title`, `key`, `re`, `fixture_date`, `parcel_no`, `content`, `url`, `html`, `detail`) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        params = (item["monitor_id"], item["monitor_title"], item["monitor_key"], item["monitor_re"], item["monitor_date"], item["parcel_no"],item["monitor_content"], item["monitor_url"], item["content_html"], item['content_detail'])
        try:
            #csv_report.output_data(item, "result", method='a')
            tx.execute(sql,params)
            if params:
                log_obj.debug(u"key saved:%s" % item["monitor_key"])
                csv_report.output_data([params,], "NEW", title=[u'爬虫编号', u'标题', u'主键', u'发布日期', u'链接', u'其他内容'], method = "a")
        except:
            log_obj.debug(u"sql insert failed:%s\nINFO:%s" %(item["monitor_key"],traceback.format_exc()))


    #错误处理方法
    def _handle_error(self, failue, item, spider):
        print failue


"""
item['parcel_name'] = ''
item['parcel_location'] = ''
item['offer_area_m2'] = ''
item['offer_area_mu'] = ''
item['purpose'] = ''
item['building_area'] = ''
item['plot_ratio'] = ''
item['starting_price_sum'] = ''
item['floor_starting_price'] = ''
item['starting_price'] = ''
item['transaction_price_sum'] = ''
item['floor_transaction_price'] = ''
item['transaction_price'] = ''
item['premium_rate'] = ''
item['competitive_person'] = ''
item['fixture_date'] = ''
item['type'] = ''
item['plate'] = ''
item['urban_district'] = ''
item['administrative_district'] = ''
item['total_development_area'] = ''
item['building_name'] = ''
item['property_type'] = ''
item['open_date'] = ''
item['commerce_area'] = ''
item['security_housing_area'] = ''
item['enterprise'] = ''
item['enterprise_type'] = ''
item['round'] = ''
item['registed_enterprise'] = ''
item['the_last_enterprise_participate_in'] = ''
item['registed_enterprise_count'] = ''
"""
