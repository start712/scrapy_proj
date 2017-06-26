# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: spider_error.py
    @time: 2017/6/26 9:48
--------------------------------
"""
import sys
import os
import datetime
import SQLite_Contrl

log_path = r'%s/log/spider_DEBUG(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
import csv_report

log_obj = set_log.Logger(log_path, set_log.logging.WARNING, set_log.logging.DEBUG)
log_obj.cleanup(log_path, if_cleanup=False)  # 是否需要在每次运行程序前清空Log文件
csv_report = csv_report.csv_report()

log_obj = set_log.Logger('spider_error.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('spider_error.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

com = SQLite_Contrl.command()

class spider_error(object):
    def __init__(self):
        pass

    def update_log(self, url, s):
        if com.check_url(url):
            print url, u'已经存在于网址数据库SQLite中'
        else:
            log_obj.error(s)

if __name__ == '__main__':
    pass