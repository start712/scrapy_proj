# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: controller.py
    @time: 2017/4/18 13:54
--------------------------------
"""
import sys
import os
import datetime

import re

import pymail
import time
import csv


sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('controller.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('controller.log', if_cleanup=False)  # 是否需要在每次运行程序前清空Log文件

csv_file = os.getcwd() + r'\log\NEW(%s).csv' %datetime.datetime.date(datetime.datetime.today())
print 'csv_file',csv_file
 #新公告将储存在这个文件中

class controller(object):
    def __init__(self):
        self.pymail = pymail.pymail()

    def start_spider(self, spider_id):
        """通过cmd命令启动爬虫"""
        if spider_id != "monitor":
            spider_id = " " + spider_id
        os.system("scrapy %s" %spider_id)

    def min_run(self):
        # 启动爬虫
        self.start_spider('monitor')

        print u'爬虫运行完毕'

        self.report()

    def report(self):

        # 有新内容的话，发送邮件
        report_file = [csv_file, ]
        if os.path.exists(csv_file):
            s = ""
            with open(report_file[0], 'rb') as f:
                rows = csv.reader(f)
                for row in rows:
                    if row:
                        if re.search(ur'工', row[0]):
                            continue
                        s = s + ",".join(row) + '\n'

            self.pymail.try_send_mail(report_file, "发现新的公告！！", txt=s, to_mail='619978637@qq.com')
            self.pymail.try_send_mail(report_file, "发现新的公告！！", txt=s, to_mail='736941030@qq.com')
            self.pymail.try_send_mail(report_file, "发现新的公告！！", txt=s, to_mail='3118734521@qq.com')

            print u"有新的公告！！！已存入NEW.csv!!!!\n" * 3

        else:
            print u"没有发现新的内容！\n" * 3

        # 发送log
        date0 = datetime.datetime.date(datetime.datetime.today() + datetime.timedelta(days=-1))
        log_file = [
            r'%s/log/pipelines_error(%s).log' % (os.getcwd(), date0),
            r'%s/log/spider_DEBUG(%s).log' % (os.getcwd(), date0)
        ]
        if os.path.exists(log_file[0]) and os.path.exists(log_file[1]):
            title = "日常报告%s" % date0
            if os.path.getsize(log_file[1]) == 0:
                title = "新的BUG报告%s" % date0

            self.pymail.try_send_mail(log_file, title, txt="", to_mail='3118734521@qq.com')
            for f in log_file:  # 发送过的文件，改掉文件名，避免多次发送发给
                os.rename(f, f + "_")
        elif os.path.exists(log_file[0] + "_") and os.path.exists(log_file[1] + "_"):
            print u'不需要发送log邮件'
        else:
            self.pymail.try_send_mail(None, "爬虫报告%s缺少文件" % date0, to_mail='3118734521@qq.com')


if __name__ == '__main__':
    controller = controller()
    controller.min_run()
