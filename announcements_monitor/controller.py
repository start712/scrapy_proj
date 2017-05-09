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

file_name = 'NEW.csv' #新公告将储存在这个文件中

class controller(object):
    def __init__(self):
        self.pymail = pymail.pymail()

    def initialize(self):
        """删除旧的公告发布数据"""
        if os.path.exists(file_name):
            os.remove(file_name)

    def start_spider(self, spider_id):
        """通过cmd命令启动爬虫"""
        if spider_id != "monitor":
            spider_id = " " + spider_id
        os.system("scrapy %s" %spider_id)

    def main(self):
        print u"""
        您正在使用爬虫控制程序.........
        
        ☆设置（不会启动爬虫）：
        1.若需要保留以前的NEW.csv中的数据，请输入save。
        
        ☆启动爬虫：
        1.启动全部爬虫，请输入go。
        2.启动某一个编号的爬虫，请输入go-爬虫编号，例如go-511696。
        """
        commands = set()
        while True:
            print u"命令集合为%s" % commands
            command = raw_input(u"请输入指令：")
            print u"输入的指令为：%s" %command
            commands.add(command)
            if command == 'timer':
                self.timer()

            if command.find('go')>=0: #判断输入命令是否正确
                go_command = command
                if go_command.split('-')[0] != 'go' and go_command != 'go':#判断输入命令是否正确
                    print u"go相关指令输入有误"
                    time.sleep(3)
                    continue
                if go_command == 'go':
                    spider_id = 'monitor'
                else:
                    spider_id = 'crawl ' + go_command.split('-')[-1]
                print u"爬虫%s准备启动中......." %spider_id
                break

        print u"\n命令集合为%s\n" % commands

        # 执行save命令
        if 'save' not in commands:
            self.initialize()
        #else:
        #    csv_size = os.path.getsize(file_name)

        # 启动爬虫
        self.start_spider(spider_id)

        print u'爬虫运行完毕'

        self.report()

        print u"爬取结束！！！！！"
        print u"爬取结束！！！！！"
        print u"爬取结束！！！！！"


    def timer(self):
        while True:
            log_obj.debug(u"Tick!Tick!Tick! Now time is %s" %time.localtime())
            print u"Tick!Tick!Tick! Now time is %s" %time.localtime()
            if 9<=time.localtime().tm_hour<=12:
                # 执行save命令
                self.initialize()

                # 启动爬虫
                self.start_spider('monitor')

                print u'爬虫运行完毕'

                self.report()

                print u"爬取结束！！！！！"
                print u"爬取结束！！！！！"
                print u"爬取结束！！！！！"
            #规定间隔时间
            time.sleep(28800)

    def report(self):

        # 有新内容的话，发送邮件
        report_file = ['NEW.csv', ]
        if os.path.exists(file_name):
            s = ""
            with open(report_file[0], 'rb') as f:
                rows = csv.reader(f)
                for row in rows:
                    if row:
                        s = s + ",".join(row[:3]) + '\n'

            self.pymail.send_mail(report_file, "发现新的公告！！", txt=s, to_mail='619978637@qq.com')

            print u"有新的公告！！！已存入NEW.csv!!!!\n" * 3

        else:
            print u"没有发现新的内容！\n" * 3

        # 发送log
        date0 = datetime.datetime.date(datetime.datetime.today() + datetime.timedelta(days=-1))
        log_file = [
            r'%s/log/sql_update(%s).log' % (os.getcwd(), date0),
            r'%s/log/spider(%s).log' % (os.getcwd(), date0)
        ]
        if os.path.exists(log_file[0]) and os.path.exists(log_file[1]):
            title = "日常报告%s" % date0
            if os.path.getsize(log_file[1]) == 0:
                title = "新的BUG报告%s" % date0

            self.pymail.send_mail(log_file, title, txt="", to_mail='3118734521@qq.com')
            for f in log_file:  # 发送过的文件，改掉文件名，避免多次发送发给
                os.rename(f, f + "_")
        elif os.path.exists(log_file[0] + "_") and os.path.exists(log_file[1] + "_"):
            print u'不需要发送log邮件'
        else:
            self.pymail.send_mail(None, "爬虫报告%s缺少文件" % date0, to_mail='3118734521@qq.com')


if __name__ == '__main__':
    controller = controller()
    controller.main()
