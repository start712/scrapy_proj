# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: command.py
    @time: 2017/6/23 13:59
--------------------------------
"""
import re
import sys
import os
import sqlite3
import time
import trace
import traceback
import datetime

log_path = r'%s/log/SQLite_Contrl(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
import mysql_connecter

log_obj = set_log.Logger(log_path, set_log.logging.WARNING, set_log.logging.DEBUG)
log_obj.cleanup(log_path, if_cleanup=False)  # 是否需要在每次运行程序前清空Log文件

mysql_con = mysql_connecter.mysql_connecter()
#'CREAT TABLE Double_Check_URL(`id` INT(10) PRIMARY KEY NOT NULL AUTO_INCREMENT, `insert_time` timestamp not null default current_timestamp, `url` VARCHAR(255))'

class command(object):
    def __init__(self):
        pass

    def check_url(self, url):
        sql = "SELECT * FROM `URL_ToCheck` WHERE `url` = \"%s\"" %url
        resp = self.connect(sql)
        m = re.search(r'\([^\(\)]+?\)', str(resp))
        if m:
            return True
        else:
            return False

    def insert_url(self, url):
        sql = "INSERT INTO `URL_ToCheck`(`url`) VALUES(\"%s\")" %url
        self.connect(sql)

    def delete_all(self):
        sql = "DELETE FROM `URL_ToCheck`"
        self.connect(sql)
        print u"\n数据库已经清空\n"

    def delete_url(self, url):
        sql = "DELETE FROM `URL_ToCheck` WHERE `url` = \"%s\"" %url
        self.connect(sql)
        print url, u"\n已从待审核的网址中移除\n"

    def update_status(self, url, status=0):
        sql = "UPDATE `URL_ToCheck` SET `status` = %s WHERE `url` = \"%s\"" %(status, url)
        self.connect(sql)
        print url, u"\n网址的状态已经更改为已复查\n"

    def show_url(self, show_all=False):
        sql = "SELECT * FROM `URL_ToCheck`"
        if show_all:
            sql = sql + "WHERE `status` = 1"
        resp = self.connect(sql)
        print resp
        return resp

    def to_mysql(self):
        #data = tuple([t[1:] for t in self.show_url(True)])
        data = ','.join(['("%s")' %('","'.join(t[1:])) for t in self.show_url(True)])
        sql = "INSERT INTO `待核查spider`(`url`, `status`, `remark`) VALUES%s" %data

        mysql_con.connect(sql, ip='192.168.1.124', user='spider', password='startspider', dbname='raw_data')

    def from_mysql(self):
        sql = "SELECT * FROM `待核查spider`"
        data = mysql_con.connect(sql, ip='192.168.1.124', user='spider', password='startspider', dbname='raw_data')
        print data
        data = ','.join(['("%s")' %('","'.join(t[1:])) for t in data])
        sql = "INSERT INTO `URL_ToCheck`(`url`, `status`, `remark`) VALUES%s" %data

        self.delete_all()
        self.connect(sql)

    def connect(self, sql, db='Data_Ctrl.db'):
        try:
            con = sqlite3.connect(db)
            cur = con.cursor()
            cur.execute(sql)
            resp = cur.fetchall()
            cur.close()
            con.commit()
            con.close()

            return resp
        except sqlite3.IntegrityError:
            pass
        except:
            log_obj.error(traceback.format_exc())


if __name__ == '__main__':
    command = command()

    #print command.connect("DROP TABLE `URL_ToCheck`")
    #print command.connect(r"CREATE TABLE URL_ToCheck(`insert_time` timestamp not null default current_timestamp,"
    #                      " `url` VARCHAR(255) PRIMARY KEY,"
    #                      " `status` CHAR(1) DEFAULT 1,"
    #                      " `remark` VARCHAR(255) DEFAULT '')")

    #print command.connect("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    #print command.insert_url('wtf222')
    #print command.updat e_status('wtf')
    command.show_url()
    command.delete_all()
    #command.from_mysql()
    #command.to_mysql()
    #while True:
    #    time.sleep(3)