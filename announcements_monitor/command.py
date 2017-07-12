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

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('command.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('command.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

#'CREAT TABLE Double_Check_URL(`id` INT(10) PRIMARY KEY NOT NULL AUTO_INCREMENT, `insert_time` timestamp not null default current_timestamp, `url` VARCHAR(255))'

class command(object):
    def __init__(self):
        pass

    def check_url(self, url):
        sql = "SELECT * FROM `Double_Check_URL` WHERE `url` = \"%s\"" %url
        resp = self.connect(sql)
        m = re.search(r'\([^\(\)]+?\)', str(resp))
        if m:
            return True
        else:
            return False

    def insert_url(self, url):
        sql = "INSERT INTO `Double_Check_URL`(`url`) VALUES(\"%s\")" %url
        self.connect(sql)

    def delete_url(self, url):
        sql = "DELETE FROM `Double_Check_URL` WHERE `url` = \"%s\"" %url
        self.connect(sql)
        print url, u"\n已从待审核的网址中移除\n"

    def show_url(self):
        sql = "SELECT * FROM `Double_Check_URL`"
        resp = self.connect(sql)
        print resp

    def connect(self, sql, db='Data_Ctrl.db'):
        con = sqlite3.connect(db)
        cur = con.cursor()
        cur.execute(sql)
        resp = cur.fetchall()
        cur.close()
        con.commit()
        con.close()

        return resp


if __name__ == '__main__':
    command = command()
    command.delete_url('sdafsdf')