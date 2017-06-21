# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: data_reader.py
    @time: 2017/6/21 9:07
--------------------------------
"""
import sys
import os
import json

import pandas as pd

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
import mysql_connecter
log_obj = set_log.Logger('data_reader.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('data_reader.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class data_reader(object):
    def __init__(self):
        self.mysql_con = mysql_connecter.mysql_connecter()

    def get_data(self):
        sql = "SELECT `fixture_date`, `detail` FROM `monitor`"
        data = self.mysql_con.connect(sql, dbname='spider', ip='localhost', user='spider', password='startspider')

        return data

    def data_to_csv(self):
        data = self.get_data()
        df = pd.DataFrame({}, index=[])
        for fixture_date, detail in data:
            addition = ''
            detail = json.loads(detail)
            if type(detail) != type({}):
                print "wrong type of data:", detail
                continue

            if 'addition' in detail:
                d = detail['addition']
                l = ['%s:%s' %(key,d[key]) for key in d]
                addition = ';'.join(l)

            detail['fixture_date'] = fixture_date
            if addition:
                detail['addition'] = addition
            df = df.append(detail, ignore_index=True)
        df.to_csv('data.csv', encoding='gbk')

if __name__ == '__main__':
    data_reader = data_reader()
    data_reader.data_to_csv()