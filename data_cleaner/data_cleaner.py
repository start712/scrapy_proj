# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: data_cleaner.py
    @time: 2017/5/9 9:46
--------------------------------
"""
import re
import sys
import os
import json
import traceback
import datetime

import datetime

import MySQLdb
import numpy as np
import pandas as pd
import time
from contextlib import closing

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
import mysql_connecter

mysql_connecter = mysql_connecter.mysql_connecter()

log_obj = set_log.Logger('data_cleaner.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('data_cleaner.log', if_cleanup=False)  # 是否需要在每次运行程序前清空Log文件


class data_cleaner(object):
    def __init__(self):
        used_data = []

    def get_data(self, length = 100):
        """每次只会读取100条数据，若是长时间没有清洗过数据了，需要更改这个数值
           输出结果格式为pd.DataFrame"""
        # 处理挂牌土地数据
        sql1 = r"SELECT `key`, `detail`, `status`, `fixture_date`, `url` FROM `monitor` WHERE `status` = 'onsell'" # LIMIT %s [length,],
        data1 = mysql_connecter.connect(sql1, dbname='spider', ip='localhost', user='spider', password='startspider')
        data1 = self.standardizing(data1) # 字典转化为DataFrame # 116.62.230.38
        #data1 = self.duplicate_check(data1) # 去除重复地块

        # 同样的方法处理已售数据
        sql2 = r"SELECT `key`, `detail`, `status`, `fixture_date`, `url` FROM `monitor` WHERE `status` = 'sold'" # LIMIT %s [length,],
        data2 = mysql_connecter.connect(sql2, dbname='spider', ip='localhost', user='spider', password='startspider')
        data2 = self.standardizing(data2)
        #data2 = self.duplicate_check(data2)

        return data1, data2

    def standardizing(self, data):
        # 获取数据,将detail中的数据格式转化为json
        detail = {t[0]: json.loads(t[1]) for t in data}
        data = {t[0]: {'status':t[2], 'fixture_date':t[3], 'url':t[4]} for t in data}
        for key in data:
            data[key].update(detail[key])

        df = pd.DataFrame(data)
        df = df.stack().swaplevel().unstack() #行列交换，不知道还有没有更好的方法

        # 将parcel_no统一格式
        df['parcel_no'] = [self.clean_parcel_no(s) for s in df['parcel_no'].tolist()]

        return df

    def duplicate_check(self, df):
        # 去除并记录DataFrame中的parcel_no重复的记录并清除
        df.sort_values('fixture_date', ascending=False)
        if np.any(df.duplicated('parcel_no')):
            file_name = u'重复土地爬虫数据%s.csv' %int(time.time())
            print u'发现重复数据，已经输出为%s' %file_name
            df[df.duplicated('parcel_no')].to_csv(os.getcwd() + '\\' + file_name)
            df = df.drop_duplicates('parcel_no')
        return df

    def clean_parcel_no(self, s):
        #print s, '------', type(s.decode('utf8')), '--', type(u'')
        if type(s) == type(u''):
            r1 = re.compile(ur'[【〔\(（]+')
            r2 = re.compile(ur'[】〕\)）]+')
            s = r1.sub('[', s)
            s = r2.sub(']', s)

        return s

    def merging_data(self, onsell_data, sold_data):
        """将待售数据与已售数据合并到一个DataFrame中"""
        # 将DataFrame的行标题替换成parcel_no，便于操作
        #onsell_data.set_axis(0, onsell_data['parcel_no'])
        onsell_data.index = onsell_data['parcel_no'].tolist()
        onsell_data = self.duplicate_check(onsell_data)
        #sold_data.set_axis(0, sold_data['parcel_no'])
        sold_data.index = sold_data['parcel_no'].tolist()
        sold_data = self.duplicate_check(sold_data)

        df = pd.DataFrame(onsell_data) # 重新创建一个对象
        df.update(sold_data)  # 用已售数据覆盖代收数据
        df = df.merge(sold_data, on=u'parcel_no', how='left') # 若待售数据缺少已售数据中的一些字段，补上

        return df

    def data_classify(self, df):
        sql = 'SELECT `parcel_no` FROM `monitor`'
        existed_data = self.download_sql(sql) #从数据库中读取全部地块编号，方法有待改进

        b = df.isin(existed_data) # 判断哪些地块已经存在于数据库中
        update_data = df[b==True]
        insert_data = df[b==False]
        return update_data, insert_data

    def upload_sql(self, df):
        with closing(MySQLdb.connect(dbname='raw_data', ip='192.168.1.124', user='spider',
                                     password='startspider', charset='utf8')) as con:
            df.to_sql('土地信息spider', con, if_exists='append')

    def download_sql(self, sql):
        with closing(MySQLdb.connect(dbname='spider', ip='localhost', user='spider',
                             password='startspider', charset='utf8')) as con:
            df = pd.read_sql(sql, con)
        return df

    def main(self):
        onsell_data, sold_data = self.get_data() # 获取待售和已售数据
        data = self.merging_data(onsell_data, sold_data) # 将同一地块的待售数据和已售数据整合在一起

        # 将数据库中已经有的地块添加至update_data，仅更新数据
        # 将数据库中没有的地块添加至insert_data，插入数据库中
        update_data, insert_data = self.data_classify(data)





if __name__ == '__main__':
    data_cleaner = data_cleaner()
    data_cleaner.main()
    #a,b = data_cleaner.get_data()
