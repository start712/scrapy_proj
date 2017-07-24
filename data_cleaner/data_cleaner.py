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
import copy

import datetime

import MySQLdb
import pymysql
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
log_obj.cleanup('data_cleaner.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class data_cleaner(object):
    def __init__(self):
        pass

    def get_data(self, length = 100):
        """每次只会读取100条数据，若是长时间没有清洗过数据了，需要更改这个数值
           输出结果格式为pd.DataFrame"""
        # 处理挂牌土地数据
        sql1 = r"SELECT `parcel_no`, `city`, `key`, `detail`, `status`, `fixture_date`, `url` FROM `monitor` WHERE `status` = 'onsell'" # LIMIT %s [length,],
        data1 = self.download_sql(sql1, 'localhost', 'spider')
        data1 = self.standardizing(data1) # 字典转化为DataFrame # 116.62.230.38
        #data1 = self.duplicate_check(data1) # 去除重复地块

        # 同样的方法处理已售数据
        sql2 = r"SELECT `parcel_no`, `city`, `key`, `detail`, `status`, `fixture_date`, `url` FROM `monitor` WHERE `status` = 'sold'" #  %s [length,],
        data2 = self.download_sql(sql2, 'localhost', 'spider')
        data2 = self.standardizing(data2)
        #data2 = self.duplicate_check(data2)

        return data1, data2

    def standardizing(self, df):
        # 获取数据,将detail中的数据格式转化为json
        df0 = df.loc[:,'detail']
        df00 = pd.DataFrame([])
        df0 = df0.fillna('')
        for i in df.index:
            #print df0.loc(axis=0)[i]
            ser = pd.read_json(df0.loc(axis=0)[i], typ='series')
            df00 = df00.append(ser, ignore_index=True)
        df = self.left_join(df, df00, how='left')
        del df['detail'] # 删除detail字段

        # 将parcel_no统一格式
        df['parcel_no'] = [self.clean_parcel_no(s) for s in df['parcel_no'].tolist()]

        return df

    def duplicate_check(self, df):
        # 去除并记录DataFrame中的parcel_no重复的记录并清除
        df.sort_values('fixture_date', ascending=False)
        if np.any(df.duplicated('parcel_no')):
            file_name = u'重复土地爬虫数据%s.csv' %int(time.time())
            print u'发现重复数据，已经输出为%s' %file_name
            df[df.duplicated('parcel_no')].to_csv(os.getcwd() + '\\cleaner_log\\' + file_name, encoding='utf_8_sig')
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
        #del sold_data['addition']
        df = self.left_join(df, sold_data, how='left')
        #df.merge(sold_data, how='left')

        return df

    def left_join(self, left, right, how='left'):
        # 用df的join方法，删除并记录名字重复的列
        df = left.join(right, how=how, lsuffix='', rsuffix='(*DEL*)')
        duplicate_col = [s for s in df.columns if (isinstance(s,str) or isinstance(s,unicode)) and re.search(ur'\(\*DEL\*\)', s)]
        #print [(isinstance(s,str) or isinstance(s,unicode)) and re.search(ur'\(\*DEL\*\)', s) for s in df.columns]


        # 记录被删除的数据中不同的数据
        df0 = pd.DataFrame([])
        for s in duplicate_col:
            ser1 = df[re.sub(ur'\(\*DEL\*\)', '',s)]
            ser2 = df[s]
            df0 = df0.append(pd.DataFrame({'发生时间':datetime.datetime.now(), 'left': ser1[ser1 != ser2], 'right': ser2[ser1 != ser2]}),ignore_index=True)

        df0.to_csv(os.getcwd() + u'\cleaner_log\被删除的数据.csv', encoding='utf_8_sig',mode='a')

        df = df.drop(duplicate_col, 1)
        return df

    def data_sync(self, df):
        sql = 'SELECT * FROM `土地信息spider` WHERE `parcel_no` in (%s)' % (
              ','.join(['"%s"' %s for s in df['parcel_no'].astype(np.str).tolist()]))
        # 旧数据
        data = self.download_sql(sql, '192.168.1.124', 'raw_data')
        data.index = data['parcel_no']
        #data.to_csv(os.getcwd() + '\cleaner_log\(3.1)data_sync.csv', encoding='utf_8_sig')
        # 统一字段
        cols = [s for s in df.columns if s in data.columns]
        print u'data_sync()整理好的数据中的未知字段%s' %[s for s in df.columns if s not in data.columns]
        data = data[cols]
        # 更新旧数据
        data.update(df)

        b = df.index.isin(data.index) # 判断哪些地块已经存在于数据库中
        insert_data = df[b==False] # 原数据中没有的新数据
        data = data.append(insert_data)

        return data[cols]

    def data_clean(self, df):
        df = df.fillna('')
        if 'parcel_no' in df.columns:
            del df['parcel_no']
        df0 = copy.deepcopy(df)

        if 'plot_ratio' in df.columns:
            df['plot_ratio'] = df['plot_ratio'].apply(self.plot_ratio_cleaner)
        if 'starting_price_sum' in df.columns:
            df['starting_price_sum'] = df['starting_price_sum'].apply(lambda x:re.search(ur'\d+[\.]*\d*', x).group() if isinstance(x,unicode) and re.search(ur'\d+[\.]*\d*', x) else x)
        if 'offer_area_m2' in df.columns:
            df['offer_area_m2'] = df['offer_area_m2'].apply(lambda x:re.sub(r'\s+', '', x) if isinstance(x,unicode) and re.search(ur'\d+[\.]*\d*', x) else x)
            df['offer_area_m2'] = df['offer_area_m2'].apply(lambda x:re.search(ur'\d+[\.]*\d*', x).group() if isinstance(x,unicode) and re.search(ur'\d+[\.]*\d*', x) else x)
        if 'building_area' in df.columns:
            df['building_area'] = df['building_area'].apply(self.building_area_cleaner)
        if 'transaction_price_sum' in df.columns:
            df['transaction_price_sum'] = df['transaction_price_sum'].apply(lambda x:re.search(ur'\d+[\.]*\d*', x).group() if isinstance(x,unicode) and re.search(ur'\d+[\.]*\d*', x) else x)
        if 'addition' in df.columns:
            df['addition'] = df['addition'].apply(lambda x:','.join(map(lambda x0,y0:'%s:%s' %(x0,y0),x.viewkeys(),x.viewvalues()) if isinstance(x,dict) else ''))


        self.check_diff(df0,df) # 输出修改日志
        return df

    def plot_ratio_cleaner(self, s):
        res = -100
        s = str(s)
        # (1)搜索所有带百分号的数字，然后返回最大值除以100
        comp = re.compile(r'\d+(?=%)')
        if comp.search(s):
            res = float(max(comp.findall(s)))/100
        # (2)搜索【数字+“号”字】的模式，返回空白
        comp = re.compile(r'\d+号')
        if comp.search(s):
            return ''
        # (3)搜索所有可能带小数点的数字，以数组形式返回，若数组中数字个数大于2个，
        # 返回空白，否则返回最大的那个数字
        comp = re.compile(r'\d+\.*\d*')
        if comp.search(s):
            if len(comp.findall(s)) > 2:
                return ''
            else:
                res = max(comp.findall(s))
        # (4)若有结果，但是结果大于10，返回空白
        res = float(res)
        if res < 0 or res > 10:
            return ''
        return res

    def building_area_cleaner(self, s):
        s = str(s)
        # (1)搜索带有地上面积字符串
        m = re.search(ur'(?<=地上).*?\d+[\.]*\d*', s)
        if m:
            return re.search(ur'\d+[\.]*\d*', m.group()).group()
        # (2)搜索所有带小数点的数字，返回最大值
        if re.search(u'\d+[\.]*\d*', s):
            return float(max(re.findall(u'\d+[\.]*\d*', s)))
        else:
            return s

    def check_diff(self,old,new):
        old.to_csv(os.getcwd() + ur'\cleaner_log\old.csv', encoding='utf_8 _sig')
        new.to_csv(os.getcwd() + ur'\cleaner_log\new.csv', encoding='utf_8 _sig')
        log_df = pd.DataFrame({})
        b = old==new
        for col in new.columns:
            #print col, np.all(np.array(b[col]))
            if not np.all(np.array(b[col])):
                df0 = pd.DataFrame([new[col][b[col]==False],old[col][b[col]==False]]).T
                df0.columns = ['new', 'old']
                log_df = log_df.append(df0)
        log_df.to_csv(os.getcwd() + ur'\cleaner_log\data_clean_log.csv', encoding='utf_8 _sig')

    def mysql_ctrl(self, sql, args=None):
        with closing(pymysql.connect(host='192.168.1.124', user='spider', password='startspider',
                                     database='raw_data',  charset='utf8')) as conn:
            with closing(conn.cursor()) as cur:
                #cur.executemany(sql, args)
                cur.execute(sql)

    def download_sql(self, sql, host, database):
        with closing(pymysql.connect(host=host, user='spider',password='startspider',
                                     database=database, charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        return df

    def insert_sql(self, df):
        df = df.fillna('') # 空白字符填充空值

        # 三个部分的SQL代码

        title = df.columns.tolist()
        sql0 = ','.join(title)

        #arr = np.ones(df.shape).astype(str)
        #arr[arr=='1.0'] = '%s'
        # 将二维列表转化为 ('',''),('',''),('','')
        sql1 = ','.join(['(%s)' %(','.join(['"%s"' %s for s in l])) for l in np.array(df).tolist()])

        sql2 = ','.join(['%s=VALUES(%s)' %(s,s) for s in title])

        sql = "INSERT INTO `土地信息spider`(%s) VALUES%s ON DUPLICATE KEY UPDATE %s" %(sql0, sql1, sql2)

        data = np.array(df).astype(str).tolist()
        #pd.DataFrame(arr).to_csv('arr.csv')
        #pd.DataFrame(data).to_csv('df.csv')
        return sql

    def mark_sql(self,df):
        pass

    def main(self):
        onsell_data, sold_data = self.get_data() # 获取待售和已售数据
        onsell_data.to_csv(os.getcwd() + '\cleaner_log\(1.1)onsell_data.csv', encoding='utf_8_sig')
        sold_data.to_csv(os.getcwd() + '\cleaner_log\(1.2)sold_data.csv', encoding='utf_8_sig')
        # 将同一地块的待售数据和已售数据整合在一起
        data = self.merging_data(onsell_data, sold_data) #(onsell_data.iloc[:, :5], sold_data.iloc[:, :5])
        data.to_csv(os.getcwd() + '\cleaner_log\(2)merging_data.csv', encoding='utf_8_sig')

        # 下载旧数据，地块编号已存在的话，更新，没有则添加
        data = self.data_sync(data)
        data.to_csv(os.getcwd() + '\cleaner_log\(3)data_sync.csv', encoding='utf_8_sig')

        data = self.data_clean(data)
        data.to_csv(os.getcwd() + '\cleaner_log\(4)data_clean.csv', encoding='utf_8_sig')

        # 组织sql语言，上传整理好的数据
        sql = self.insert_sql(data)
        log_obj.debug(sql)
        #mysql_connecter.connect(sql, dbname='raw_data', ip='192.168.1.124', user='spider', password='startspider')
        #insert_sql = self.insert_sql(insert_data)

        # 将已清洗的数据做上标记
        self.mark_sql(data)


if __name__ == '__main__':
    data_cleaner = data_cleaner()
    data_cleaner.main()
    #a,b = data_cleaner.get_data()
