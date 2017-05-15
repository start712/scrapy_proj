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
        """每次只会读取100条数据，若是长时间没有清洗过数据了，需要更改这个数值"""
        sql = "SELECT `key`, `detail` FROM `monitor` LIMIT %s"
        data = mysql_connecter.connect(sql, [length,], dbname='spider', ip='116.62.230.38', user='spider', password='startspider')
        return data

    def data_calculate(self, d):
        return d

    def clean_parcel_no(self, parcel_no):
        #找出字符串中那些字符需要改
        parcel_no = parcel_no.decode('utf8')
        mark_list_left = set(u'【〔(（') & set(parcel_no)
        mark_list_right = set(u'】〕)）') & set(parcel_no)

        try:
            if mark_list_left:
                if '(' in mark_list_left:
                    mark_list_left.add('\(')
                    mark_list_left.discard('(')
                r = re.compile(u'|'.join(mark_list_left))
                parcel_no = r.sub(u'[', parcel_no)
            if mark_list_right:
                if ')' in mark_list_right:
                    mark_list_right.add('\)')
                    mark_list_right.discard(')')
                r = re.compile(u'|'.join(mark_list_right))
                parcel_no = r.sub(u']', parcel_no)
        except:
            log_obj.error('清洗土地编号出错parcel_no:%s mark_list_left:%s mark_list_right:%s \n%s' %(parcel_no,mark_list_left,mark_list_right,traceback.format_exc()))

        return parcel_no#.encode('utf8')

    def dict2str(self, d):
        """将字典的内容输出成字符串，便于在MySQL中显示出来"""
        l = []
        for k in d:
            l.append("%s:%s" %(k,d[k]))
        return ','.join(l)

    def set_method(self, new_data, keys):
        if 'parcel_no' not in new_data:
            return None, 0
        key_str = ','.join(keys)
        check_sql = "SELECT %s FROM `raw_data`.`土地信息spider` WHERE `parcel_no` = %s" %(key_str, r'%s')
        res = mysql_connecter.connect(check_sql, [new_data['parcel_no'],])[0]
        if res:
            #update_dict = {}
            # 需要更新数据
            for key in keys:
                if res[key] != '' and new_data[key] == '':
                    new_data[key] = res[key]
                    #update_dict[]
                if res[key] != new_data[key]:
                    new_data[key] = res[key]
                    with open('\cleaner_log\changed_data.txt', 'a') as f:
                        f.write('时间：%s\n地块%s中的"%s"新旧数据有出入，已替换成新数据\n' %(datetime.datetime.now(), new_data['parcel_no'], key))
            return new_data, 'update'
        else:
            # 需要插入数据
            return new_data, 'insert'

    def main(self):
        # 获取数据
        data = {row[0]: json.loads(row[1]) for row in self.get_data()}
        with open('list_structure.txt', 'r') as f:
            list_structure = f.read()
            list_structure = list_structure.split(',')

        data0 = {}
        update_list = []
        insert_list = []
        update_row_count = 0
        insert_row_count = 0
        method = 0
        for key in data:
            #补全表结构
            data0 = {}
            for s in list_structure:

                if s in data[key]:
                    data0[s] = data[key][s]
                else:
                    data0[s] = ''

                if s == 'spider_key':
                    data0[s] = key

                # 有待商榷：是每次都调用数据库来判断地块已经存在了，还是一起次先取出所有地块的编号，在Python中查找
                # parcel_no字段括号更新
                if s == 'parcel_no':
                    data0[s] = self.clean_parcel_no(data0[s])
                    #check_sql = "SELECT `parcel_no` FROM `raw_data`.`土地信息spider` WHERE `parcel_no` = %s"
                    #res = mysql_connecter.connect(check_sql, [data0[s],])

                # addition字段特殊处理
                if s == 'addition':
                    data0[s] = self.dict2str(data0[s])

                data0, method = self.set_method(data0, list_structure)

            # 进行相关的数据计算
            data0 = self.data_calculate(data0)

            # 将数据输入列表，备用
            if method == 'update':
                update_list.append(list(data0.viewvalues()))  #每条数据存在不同子列表中
                update_row_count += 1
            elif method == 'insert':
                insert_list.extend(list(data0.viewvalues()))
                insert_row_count += 1
            else:
                log_obj.debug(u"sql code => None")

        list_col_name = list(data0.viewkeys())
        col_len = len(list_col_name)
        row_blank = '(%s)' %(','.join([r'%s',] * col_len)) #(%s, %s.......) # 一行数据的%s符号
        #data_blank = ','.join([data_blank,] * row_count) #(%s, %s...),(%s, %s...),(%s, %s...),(%s, %s...)

        # 批量更新数据
        """
        UPDATE raw_data.`土地信息spider`
SET 
parcel_name = CASE parcel_no
WHEN '萧政储出[2017]4号' THEN 'value' 
WHEN '富政储出[2016]10号' THEN 'value' 
END 
WHERE parcel_no in ('萧政储出[2017]4号', '富政储出[2016]10号')

        # 批量插入数据
        data_blank = ','.join([row_blank, ] * insert_row_count)  # (%s, %s...),(%s, %s...),(%s, %s...),(%s, %s...)
        sql = "INSERT INTO `土地信息spider`(%s) VALUES%s;" %(','.join(list_col_name), data_blank)#%(sql_col_name,sql_data)


        try:
            mysql_connecter.connect(sql, data_list, dbname='raw_data', ip='192.168.1.124', user='spider', password='startspider')
            print u"上传MySQL成功！"
        except MySQLdb.IntegrityError:
            pass
        else:
            log_obj.debug(u"sql insert failed:\n%s" %traceback.format_exc())

"""
if __name__ == '__main__':
    data_cleaner = data_cleaner()
    data_cleaner.main()