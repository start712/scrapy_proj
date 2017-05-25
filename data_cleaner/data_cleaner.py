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
        sql = r"SELECT `key`, `detail` FROM `monitor` WHERE `parcel_no` <> ''" # LIMIT %s [length,],
        data = mysql_connecter.connect(sql,  dbname='spider', ip='116.62.230.38', user='spider', password='startspider')
        return data

    def duplicate_check(self, data):
        # 由于此处是一条sql代码上传多条数据，所以要确保这条sql代码里的数据没有重复
        d = {}
        key_list = data.keys()
        for key in key_list:
            # 没有地块编号的不做处理
            if 'parcel_no' not in data[key]:
                continue

            parcel_no = data[key]['parcel_no']
            if parcel_no not in d:
                d[parcel_no] = key
            else:
                # 以已售的数据为主
                if data[d[parcel_no]]['status'] == 'sold':
                    new_data = data[d[parcel_no]]
                    old_data = data[key]
                else:
                    new_data = data[key]
                    old_data = data[d[parcel_no]]
                # 数据取出来后，删除原有数据
                data.pop(key)
                data.pop(d[parcel_no])

                # 需要更新数据，只要是成交的数据都试图更新一遍
                d0 = {} # 创建新的数据字典
                for k0 in set(new_data.keys() + old_data.keys()):
                    # 新旧数据不一致，记录下载
                    if k0 in old_data.keys() and k0 in new_data.keys() and old_data[k0] != new_data[k0]:
                        with open(os.getcwd() + '\cleaner_log\changed_data.txt', 'a') as f:
                            f.write('时间：%s\n地块%s中的"%s"新旧数据有出入，已替换成新数据\n' % (
                            datetime.datetime.now(), new_data['parcel_no'], k0))
                    # 往字典里写入数据，若新旧数据都有，则用新数据
                    if k0 in old_data.keys():
                        d0[k0] = old_data[k0]
                    if k0 in new_data.keys():
                        d0[k0] = new_data[k0]
                data[key] = d0
            print key

        return data

    def data_calculate(self, d):
        if not d:
            return None
        # 出让面积（㎡）
        if not d['offer_area_mu'] and d['offer_area_m2']:
            d['offer_area_mu'] = float(d['offer_area_m2']) * 0.0015

        # 建筑面积（㎡）
        if not d['building_area'] and d['offer_area_m2'] and d['plot_ratio'] :
            d['building_area'] = float(d['offer_area_m2']) * float(d['plot_ratio'])

        # 楼面起价（元/㎡）
        if not d['floor_starting_price'] and d['starting_price_sum'] and d['building_area'] :
            d['floor_starting_price'] = float(d['starting_price_sum']) * 10000.0 / float(d['building_area'])

        # 起始单价
        if not d['starting_price'] and d['starting_price_sum'] and d['offer_area_m2'] :
            d['starting_price'] = float(d['starting_price_sum']) / (float(d['offer_area_m2']) * 0.0015)

        # 成交楼面价（元/㎡）
        if not d['floor_transaction_price'] and d['transaction_price_sum'] and d['building_area'] :
            d['floor_transaction_price'] = float(d['transaction_price_sum']) * 10000 / float(d['building_area'])

        # 成交单价
        if not d['transaction_price'] and d['transaction_price_sum'] and d['offer_area_m2'] :
            d['transaction_price'] = float(d['transaction_price_sum']) / float(d['offer_area_m2']) * 666.67

        # 溢价率
        if not d['premium_rate'] and d['transaction_price_sum'] and d['starting_price_sum']:
            d['premium_rate'] = (float(d['transaction_price_sum'])-float(d['starting_price_sum'])) / float(d['starting_price_sum'])
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

    def set_method(self, new_data):
        """
        :param new_data: 字典
        :return: 
        """
        if 'parcel_no' not in new_data:
            return None, 0
        keys = list(new_data.viewkeys())
        key_str = ','.join(keys)
        # 确保sql代码中的数据与数据库中的数据不重复
        # 有待商榷：是每次都调用数据库来判断地块已经存在了，还是一起次先取出所有地块的编号，在Python中查找
        check_sql = "SELECT %s FROM `raw_data`.`土地信息spider` WHERE `parcel_no` = %s" %(key_str, r'%s')
        res = mysql_connecter.connect(check_sql, [new_data['parcel_no'],], dbname='raw_data', ip='192.168.1.124', user='spider', password='startspider')
        if res:
            #已经有旧数据了
            res = res[0]

            # 需要更新数据，只要是成交的数据都试图更新一遍
            if new_data['status'] == 'sold':
                old_data = {keys[i]: res[i] for i in xrange(len(keys))}  # 这块地的旧数据
                parcel_no = new_data['parcel_no']
                for key in keys:
                    # 旧数据里有而新数据里没有的，写入新数据里
                    if old_data[key] != '' and new_data[key] == '':
                        new_data[key] = old_data[key]

                    if old_data[key] != new_data[key]:
                        new_data[key] = old_data[key]
                        with open(os.getcwd() + '\cleaner_log\changed_data.txt', 'a') as f:
                            f.write('时间：%s\n地块%s中的"%s"新旧数据有出入，已替换成新数据\n' %(datetime.datetime.now(), new_data['parcel_no'], key))
                return new_data, 'update'
            else:
                with open(os.getcwd() + '\cleaner_log\duplicate_onsell.txt', 'a') as f:
                    f.write('时间：%s\n地块%s已经存在旧在售数据，没有写入新的在售数据' % (datetime.datetime.now(), new_data['parcel_no']))
                return None, 0
        else:
            # 没有旧数据，需要插入数据
            return new_data, 'insert'

    def output_update_sql(self, l):
        """
        生成器
        :param l: [{},{},{}] 输入包含字典的列表 
        :return: 每个字典中不同key组成的sql语句
        """
        #print 'output_update_sql1:', l
        l = [{k:d[k] for k in d if d[k]} for d in l]
        #print 'output_update_sql2:', l

        d0 = {}
        res = []
        # 生成第一级为各个需要更新的字段名的json字典，第二级为对应字段下parcel_no对应的数据
        for d in l:
            for k in d:
                if k not in d0:
                    d0[k] = {}
                d0[k][d['parcel_no']] = d[k]

        # 对每个字段生成一个sql update代码
        for key in d0:
            sql0 = ""
            sql1 = []
            data0 = []
            blank0 = []
            for k0 in d0[key]:
                sql0 = sql0 + "WHEN %s THEN %s\n"
                sql1.append(r'%s')
                data0.append(k0)
                data0.append(d0[key][k0])
                blank0.append(k0)

            sql = """
                  UPDATE raw_data.`土地信息spider`
                  SET 
                  %s = CASE parcel_no
                  %s
                  END 
                  WHERE parcel_no in (%s)
                  """ %(key, sql0, ','.join(sql1))
            data0.extend(blank0)
            yield  [sql, data0, key]

    def clean_building_area(self, s):
        r = re.compile(r'[\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+'.decode('utf8'))
        m = r.search(s.decode('utf8'))
        if m:
            l = r.split(s)
            for s0 in l:
                m = re.search('地上'.decode('utf8'), s0.decode('utf8'))
                if m:
                    return re.search('\d+', s).group(), s
            return s, None
        else:
            return s, None

    def clean_num(self, s):
        return re.sub(r'[^\d\.]', '', s)

    def main(self):
        # 获取数据
        data = {row[0]: json.loads(row[1]) for row in self.get_data()}
        data = self.duplicate_check(data)
        with open('list_structure.txt', 'r') as f:
            list_structure = f.read()
            list_structure = list_structure.split(',')

        update_list = []
        insert_list = []
        update_row_count = 0
        insert_row_count = 0
        #method = 0
        for key in data:
            #补全表结构
            data0 = {} # data0是一行数据中，以字段名为key的字典
            # 先补满字段
            for s in list_structure:
                #print data[key]
                if s in data[key]:
                    data0[s] = data[key][s]
                else:
                    data0[s] = ''
            #print key
            data0['spider_key'] = key

            # parcel_no字段括号更新
            data0['parcel_no'] = self.clean_parcel_no(data0['parcel_no'])

            # 建筑面积往往是数字与文字的混合，需要去除数字
            data0['building_area'], a0 = self.clean_building_area(data0['building_area'])
            if a0: # 若提取过数字，把原文放在备注栏中
                if 'addition' not in data0 or type(data0['addition']) != type({}):
                    data0['addition'] = {}
                data0['addition']['建筑面积'] = a0

            # addition字段特殊处理
            data0['addition'] = self.dict2str(data0['addition'])

            # 判定数据在数据库中的处理方法
            data0, method = self.set_method(data0)

            # 进行相关的数据计算
            try:
                data0 = self.data_calculate(data0)
            except:
                log_obj.error("地块%s的数据结构有错误：\n%s" %(data0['parcel_no'], traceback.format_exc()))

            # 将数据输入列表，备用
            if method == 'update':
                update_list.append(data0)  # 每条数据字典存在列表中
                update_row_count += 1
            elif method == 'insert':
                insert_list.extend(list(data0.viewvalues()))
                insert_row_count += 1
            else:
                log_obj.debug(u"sql code => None")

        list_col_name = list({s: "" for s in list_structure}.viewkeys()) #获取字段在字典中作为key的顺序
        col_len = len(list_col_name)
        row_blank = '(%s)' %(','.join([r'%s',] * col_len)) #(%s, %s.......) # 一行数据的%s符号
        #data_blank = ','.join([data_blank,] * row_count) #(%s, %s...),(%s, %s...),(%s, %s...),(%s, %s...)

        # 批量更新数据
        # 调整需要更新的数据的结构
        if update_list:
            update_sql_data_list = self.output_update_sql(update_list)
            for l in update_sql_data_list:
                sql, data, col = l
                #print sql
                #print data
                try:
                    mysql_connecter.connect(sql, data, dbname='raw_data', ip='192.168.1.124', user='spider', password='startspider')
                    print u"更新MySQL（%s字段）成功！" %col
                except:
                    log_obj.debug(u"sql update failed:\n%s" % traceback.format_exc())

        # 批量插入数据
        if insert_list:
            data_blank = ','.join([row_blank, ] * insert_row_count)  # (%s, %s...),(%s, %s...),(%s, %s...),(%s, %s...)
            sql = "INSERT INTO `土地信息spider`(%s) VALUES%s;" %(','.join(list_col_name), data_blank)#%(sql_col_name,sql_data)
            try:
                mysql_connecter.connect(sql, insert_list, dbname='raw_data', ip='192.168.1.124', user='spider', password='startspider')
                print u"上传MySQL成功！"
            #except MySQLdb.IntegrityError:
            #    pass
            except:
                print u"数据上传失败！！"
                print u"sql insert failed:\n%s" %traceback.format_exc()

if __name__ == '__main__':
    data_cleaner = data_cleaner()
    data_cleaner.main()