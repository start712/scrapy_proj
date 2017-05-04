# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: __init__.py.py
    @time: 2017/4/18 16:04
--------------------------------
"""
import sys
import os

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"

log_obj = set_log.Logger('__init__.py.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('__init__.py.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件


class __init__.py(object):

    def __init__(self):
        pass


if __name__ == '__main__':
    pass