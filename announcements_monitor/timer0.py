# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: mytimer.py
    @time: 2017/7/20 11:18
--------------------------------
"""
import sys
import os

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import mytimer
mytimer = mytimer.mytimer()

class timer0(object):
    def __init__(self):
        pass
    def main(self):
        mytimer.cmd_timer('python %s' %(os.getcwd() + r'\controller.py'), '9:00', 60*60*24)

if __name__ == '__main__':
    timer0 = timer0()
    timer0.main()
