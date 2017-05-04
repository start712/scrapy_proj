# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: pymail.py
    @time: 2017/4/19 9:06
--------------------------------
"""
import sys
import os
import csv

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')

import smtplib, mimetypes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

mail_host = "smtp.163.com"
mail_user = "from"
mail_pass = "li90"
mail_postfix = "163.com"


class pymail(object):
    def __init__(self):
        pass

    ######################
    def send_mail(self, file_list, title = "Python邮件", txt = "邮件内容(空)", to_mail = '3118734521@qq.com'):
        msg = MIMEMultipart()
        msg['From'] = "start_spider@sina.com"
        msg['To'] = to_mail
        msg['Subject'] = title

        # 添加邮件内容
        txt = MIMEText(txt)
        msg.attach(txt)

        # 添加二进制附件
        if file_list:
            for fileName in file_list:
                ctype, encoding = mimetypes.guess_type(fileName)
                if ctype is None or encoding is not None:
                    ctype = 'application/octet-stream'
                maintype, subtype = ctype.split('/', 1)
                att1 = MIMEImage((lambda f: (f.read(), f.close()))(open(fileName, 'rb'))[0], _subtype=subtype)
                att1.add_header('Content-Disposition', 'attachment', filename=fileName)
                msg.attach(att1)

        # 发送邮件
        smtp = smtplib.SMTP()
        smtp.connect('smtp.sina.com')
        smtp.login('start_spider', 'start12345')
        smtp.sendmail(msg['From'], msg['To'], msg.as_string())
        smtp.quit()
        print u'to_mail=%s邮件发送成功' %to_mail

if __name__ == '__main__':
    pymail = pymail()
    s = ""
    with open('NEW.csv', 'rb') as f:  # 采用b的方式处理可以省去很多问题
        rows = csv.reader(f)
        for row in rows:
            if row:
                s = s + ",".join(row) + '\n'

    pymail.send_mail("NEW.csv", txt = s )