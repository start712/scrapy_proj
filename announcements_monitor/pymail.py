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

from email import encoders, MIMEBase, MIMEMultipart
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
import smtplib


class pymail(object):
    def __init__(self):
        pass

    def try_send_mail(self, file_list, title = "Python邮件", txt = "邮件内容(空)", to_mail = '3118734521@qq.com'):
        while True:
            try:
                self.send_mail(file_list, title, txt, to_mail)
                break
            except:
                pass

    def _format_addr(self, s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(),addr.encode('utf-8') if isinstance(addr, unicode) else addr))


    def send_mail(self, file_list, title = "Python邮件", txt = "邮件内容(空)", to_mail = '3118734521@qq.com'):
        print u'准备发送邮件给%s' %to_mail
        from_addr = 'start712@aliyun.com'
        password = 'start12345'
        to_addr = to_mail
        smtp_server = 'smtp.aliyun.com'

        msg = MIMEMultipart.MIMEMultipart()
        msg['From'] = self._format_addr(from_addr)
        msg['To'] = self._format_addr(to_addr)
        msg['Subject'] = Header(title, 'utf-8').encode()
        # 邮件正文是MIMEText:
        msg.attach(MIMEText(txt, 'plain', 'utf-8'))

        if file_list:
            for file_name in file_list:
                with open(file_name, 'rb') as f:
                    # 设置附件的MIME和文件名，这里是png类型:
                    mime = MIMEBase.MIMEBase('NEW', 'csv', filename=file_name)
                    # 加上必要的头信息:
                    mime.add_header('Content-Disposition', 'attachment', filename=file_name)
                    mime.add_header('Content-ID', '<0>')
                    mime.add_header('X-Attachment-Id', '0')
                    # 把附件的内容读进来:
                    mime.set_payload(f.read())
                    # 用Base64编码:
                    encoders.encode_base64(mime)
                    # 添加到MIMEMultipart:
                    msg.attach(mime)

        #server = smtplib.SMTP(smtp_server, 25)
        server = smtplib.SMTP_SSL(smtp_server, 465)
        server.set_debuglevel(1)
        server.ehlo()
        server.login(from_addr, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
        print u'邮件发送成功'

if __name__ == '__main__':
    pymail = pymail()
    #pymail.try_send_mail(None, "爬虫报告缺少文件", to_mail='736941030@qq.com')