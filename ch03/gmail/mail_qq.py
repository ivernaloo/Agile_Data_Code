#!/usr/bin/env python
#coding:utf-8

import imaplib
import re from pprint import pprint
list_response_pattern = re.compile(r'\((?P<flags>.*?)\) "(?P<delimiter>.*)" (?P<name>.*)')

#imaplib.Debug = 4 #设置debug模式
def parse_list_response(line):
    flags, delimiter, mailbox_name = list_response_pattern.match(line).groups()
    mailbox_name = mailbox_name.strip('"')
    return (flags, delimiter, mailbox_name)


hostname='imap.qq.com' #设置收取腾讯邮件
connection = imaplib.IMAP4_SSL(hostname)  #要求使用SSL加密方式登录  不强制的话可以用imaplib.IMAP4

username = '' #用户名
password = '' #密码
print 'Logging in as', username
connection.login(username, password)  #连接  注意我这个程序没有异常处理
typ,data =  connection.list()  #检索Mailbox
pprint(data)



for line in data:
    flags, delimiter, mailbox_name = parse_list_response(line)
    print 'Parsed response:', (flags, delimiter, mailbox_name)
    print connection.status(mailbox_name, '(MESSAGES RECENT UIDNEXT UIDVALIDITY UNSEEN)') #邮箱状态,MESSAGES:消息数量，RECENT：最近查看的邮件数量，
                                                #在UIDNEXT：邮箱的下一个唯一标识符值,UIDVALIDITY:邮箱的有效性的唯一标识符值,UNSEEN:还没有查看的邮件数量
    connection.select(mailbox_name, readonly=True)  
    typ, msg_ids = connection.search(None, 'ALL')#寻找邮件包含'ALL'数量
    typ, msg_ids = connection.search(None, '( FROM "Doug" SUBJECT "test message 2")')  #寻找邮件主题包含’test message 2‘，发件人是'Doug'的邮件数量
    typ, msg_data = c.fetch('1', '(BODY.PEEK[HEADER] FLAGS)') #
    print mailbox_name, typ, msg_ids    



try:
    connection.select('INBOX', readonly=True)



    print 'HEADER:'
    typ, msg_data = connection.fetch('1', '(BODY.PEEK[HEADER])')
    for response_part in msg_data:
        if isinstance(response_part, tuple):
            print response_part[1]  #打印邮件头内容



    print 'BODY TEXT:'
    typ, msg_data = connection.fetch('1', '(BODY.PEEK[TEXT])')
    for response_part in msg_data:
        if isinstance(response_part, tuple):
            print response_part[1] #打印内容



    print '\nFLAGS:'
    typ, msg_data = connection.fetch('1', '(FLAGS)')
    for response_part in msg_data:
        print response_part
        print imaplib.ParseFlags(response_part)
finally:
    try:
        c.close()
    except:
        pass
connection.logout()