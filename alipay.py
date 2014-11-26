#!/usr/bin/env python
# -*- coding: utf-8 -*-
#---------------------------------------
#   程序：支付宝订单列表爬虫
#   版本：0.1
#   作者：cinwell
#   日期：2014-11-25
#   语言：Python 2.7
#---------------------------------------

import urllib2,MySQLdb, MySQLdb.cursors, base64, re, time, threading
#连接数据库查询cookies和regex
conn=MySQLdb.connect(host="10.200.10.90",user="rootZ",passwd="8520",db="juzidb",charset="utf8",cursorclass = MySQLdb.cursors.DictCursor)  

#爬取网站
url = 'https://consumeprod.alipay.com/record/advanced.htm?dateRange=sevenDays&status=all&keyword=bizOutNo&keyValue=&dateType=createDate&minAmount=&maxAmount=&fundFlow=in&tradeModes=FP&tradeType=alipay&categoryId=&_input_charset=utf-8'
user_agent = 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.65 Safari/537.36'   

#设置程序是否在运行中
def enabled(status):
	cursor = conn.cursor()
	cursor.execute('update settings set `value`=%s where `key`="enabled"' % status)
	conn.commit()
	return status if True else False

#设置请求头和正则表达式
def settings():
	enabled(1)

	cursor = conn.cursor()
	cursor.execute("select * from settings where `key` = 'cookies' or `key` = 'regex'")    
	result = cursor.fetchall()

	for r in result:
		if r['key'] == 'cookies':
			cookies = base64.b64decode(r['value'])
		else:
			regex = r['value']

	cursor.close()
	#正则表达式
	try:
		p = re.compile(regex,re.S)
	except Exception, e:
		print u'正则表达式没写对'
		return enabled(0)

	headers = {'User-Agent':user_agent,
			   'Cookie':cookies,
			   'Connection':'keep-alive',
			   'Host':'consumeprod.alipay.com'}
	request = urllib2.Request(url, headers = headers)
	print u'开始啦'
	return [request,p]

#爬虫主体
def crawler(request,p):
	try:
		reponse = urllib2.urlopen(request)
	except urllib2.HTTPError, e:
		print e.code
		return enabled(0)
	
	html = reponse.read()
	html = html.decode('gbk').encode('utf-8')

	#正则匹配
	items = p.finditer(html)
	values = []
	try:	
		for item in items:
			datetime = '%s %s' % (item.group('date'),item.group('time'))
			timeArray = time.strptime(datetime, '%Y.%m.%d %H:%M')
			timeStamp = time.mktime(timeArray)
			tradeno = item.group('tradeno')
			if item.group('status').find('成功')<0:
				continue
			values.append((item.group('tradeno'),item.group('tradeno')[-4:],item.group('name'),item.group('amount'),item.group('remark'),item.group('status'),timeStamp))
		if values == []:
			return enabled(0)
	except Exception, e:
		print u'正则匹配失败'
		return enabled(0)

	#插入数据库
	cursor = conn.cursor()
	cursor.executemany('insert ignore into alipay values(%s,%s,%s,%s,%s,%s,%s,0)',values)
	conn.commit()
	return True

def loopset():
	while True:
		s = settings()
		if s:
			return s
		time.sleep(5)

#线程主程序循环
def loop(s):
    while True:
    	if crawler(s[0],s[1]):
    		print u'插入成功'
   		time.sleep(5)
    	else:
    		s = loopset()
    		print u'插入失败'
   		time.sleep(5)

t = threading.Thread(target=loop(loopset()))
t.start()