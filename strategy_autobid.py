#!/usr/bin/env python
# coding:utf8

import time
import sys
reload(sys)
sys.setdefaultencoding( "utf8" )
import warnings
warnings.filterwarnings("ignore")
import MySQLdb
import MySQLdb.cursors
import numpy as np
from config import *
import pprint
import random
import pandas as pd
import logging
from subprocess import Popen

# ppdai api
from openapi_client import openapi_client as client
from core.rsa_client import rsa_client as rsa
import json
import datetime
import os
from datetime import timedelta

from run import app, time2str, str2time, get_previous_month, get_next_month, connectdb, closedb

strategyId = sys.argv[1]
OpenID = sys.argv[2]
APPID = sys.argv[3]
AccessToken = sys.argv[4]
Username = sys.argv[5]

# 策略投标
try:
	(db,cursor) = connectdb()

	cursor.execute("select * from strategy where id=%s", [strategyId])
	strategy = cursor.fetchone()

	content = json.loads(strategy['content'])
	timedelta = int(strategy['timedelta'])
	flag = True
	for key in content.keys():
		if not key in [u'初始评级', u'借款利率', u'借款期限']:
			flag = False

	# 只需基本信息
	if flag:
		finish = False
		while True:
			PageIndex = 1
			Listings = []
			while True:
				access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/LoanList"
				data =  {
				  "PageIndex": PageIndex, 
				}
				sort_data = rsa.sort(data)
				sign = rsa.sign(sort_data)
				list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
				if list_result == '':
					continue
				list_result = json.loads(list_result)
				if len(list_result['LoanInfos']) == 0:
					break
				for item in list_result['LoanInfos']:
					Listings.append(item)
				PageIndex += 1
			for item in Listings:
				flag = True
				if content.has_key(u'初始评级') and (not item['CreditCode'] in content[u'初始评级']):
					flag = False
				if content.has_key(u'借款利率'):
					cflag = False
					condition = content[u'借款利率']
					for c in condition:
						if c == u'13%以下':
							if int(item['Rate']) <= 13:
								cflag = True
						elif c == u'22%以上':
							if int(item['Rate']) >= 22:
								cflag = True
						else:
							c = c[:-1].split('-')
							if int(item['Rate']) >= int(c[0]) and int(item['Rate']) <= int(c[1]):
								cflag = True
					if not cflag:
						flag = False
				if content.has_key(u'借款期限'):
					cflag = False
					condition = content[u'借款期限']
					for c in condition:
						if c == u'3个月以下' and int(item['Months']) <= 3:
							cflag = True
						elif c == u'12个月以上' and int(item['Months']) >= 12:
							cflag = True
						elif c == u'4至6个月' and int(item['Months']) >= 4 and int(item['Months']) <= 6:
							cflag = True
						elif c == u'6至12个月' and int(item['Months']) >= 6 and int(item['Months']) <= 12:
							cflag = True
					if not cflag:
						flag = False
				if flag:
					access_url = "http://gw.open.ppdai.com/invest/BidService/Bidding"
					data = {
						"ListingId": int(item['ListingId']), 
						"Amount": int(strategy['amount']),
						# 50 - 500
					}
					sort_data = rsa.sort(data)
					sign = rsa.sign(sort_data)
					list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)

					if list_result == '':
						continue
					list_result = json.loads(list_result)
					
					if list_result['Result'] == 0:
						cursor.execute("insert into bidding(OpenID, ListingId, strategyId, amount, timestamp) values(%s,%s,%s,%s,%s)", [OpenID, list_result['ListingId'], strategy['id'], list_result['Amount'], int(time.time())])

						# 更新数据
						while True:
							cursor.execute("select timestamp, report from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
							d = cursor.fetchall()
							if len(d) == 0:
								cursor.execute("insert into task(name, OpenID, status) values(%s, %s, %s)", ['bidBasicInfo', OpenID, 'pending'])

								Popen('python history_basic.py ' + OpenID + ' ' + APPID + ' ' + AccessToken + ' ' + str(1180627200) + ' ' + Username, shell=True)
								
								break
							else:
								d = d[0]
								s = d['report']
								timestamp = d['timestamp']
								if int(s) == 0:
									cursor.execute("delete from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
									cursor.execute("insert into task(name, OpenID, status) values(%s, %s, %s)", ['bidBasicInfo', OpenID, 'pending'])

									Popen('python history_basic.py ' + OpenID + ' ' + APPID + ' ' + AccessToken + ' ' + str(int(timestamp) - 600) + ' ' + Username, shell=True)

									break

						# 检查余额
						while True:
							access_url = "http://gw.open.ppdai.com/balance/balanceService/QueryBalance"
							data = {}
							sort_data = rsa.sort(data)
							sign = rsa.sign(sort_data)
							balance = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
							if balance == '':
								continue
							else:
								balance = json.loads(balance)['Balance']
								break
						cursor.execute('update user set balance=%s, balanceBid=%s, balanceWithdraw=%s where OpenID=%s', [balance[1]['Balance'], balance[0]['Balance'], balance[2]['Balance'], OpenID])
						if float(balance[1]['Balance']) + float(balance[0]['Balance']) < float(strategy['amount']):
							finish = True

						break

				# 检查任务是否已结束
				if strategy['OpenID'] in [0, '0']:
					cursor.execute("select strategy from user where OpenID=%s", [OpenID])
					sys_strategy = cursor.fetchone()['strategy']
					if sys_strategy == '':
						finish = True
						break
					sys_strategy = sys_strategy.split('-')
					sys_strategy = [int(s) for s in sys_strategy]
					if not int(strategy['id']) in sys_strategy:
						finish = True
						break
				else:
					cursor.execute("select active from strategy where id=%s", [strategy['id']])
					active = cursor.fetchone()['active']
					if int(active) == 0:
						finish = True
						break
			if finish:
				break

			time.sleep(60 * timedelta)

	# 还需详细信息
	else:
		pass

	# 修改状态
	if strategy['OpenID'] in [0, '0']:
		cursor.execute("select strategy from user where OpenID=%s", [OpenID])
		sys_strategy = cursor.fetchone()['strategy']
		if not sys_strategy == '':
			sys_strategy = sys_strategy.split('-')
			tmp = ''
			for s in sys_strategy:
				if not int(s) == int(strategy['id']):
					tmp = tmp + s + '-'
			if not tmp == '':
				tmp = tmp[:-1]
			sys_strategy = tmp
			cursor.execute("update user set strategy=%s where OpenID=%s", [sys_strategy, OpenID])
	else:
		cursor.execute("update strategy set active=%s where id=%s", [0, strategy['id']])

	closedb(db,cursor)
except Exception, e:
	app.logger.error(e)
else:
	pass
finally:
	pass