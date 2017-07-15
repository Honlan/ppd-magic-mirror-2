#!/usr/bin/env python
# coding:utf8

import time
import sys
reload(sys)
sys.setdefaultencoding( "utf8" )
from flask import *
import warnings
warnings.filterwarnings("ignore")
import MySQLdb
import MySQLdb.cursors
import numpy as np
from config import *
import pprint
import random
from celery import Celery
import pandas as pd

# ppdai api
from openapi_client import openapi_client as client
from core.rsa_client import rsa_client as rsa
import json
import datetime
import os
from datetime import timedelta

app = Flask(__name__)
app.config.from_object(__name__)
app.secret_key = SECRETKEY
app.permanent_session_lifetime = timedelta(days=90)

celery = Celery(app.name, broker=CELERY_BROKER_URL)
celery.conf.update(app.config)

# 判断是否已授权
def is_auth():
	result = {}

	(db,cursor) = connectdb()
	cursor.execute('select count(*) as count from user')
	count = cursor.fetchone()['count']
	closedb(db,cursor)

	if 'OpenID' in session:
		result['is_auth'] = True
		result['Username'] = escape(session['Username'])
		result['count'] = count
	else:
		result['is_auth'] = False
		result['count'] = count
	return result

# 获取用户授权资料
def auth_data():
	result = {}
	result['Username'] = escape(session['Username'])
	result['OpenID'] = escape(session['OpenID'])
	result['AccessToken'] = escape(session['AccessToken'])
	result['RefreshToken'] = escape(session['RefreshToken'])
	result['ExpiresIn'] = escape(session['ExpiresIn'])
	result['AuthTimestamp'] = escape(session['AuthTimestamp'])

	return result

def time2str(t, f):
	return time.strftime(f, time.localtime(t))

def str2time(s, f):
	return int(time.mktime(time.strptime(s, f)))

def get_previous_month(current, spliter, hasday):
	current = current.split(spliter)
	year = int(current[0])
	month = int(current[1])
	if hasday:
		day = int(current[2])

	month -= 1
	if month == 0:
		month = 12
		year -= 1
	year = str(year)
	if month < 10:
		month = '0' + str(month)
	else:
		month = str(month)
	if hasday:
		if day < 10:
			day = '0' + str(day)
		else:
			day = str(day)
	if hasday:
		return year + spliter + month + spliter + day
	else:
		return year + spliter + month

def get_next_month(current, spliter, hasday):
	current = current.split(spliter)
	year = int(current[0])
	month = int(current[1])
	if hasday:
		day = int(current[2])

	month += 1
	if month == 13:
		month = 1
		year += 1
	year = str(year)
	if month < 10:
		month = '0' + str(month)
	else:
		month = str(month)
	if hasday:
		if day < 10:
			day = '0' + str(day)
		else:
			day = str(day)
	if hasday:
		return year + spliter + month + spliter + day
	else:
		return year + spliter + month

# 连接数据库
def connectdb():
	db = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWORD, db=DATABASE, port=PORT, charset=CHARSET, cursorclass = MySQLdb.cursors.DictCursor)
	db.autocommit(True)
	cursor = db.cursor()
	return (db,cursor)

# 关闭数据库
def closedb(db,cursor):
	db.close()
	cursor.close()

# 平台透视
@app.route('/')
def index():
	(db,cursor) = connectdb()
	cursor.execute("select * from json_data where page=%s",['index'])
	json_data = cursor.fetchall()
	
	dataset = {}
	dataset['json'] = {item['keyword']: json.loads(item['json']) for item in json_data}

	closedb(db,cursor)

	# 处理雷达图数据
	radar = dataset['json']['bid_status_stat']['pies'];
	for key, value in radar.items():
		tmp = []
		for d in value:
			tall = np.sum([x['value'] for x in d['value']])
			t = [float('%.3f' % (float(x['value']) / tall)) for x in d['value']]
			tmp.append({'name': d['name'], 'value': t})
		radar[key] = tmp
	dataset['json']['bid_status_stat']['pies'] = radar;

	return render_template('index.html', dataset=json.dumps(dataset), auth=is_auth())

# 个人中心数据是否加载完毕
@app.route('/user_ready', methods=['POST'])
def user_ready():
	(db,cursor) = connectdb()
	while True:
		cursor.execute("select data from user where OpenID=%s", [session['OpenID']])
		profile = cursor.fetchone()['data']
		if not profile == '':
			break
		time.sleep(5)
	closedb(db,cursor)

	return json.dumps({'result': 'ok', 'msg': '个人中心数据加载完毕'})

# 个人中心
@app.route('/user')
def user():
	(db,cursor) = connectdb()
	cursor.execute("select data from user where OpenID=%s",[session['OpenID']])
	profile = cursor.fetchone()['data']

	if profile == '':
		closedb(db,cursor)
		return render_template('user_wait.html', auth=is_auth())

	else:
		dataset = {}
		dataset['json'] = json.loads(profile)

		for key in ['daily_amount_sum', 'daily_amount_sum_back', 'daily_interest', 'daily_rate', 'daily_amount_average', 'daily_term', 'daily_interest_sum', 'daily_interest_sum_total']:
			dataset['json']['bid_stat'][key] = [float('%.1f' % d) for d in dataset['json']['bid_stat'][key]]

		dataset['age'] = '%.1f' % ((float(time.time()) - dataset['json']['bid_stat']['from']) / 3600 / 24 / 365)
		cursor.execute("update user set age=%s where OpenID=%s", [dataset['age'], session['OpenID']])
		cursor.execute("select age from user where age!=%s order by age asc", [-1])
		ages = cursor.fetchall()
		ages = [x['age'] for x in ages]
		idx = -1
		for x in range(0, len(ages)):
			if float(ages[x]) == float(dataset['age']):
				idx = x
				break
		dataset['other'] = '%.1f' % (float(idx) * 100 / len(ages))
		closedb(db,cursor)

		dataset['tags'] = [];
		if dataset['json']['bid_stat']['bid_interest_average'] < 12:
			dataset['tags'].append('低风险')
			dataset['tags'].append('低收益')
		elif dataset['json']['bid_stat']['bid_interest_average'] < 18:
			dataset['tags'].append('中风险')
			dataset['tags'].append('中收益')
		else:
			dataset['tags'].append('高风险')
			dataset['tags'].append('高收益')
		if dataset['json']['bid_stat']['bid_term_average'] < 8:
			dataset['tags'].append('短期投资')
		elif dataset['json']['bid_stat']['bid_term_average'] < 16:
			dataset['tags'].append('中期投资')
		else:
			dataset['tags'].append('长期投资')

		return render_template('user.html', dataset=json.dumps(dataset), auth=is_auth())

# 个人中心例子
@app.route('/example')
def example():
	(db,cursor) = connectdb()
	cursor.execute("select * from json_data where page=%s",['user'])
	json_data = cursor.fetchall()
	
	dataset = {}
	dataset['json'] = {item['keyword']: json.loads(item['json']) for item in json_data}

	closedb(db,cursor)

	for key in ['daily_amount_sum', 'daily_amount_sum_back', 'daily_interest', 'daily_rate', 'daily_amount_average', 'daily_term', 'daily_interest_sum', 'daily_interest_sum_total']:
		dataset['json']['bid_stat'][key] = [float('%.1f' % d) for d in dataset['json']['bid_stat'][key]]

	dataset['age'] = '%.1f' % ((float(time.time()) - dataset['json']['bid_stat']['from']) / 3600 / 24 / 365)
	dataset['tags'] = [];
	if dataset['json']['bid_stat']['bid_interest_average'] < 12:
		dataset['tags'].append('低风险')
		dataset['tags'].append('低收益')
	elif dataset['json']['bid_stat']['bid_interest_average'] < 18:
		dataset['tags'].append('中风险')
		dataset['tags'].append('中收益')
	else:
		dataset['tags'].append('高风险')
		dataset['tags'].append('高收益')
	if dataset['json']['bid_stat']['bid_term_average'] < 8:
		dataset['tags'].append('短期投资')
	elif dataset['json']['bid_stat']['bid_term_average'] < 16:
		dataset['tags'].append('中期投资')
	else:
		dataset['tags'].append('长期投资')

	return render_template('user.html', dataset=json.dumps(dataset), auth=is_auth())

# 投资顾问
@app.route('/invest')
def invest():
	dataset = {}

	# session['OpenID'] = '2fc103ba972f4212aaf5f3213d1968f1'
	# session['Username'] = 'zhanghonglun'
	(db,cursor) = connectdb()
	cursor.execute("select * from strategy where OpenID=%s",[0])
	dataset['sys'] = cursor.fetchall()
	cursor.execute("select * from strategy where OpenID=%s",[session['OpenID']])
	dataset['my'] = cursor.fetchall()
	cursor.execute("select strategy from user where OpenID=%s", [session['OpenID']])
	dataset['strategy_count'] = 0
	sys_strategy = cursor.fetchone()['strategy']
	if not sys_strategy == '':
		sys_strategy = sys_strategy.split('-')
		for s in sys_strategy:
			for x in xrange(0, len(dataset['sys'])):
				if int(dataset['sys'][x]['id']) == int(s):
					dataset['sys'][x]['active'] = 1
					dataset['strategy_count'] += 1
					break

	cursor.execute("select balance, balanceBid, balanceWithdraw from user where OpenID=%s", [session['OpenID']])
	dataset['balance'] = cursor.fetchone()

	cursor.execute("select count(*) as count from bidding where OpenID=%s", [session['OpenID']])
	dataset['bidding_count'] = cursor.fetchone()['count']

	cursor.execute("select count(*) as count from strategy where OpenID=%s and active=%s", [session['OpenID'], 1])
	dataset['strategy_count'] += cursor.fetchone()['count']

	closedb(db,cursor)

	return render_template('invest.html', auth=is_auth(), datasetJson=json.dumps(dataset), dataset=dataset)

# 交流社区
@app.route('/chat')
def chat():
	return render_template('chat.html', auth=is_auth())

# 授权登陆
@app.route('/auth')
def auth():
	code = request.values.get('code')
	authorizeStr = client.authorize(appid=APPID, code=code)
	authorizeObj = json.loads(authorizeStr)

	OpenID = str(authorizeObj['OpenID'])
	AccessToken = authorizeObj['AccessToken']
	RefreshToken = authorizeObj['RefreshToken']
	ExpiresIn = authorizeObj['ExpiresIn']
	AuthTimestamp = int(time.time())

	session['OpenID'] = OpenID
	session['AccessToken'] = AccessToken
	session['RefreshToken'] = RefreshToken
	session['AuthTimestamp'] = AuthTimestamp
	session['ExpiresIn'] = ExpiresIn

	access_url = "http://gw.open.ppdai.com/open/openApiPublicQueryService/QueryUserNameByOpenID"
	data = {
	  "OpenID": OpenID
	}
	sort_data = rsa.sort(data)
	sign = rsa.sign(sort_data)
	list_result = json.loads(client.send(access_url, json.dumps(data), APPID, sign, AccessToken))
	Username = rsa.decrypt(list_result['UserName'])
	session['Username'] = Username

	access_url = "http://gw.open.ppdai.com/balance/balanceService/QueryBalance"
	data = {}
	sort_data = rsa.sort(data)
	sign = rsa.sign(sort_data)
	balance = json.loads(client.send(access_url, json.dumps(data), APPID, sign, AccessToken))['Balance']

	(db,cursor) = connectdb()
	cursor.execute("select count(*) as count from user where OpenID=%s", [OpenID])
	count = cursor.fetchone()['count']
	if count > 0:
		cursor.execute('update user set AccessToken=%s, RefreshToken=%s, ExpiresIn=%s, AuthTimestamp=%s, Username=%s, balance=%s, balanceBid=%s, balanceWithdraw=%s where OpenID=%s', [AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username, balance[1]['Balance'], balance[0]['Balance'], balance[2]['Balance'], OpenID])
	else:
		cursor.execute('insert into user(OpenID, AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username, balance, balanceBid, balanceWithdraw) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)', [OpenID, AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username, balance[1]['Balance'], balance[0]['Balance'], balance[2]['Balance']])
	
	# 是否需要获取个人投资记录
	cursor.execute("select count(*) as count from task where name=%s and OpenID=%s", ['bidBasicInfo', session['OpenID']])
	count = cursor.fetchone()['count']
	if count == 0:
		history_basic.apply_async(args=[session['OpenID'], APPID, session['AccessToken']])
		for x in range(0, 10):
			history_detail.apply_async(args=[session['OpenID'], APPID, session['AccessToken'], x])
			history_money.apply_async(args=[session['OpenID'], APPID, session['AccessToken'], x])
			history_status.apply_async(args=[session['OpenID'], APPID, session['AccessToken'], x])
			history_payback.apply_async(args=[session['OpenID'], APPID, session['AccessToken'], x])
		history_user.apply_async(args=[session['OpenID'], session['Username']])

	closedb(db,cursor)

	return redirect(url_for('user'))

# 退出授权
@app.route('/logout')
def logout():
	session.pop('Username')
	session.pop('OpenID')
	session.pop('AccessToken')
	session.pop('RefreshToken')
	session.pop('AuthTimestamp')
	session.pop('ExpiresIn')
	return redirect(url_for('index'))

# 新增个人策略
@app.route('/strategy_add', methods=['POST'])
def strategy_add():
	data = dict(request.form)
	name = data['name']
	description = data['description']
	timedelta = data['timedelta']
	amount = data['amount']
	data.pop('name')
	data.pop('description')
	data.pop('timedelta')
	data.pop('amount')
	(db,cursor) = connectdb()
	cursor.execute("insert into strategy(OpenID, content, weight, active, name, description, timedelta, amount) values(%s, %s, %s, %s, %s, %s, %s, %s)", [session['OpenID'], json.dumps(data), 1, 0, name, description, timedelta, amount])
	closedb(db,cursor)
	return json.dumps({'result': 'ok', 'msg': '新增个人策略成功'})

# 删除个人策略
@app.route('/strategy_delete', methods=['POST'])
def strategy_delete():
	data = request.form
	(db,cursor) = connectdb()
	cursor.execute("delete from strategy where OpenID=%s and id=%s", [session['OpenID'], data['strategyId']])
	closedb(db,cursor)
	return json.dumps({'result': 'ok', 'msg': '删除个人策略成功'})

# 开启策略投标
@app.route('/strategy_start', methods=['POST'])
def strategy_start():
	(db,cursor) = connectdb()

	data = request.form
	if data['type'] == 'sys':
		cursor.execute("select strategy from user where OpenID=%s", [session['OpenID']])
		sys_strategy = cursor.fetchone()['strategy']
		if sys_strategy == '':
			sys_strategy = data['strategyId']
		else:
			sys_strategy = sys_strategy + '-' + data['strategyId']
		cursor.execute("update user set strategy=%s where OpenID=%s", [sys_strategy, session['OpenID']])
	else:
		cursor.execute("update strategy set active=%s where id=%s", [1, data['strategyId']])
	strategy_autobid.apply_async(args=[data['strategyId'], session['OpenID'], APPID, session['AccessToken']])
	
	closedb(db,cursor)
	
	return json.dumps({'result': 'ok', 'msg': '启用策略成功'})

# 关闭策略投标
@app.route('/strategy_stop', methods=['POST'])
def strategy_stop():
	(db,cursor) = connectdb()

	data = request.form
	if data['type'] == 'sys':
		cursor.execute("select strategy from user where OpenID=%s", [session['OpenID']])
		sys_strategy = cursor.fetchone()['strategy']
		if not sys_strategy == '':
			sys_strategy = sys_strategy.split('-')
			tmp = ''
			for s in sys_strategy:
				if not int(s) == int(data['strategyId']):
					tmp = tmp + s + '-'
			if not tmp == '':
				tmp = tmp[:-1]
			sys_strategy = tmp
			cursor.execute("update user set strategy=%s where OpenID=%s", [sys_strategy, session['OpenID']])
	else:
		cursor.execute("update strategy set active=%s where id=%s", [0, data['strategyId']])
	
	closedb(db,cursor)

	return json.dumps({'result': 'ok', 'msg': '停用策略成功'})

# 策略投标
@celery.task
def strategy_autobid(strategyId, OpenID, APPID, AccessToken):
	(db,cursor) = connectdb()

	cursor.execute("select * from strategy where id=%s", [strategyId])
	strategy = cursor.fetchone()

	content = json.loads(strategy['content'])
	timedelta = int(strategy['timedelta'])
	flag = True
	for key in content.keys():
		if not key in ['初始评级', '借款利率', '借款期限']:
			flag = False

	# 只需基本信息
	if flag:
		finish = False
		while True:
			access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/LoanList"
			data =  {
			  "PageIndex": 1, 
			}
			sort_data = rsa.sort(data)
			sign = rsa.sign(sort_data)
			list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
			if list_result == '':
				continue
			list_result = json.loads(list_result)

			for item in list_result['LoanInfos']:
				flag = True
				if content.has_key('初始评级') and (not item['CreditCode'] in content['初始评级']):
					flag = False
				if content.has_key('借款利率'):
					cflag = False
					condition = content['借款利率'].split('_')
					for c in condition:
						if c == '13%以下' and int(item['Rate']) <= 13:
							cflag = True
						elif c == '22%以上' and int(item['Rate']) >= 22:
							cflag = True
						else:
							c = c[:-1].split('-')
							if int(item['Rate']) >= int(c[0]) and int(item['Rate']) <= int(c[1]):
								cflag = True
					if not cflag:
						flag = False
				if content.has_key('借款期限'):
					cflag = False
					condition = content['借款期限'].split('_')
					for c in condition:
						if c == '3个月以下' and int(item['Months']) <= 3:
							cflag = True
						elif c == '12个月以上' and int(item['Months']) >= 12:
							cflag = True
						elif c == '4至6个月' and int(item['Months']) >= 4 and int(item['Months']) <= 6:
							cflag = True
						elif c == '6至12个月' and int(item['Months']) >= 6 and int(item['Months']) <= 12:
							cflag = True
					if not cflag:
						flag = False
				if flag:
					access_url = "http://gw.open.ppdai.com/invest/BidService/Bidding"
					data = {
						"ListingId": item['ListingId'], 
						"Amount": int(strategy['amount']),
						# 50 - 500
					}
					sort_data = rsa.sort(data)
					sign = rsa.sign(sort_data)
					list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
					if list_result == '':
						continue
					list_result = json.loads(list_result)
					print item['ListingId'], strategy['id'], list_result
					if list_result['Result'] == 0:
						cursor.execute("insert into bidding(OpenID, ListingId, strategyId, amount, timestamp) values(%s,%s,%s,%s,%s)", [OpenID, list_result['ListingId'], strategy['id'], list_result['Amount'], int(time.time())])
						timedelta = int(strategy['timedelta'])
						break

				# 检查余额
				access_url = "http://gw.open.ppdai.com/balance/balanceService/QueryBalance"
				data = {}
				sort_data = rsa.sort(data)
				sign = rsa.sign(sort_data)
				balance = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
				if not balance == '':
					balance = json.loads(balance)['Balance']
					cursor.execute('update user set balance=%s, balanceBid=%s, balanceWithdraw=%s where OpenID=%s', [balance[1]['Balance'], balance[0]['Balance'], balance[2]['Balance'], OpenID])
					if balance[1]['Balance'] + balance[0]['Balance'] < strategy['amount']:
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
					if active == 0:
						finish = True
						break

			if finish:
				break

			timedelta = 2 * timedelta

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

	return

# 获取用户投标记录基本信息
@celery.task
def history_basic(OpenID, APPID, AccessToken):
	(db,cursor) = connectdb()

	cursor.execute("delete from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
	cursor.execute("insert into task(name, OpenID, status) values(%s, %s, %s)", ['bidBasicInfo', OpenID, 'pending'])
	access_url = "http://gw.open.ppdai.com/invest/BidService/BidList"
	current = int(time.time()) + 3600 * 24
	while current > 1180627200:
		data = {
			"StartTime": time.strftime('%Y-%m-%d', time.localtime(float(current - 3600 * 24 * 30))), 
			"EndTime": time.strftime('%Y-%m-%d', time.localtime(float(current))), 
			"PageIndex": 1, 
			"PageSize": 1000000
		}
		sort_data = rsa.sort(data)
		sign = rsa.sign(sort_data)
		list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
		if list_result == '':
			continue
		list_result = json.loads(list_result)
		for item in list_result['BidList']:
			if int(item['ListingId']) == 0:
				continue
			cursor.execute("select count(*) as count from listing where ListingId=%s", [item['ListingId']])
			count = cursor.fetchone()['count']
			if count == 0:
				cursor.execute("insert into listing(ListingId, Title, Months, CurrentRate, Amount, OpenID) values(%s, %s, %s, %s, %s, %s)", [item['ListingId'], str(item['Title']), item['Months'], item['Rate'], item['Amount'], OpenID])
			else:
				cursor.execute("update listing set OpenID=%s where ListingId=%s", [OpenID, item['ListingId']])
		current -= 3600 * 24 * 30

	cursor.execute("update task set status=%s, timestamp=%s where name=%s and OpenID=%s", ['finished', int(time.time()), 'bidBasicInfo', OpenID])
	closedb(db,cursor)

	return

# 获取用户投标记录详细信息
@celery.task
def history_detail(OpenID, APPID, AccessToken, tail):
	(db,cursor) = connectdb()

	while True:
		cursor.execute("select status from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
		status = cursor.fetchone()['status']
		if status == 'finished':
			break
		else:
			time.sleep(10)

	cursor.execute("select ListingId from listing where ListingId like %s and OpenID=%s", ['%' + str(tail), OpenID])
	ListingIds = cursor.fetchall()
	ListingIds = [x['ListingId'] for x in ListingIds]
	for x in range(0, len(ListingIds), 10):
		if x + 10 <= len(ListingIds):
			y = x + 10
		else:
			y = len(ListingIds)
		while True:
			access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/BatchListingInfos"
			data = {"ListingIds": ListingIds[x:y]}
			sort_data = rsa.sort(data)
			sign = rsa.sign(sort_data)
			list_result = client.send(access_url, json.dumps(data), APPID, sign)
			if list_result == '':
				continue
			list_result = json.loads(list_result)
			for item in list_result['LoanInfos']:
				cursor.execute("update listing set FistBidTime=%s, LastBidTime=%s, LenderCount=%s, AuditingTime=%s, RemainFunding=%s, DeadLineTimeOrRemindTimeStr=%s, CreditCode=%s, Amount=%s, Months=%s, CurrentRate=%s, BorrowName=%s, Gender=%s, EducationDegree=%s, GraduateSchool=%s, StudyStyle=%s, Age=%s, SuccessCount=%s, WasteCount=%s, CancelCount=%s, FailedCount=%s, NormalCount=%s, OverdueLessCount=%s, OverdueMoreCount=%s, OwingPrincipal=%s, OwingAmount=%s, AmountToReceive=%s, FirstSuccessBorrowTime=%s, RegisterTime=%s, CertificateValidate=%s, NciicIdentityCheck=%s, PhoneValidate=%s, VideoValidate=%s, CreditValidate=%s, EducateValidate=%s, LastSuccessBorrowTime=%s, HighestPrincipal=%s, HighestDebt=%s, TotalPrincipal=%s where ListingId=%s", [str(item['FistBidTime']), str(item['LastBidTime']), item['LenderCount'], str(item['AuditingTime']), item['RemainFunding'], item['DeadLineTimeOrRemindTimeStr'], item['CreditCode'], item['Amount'], item['Months'], item['CurrentRate'], item['BorrowName'], item['Gender'], item['EducationDegree'], item['GraduateSchool'], item['StudyStyle'], item['Age'], item['SuccessCount'], item['WasteCount'], item['CancelCount'], item['FailedCount'], item['NormalCount'], item['OverdueLessCount'], item['OverdueMoreCount'], item['OwingPrincipal'], item['OwingAmount'], item['AmountToReceive'], str(item['FirstSuccessBorrowTime']), str(item['RegisterTime']), item['CertificateValidate'], item['NciicIdentityCheck'], item['PhoneValidate'], item['VideoValidate'], item['CreditValidate'], item['EducateValidate'], str(item['LastSuccessBorrowTime']), item['HighestPrincipal'], item['HighestDebt'], item['TotalPrincipal'], item['ListingId']])
			break

	cursor.execute("update task set d" + str(tail) + "=%s, timestamp=%s where name=%s and OpenID=%s", [1, int(time.time()), 'bidBasicInfo', OpenID])

	closedb(db,cursor)

	return

# 获取用户投标记录投资金额
@celery.task
def history_money(OpenID, APPID, AccessToken, tail):
	(db,cursor) = connectdb()

	while True:
		cursor.execute("select status from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
		status = cursor.fetchone()['status']
		if status == 'finished':
			break
		else:
			time.sleep(10)

	cursor.execute("select ListingId from listing where ListingId like %s and OpenID=%s", ['%' + str(tail), OpenID])
	ListingIds = cursor.fetchall()
	ListingIds = [x['ListingId'] for x in ListingIds]
	for x in range(0, len(ListingIds), 5):
		if x + 5 <= len(ListingIds):
			y = x + 5
		else:
			y = len(ListingIds)
		while True:
			access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/BatchListingBidInfos"
			data = {"ListingIds": ListingIds[x:y]}
			sort_data = rsa.sort(data)
			sign = rsa.sign(sort_data)
			list_result = client.send(access_url, json.dumps(data), APPID, sign)
			if list_result == '':
				continue
			list_result = json.loads(list_result)
			for item in list_result['ListingBidsInfos']:
				cursor.execute("delete from lender where ListingId=%s", [item['ListingId']])
				for i in item['Bids']:
					cursor.execute("insert into lender(ListingId, LenderName, BidAmount, BidDateTime) values(%s, %s, %s, %s)", [item['ListingId'], i['LenderName'], i['BidAmount'], i['BidDateTime']])					
			break

	cursor.execute("update task set m" + str(tail) + "=%s, timestamp=%s where name=%s and OpenID=%s", [1, int(time.time()), 'bidBasicInfo', OpenID])

	closedb(db,cursor)

	return

# 获取用户投标记录标的状态
@celery.task
def history_status(OpenID, APPID, AccessToken, tail):
	(db,cursor) = connectdb()

	while True:
		cursor.execute("select status from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
		status = cursor.fetchone()['status']
		if status == 'finished':
			break
		else:
			time.sleep(10)

	cursor.execute("select ListingId from listing where ListingId like %s and OpenID=%s", ['%' + str(tail), OpenID])
	ListingIds = cursor.fetchall()
	ListingIds = [x['ListingId'] for x in ListingIds]
	for x in range(0, len(ListingIds), 20):
		if x + 20 <= len(ListingIds):
			y = x + 20
		else:
			y = len(ListingIds)
		while True:
			access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/BatchListingStatusInfos"
			data ={"ListingIds": ListingIds[x:y]}
			sort_data = rsa.sort(data)
			sign = rsa.sign(sort_data)
			list_result = client.send(access_url, json.dumps(data), APPID, sign)
			if list_result == '':
				continue
			list_result = json.loads(list_result)
			for item in list_result['Infos']:
				cursor.execute("update listing set Status=%s where ListingId=%s", [item['Status'], item['ListingId']])
			break
	
	cursor.execute("update task set s" + str(tail) + "=%s, timestamp=%s where name=%s and OpenID=%s", [1, int(time.time()), 'bidBasicInfo', OpenID])

	closedb(db,cursor)

	return

# 获取用户投标记录还款状态
@celery.task
def history_payback(OpenID, APPID, AccessToken, tail):
	(db,cursor) = connectdb()

	while True:
		cursor.execute("select status from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
		status = cursor.fetchone()['status']
		if status == 'finished':
			break
		else:
			time.sleep(10)

	cursor.execute("select ListingId from listing where ListingId like %s and OpenID=%s", ['%' + str(tail), OpenID])
	ListingIds = cursor.fetchall()
	ListingIds = [x['ListingId'] for x in ListingIds]
	for x in ListingIds:
		while True:
			access_url = "http://gw.open.ppdai.com/invest/RepaymentService/FetchLenderRepayment"
			data =  {"ListingId": x}
			sort_data = rsa.sort(data)
			sign = rsa.sign(sort_data)
			list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
			if list_result == '':
				continue
			list_result = json.loads(list_result)
			cursor.execute("delete from payback where ListingId=%s", [x])
			for item in list_result['ListingRepayment']:
				cursor.execute("insert into payback(ListingId, OrderId, DueDate, RepayDate, RepayPrincipal, RepayInterest, OwingPrincipal, OwingInterest, OwingOverdue, OverdueDays, RepayStatus, OpenID) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [item['ListingId'], item['OrderId'], item['DueDate'], item['RepayDate'], item['RepayPrincipal'], item['RepayInterest'], item['OwingPrincipal'], item['OwingInterest'], item['OwingOverdue'], item['OverdueDays'], item['RepayStatus'], OpenID])
			break

	cursor.execute("update task set p" + str(tail) + "=%s, timestamp=%s where name=%s and OpenID=%s", [1, int(time.time()), 'bidBasicInfo', OpenID])

	closedb(db,cursor)

	return

# 生成个人主页数据
@celery.task
def history_user(OpenID, Username):
	(db,cursor) = connectdb()

	while True:
		cursor.execute("select * from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
		task = cursor.fetchone()
		s = 0
		for c in ['d', 'm', 's', 'p']:
			for x in range(0, 10):
				s += task[c + str(x)]
		if s == 40:
			break
		else:
			time.sleep(10)

	cursor.execute("select ListingId from lender where LenderName=%s", [Username])
	ListingIds = cursor.fetchall()
	ListingIds = [x['ListingId'] for x in ListingIds]
	cursor.execute("select * from listing where ListingId in %s and Status=%s", [ListingIds, 3])
	basic = cursor.fetchall()
	data_dict = {}
	for item in basic:
		l = str(item['ListingId']) 
		data_dict[l] = {
			'ListingId': str(item['ListingId']),
			'借款金额': float(item['Amount']),
			'借款期限': float(item['Months']),
			'借款利率': float(item['CurrentRate']),
			'借款成功日期': str(item['AuditingTime']),
			'初始评级': item['CreditCode'],
			'年龄': float(item['Age']),
			'历史成功借款次数': float(item['SuccessCount']),
			'历史成功借款金额': float(item['TotalPrincipal']),
			'总待还本金': float(item['OwingPrincipal']),
			'历史正常还款期数': float(item['NormalCount']),
			'历史逾期还款期数': float(item['OverdueLessCount']) + float(item['OverdueMoreCount'])}
		if int(item['SuccessCount']) == 0:
			data_dict[l]['是否首标'] = '是'
		else:
			data_dict[l]['是否首标'] = '否'
		if int(item['Gender']) == 0:
			data_dict[l]['性别'] = '女'
		else:
			data_dict[l]['性别'] = '男'
		if int(item['PhoneValidate']) == 1:
			data_dict[l]['手机认证'] = '成功认证'
		else:
			data_dict[l]['手机认证'] = '未成功认证'
		if int(item['NciicIdentityCheck']) == 1:
			data_dict[l]['户口认证'] = '成功认证'
		else:
			data_dict[l]['户口认证'] = '未成功认证'
		if int(item['VideoValidate']) == 1:
			data_dict[l]['视频认证'] = '成功认证'
		else:
			data_dict[l]['视频认证'] = '未成功认证'
		if int(item['CertificateValidate']) == 1:
			data_dict[l]['学历认证'] = '成功认证'
		else:
			data_dict[l]['学历认证'] = '未成功认证'
		if int(item['CreditValidate']) == 1:
			data_dict[l]['征信认证'] = '成功认证'
		else:
			data_dict[l]['征信认证'] = '未成功认证'
		if random.random() < 0.6 / 100:
			data_dict[l]['淘宝认证'] = '成功认证'
		else:
			data_dict[l]['淘宝认证'] = '未成功认证'
		r = random.random()
		if r < 0.004:
			data_dict[l]['借款类型'] = '应收安全标'
		elif r < 0.006:
			data_dict[l]['借款类型'] = '电商'
		elif r < 0.302:
			data_dict[l]['借款类型'] = 'APP闪电'
		elif r < 0.722:
			data_dict[l]['借款类型'] = '普通'
		else:
			data_dict[l]['借款类型'] = '其他'
		cursor.execute("select BidAmount from lender where ListingId=%s and LenderName=%s", [item['ListingId'], Username])
		data_dict[l]['我的投资金额'] = float(cursor.fetchone()['BidAmount'])

		cursor.execute("select * from payback where ListingId=%s and OpenID=%s order by OrderId asc", [item['ListingId'], OpenID])
		payback = cursor.fetchall()

		current = -1
		for x in range(0, len(payback)):
			# 0：等待还款 1：准时还款 2：逾期还款 3：提前还款 4：部分还款
			if int(payback[x]['RepayStatus']) == 0:
				current = x
				break
		if current == -1:
			data_dict[l]['当前到期期数'] = int(payback[-1]['OrderId'])
			data_dict[l]['当前还款期数'] = int(payback[-1]['OrderId'])
		else:
			data_dict[l]['当前到期期数'] = int(payback[current]['OrderId'])
			data_dict[l]['当前还款期数'] = current
		if current == -1:
			data_dict[l]['标当前状态'] = '已还清'
		elif int(time.time()) > int(time.mktime(time.strptime(payback[current]['DueDate'], "%Y-%m-%d"))):
			data_dict[l]['标当前状态'] = '逾期中'
		else:
			data_dict[l]['标当前状态'] = '正常还款中'
		if current == 0:
			data_dict[l]['上次还款日期'] = 'NULL'
			data_dict[l]['上次还款本金'] = 'NULL'
			data_dict[l]['上次还款利息'] = 'NULL'
		elif current == -1:
			data_dict[l]['上次还款日期'] = payback[-1]['RepayDate']
			data_dict[l]['上次还款本金'] = payback[-1]['RepayPrincipal']
			data_dict[l]['上次还款利息'] = payback[-1]['RepayInterest']
		else:
			data_dict[l]['上次还款日期'] = payback[current - 1]['RepayDate']
			data_dict[l]['上次还款本金'] = payback[current - 1]['RepayPrincipal']
			data_dict[l]['上次还款利息'] = payback[current - 1]['RepayInterest']

		if current == -1 or current == len(payback) - 1:
			data_dict[l]['下次计划还款日期'] = 'NULL'
			data_dict[l]['下次计划还款本金'] = 'NULL'
			data_dict[l]['下次计划还款利息'] = 'NULL'
		else:
			data_dict[l]['下次计划还款日期'] = payback[current + 1]['RepayDate']
			data_dict[l]['下次计划还款本金'] = payback[current + 1]['RepayPrincipal']
			data_dict[l]['下次计划还款利息'] = payback[current + 1]['RepayInterest']

		data_dict[l]['已还本金'] = np.sum([float(p['RepayPrincipal']) for p in payback])
		data_dict[l]['已还利息'] = np.sum([float(p['RepayInterest']) for p in payback])
		data_dict[l]['待还本金'] = np.sum([float(p['OwingPrincipal']) for p in payback])
		data_dict[l]['待还利息'] = np.sum([float(p['OwingInterest']) for p in payback])
		data_dict[l]['标当前逾期天数'] = np.sum([0 if float(p['OverdueDays']) < 0 else float(p['OverdueDays']) for p in payback])
	data_dict = [v for v in data_dict.values()]
	data_dict = {k: [data_dict[x][k] for x in range(0, len(data_dict))] for k in data_dict[0].keys()}
	
	data = pd.DataFrame.from_dict(data_dict)
	data['借款成功时间戳'] = data['借款成功日期'].apply(lambda x:str2time(x[:x.rfind('.')], "%Y-%m-%dT%H:%M:%S"))
	data.sort_values('借款成功时间戳')
	data_dict = data.to_dict('records')
	profile = {}

	stats = {}
	stats['from'] = data_dict[0]['借款成功时间戳']
	stats['bid_num'] = len(data)
	stats['bid_amount_sum'] = int(data['我的投资金额'].sum())
	stats['bid_amount_average'] = data['我的投资金额'].mean()
	stats['bid_amount_back_sum'] = int(data['已还本金'].sum())
	stats['bid_amount_waiting_sum'] = int(data['待还本金'].sum())
	stats['bid_interest_sum'] = int(data['已还利息'].sum())
	stats['bid_interest_waiting_sum'] = int(data['待还利息'].sum())

	i = 0.0
	t = 0.0
	w = 0.0
	for item in data_dict:
	    i += item['我的投资金额'] * item['借款利率']
	    t += item['我的投资金额'] * item['借款期限']
	    w += item['我的投资金额']
	stats['bid_interest_average'] = i / w
	stats['bid_term_average'] = t / w

	dates = []

	start = np.min([d['借款成功时间戳'] for d in data_dict])
	end = np.max([d['借款成功时间戳'] for d in data_dict])

	while start <= end:
	    dates.append(time2str(start, '%Y/%m/%d')[2:])
	    start += 3600 * 24

	daily_num = {d:0 for d in dates}
	daily_amount = {d:0.0 for d in dates}
	daily_amount_back = {d:0.0 for d in dates}
	daily_amount_average = []
	daily_interest = {d:0.0 for d in dates}
	daily_interest_total = {d:0.0 for d in dates}
	daily_rate = {d:0.0 for d in dates}
	daily_term = {d:0.0 for d in dates}
	daily_weight = {d:0.0 for d in dates}
	daily_amount_sum = []
	daily_amount_sum_back = []
	daily_interest_sum = []
	daily_interest_sum_total = []
	for item in data_dict:
	    date = time2str(item['借款成功时间戳'], '%Y/%m/%d')[2:]
	    
	    daily_num[date] += 1

	    daily_amount[date] += item['我的投资金额']
	    daily_interest_total[date] += item['已还利息'] + item['待还利息']
	    
	    daily_rate[date] += item['借款利率'] * item['我的投资金额']
	    daily_term[date] += item['借款期限'] * item['我的投资金额']
	    daily_weight[date] += item['我的投资金额']
	    
	    back_count = item['借款期限'] - int(item['借款期限'] * item['待还本金'] / item['我的投资金额'])
	    N = back_count
	    while N > 0:
	        N -= 1
	        date = get_next_month(date, '/', True)
	        
	        if not daily_amount_back.has_key(date):
	            daily_amount_back[date] = 0.0
	        daily_amount_back[date] += item['已还本金'] / back_count
	        
	        if not daily_interest.has_key(date):
	            daily_interest[date] = 0.0
	        daily_interest[date] += item['已还利息'] / back_count

	r = []
	t = []
	s1 = 0
	s2 = 0
	s3 = 0
	s4 = 0
	for d in dates:
	    s1 += daily_amount[d]
	    s2 += daily_amount_back[d]
	    s3 += daily_interest[d]
	    s4 += daily_interest_total[d]
	    daily_amount_sum.append(s1 / 10000)
	    daily_amount_sum_back.append(s2 / 10000)
	    daily_interest_sum.append(s3 / 10000)
	    daily_interest_sum_total.append(s4 / 10000)
	    
	    if daily_num[d] == 0:
	        daily_amount_average.append(0)
	        r.append(0)
	        t.append(0)
	    else:
	        daily_amount_average.append(daily_amount[d] / daily_num[d])
	        r.append(daily_rate[d] / daily_weight[d])
	        t.append(daily_term[d] / daily_weight[d])
	daily_rate = r
	daily_term = t
	        
	daily_num = [daily_num[d] for d in dates]
	daily_amount = [daily_amount[d] for d in dates]
	daily_amount_back = [daily_amount_back[d] for d in dates]
	daily_interest = [daily_interest[d] for d in dates]

	stats['daily_num'] = daily_num
	stats['daily_amount'] = daily_amount
	stats['daily_amount_back'] = daily_amount_back
	stats['daily_amount_average'] = daily_amount_average
	stats['daily_interest'] = daily_interest
	stats['daily_rate'] = daily_rate
	stats['daily_term'] = daily_term
	stats['daily_amount_sum'] = daily_amount_sum
	stats['daily_amount_sum_back'] = daily_amount_sum_back
	stats['daily_interest_sum'] = daily_interest_sum
	stats['daily_interest_sum_total'] = daily_interest_sum_total
	stats['dates'] = dates

	profile['bid_stat'] = stats

	data['借款期限区间'] = '1-4'
	data.at[data['借款期限'] >= 5, '借款期限区间'] = '5-8'
	data.at[data['借款期限'] >= 9, '借款期限区间'] = '9-12'
	data.at[data['借款期限'] >= 13, '借款期限区间'] = '13-16'
	data.at[data['借款期限'] >= 17, '借款期限区间'] = '17-20'
	data.at[data['借款期限'] >= 21, '借款期限区间'] = '21-24'
	params = {}
	params['初始评级'] = ['AAA', 'AA', 'A', 'B', 'C', 'D', 'E', 'F']
	params['借款类型'] = ['应收安全标', '电商', 'APP闪电', '普通', '其他']
	params['期限区间'] = ['1-4', '5-8', '9-12', '13-16', '17-20', '21-24']
	params['是否首标'] = ['是', '否']
	params['年龄区间'] = ['10-20', '20-30', '30-40', '40-50', '50-60']
	params['性别'] = ['男', '女']

	stats = {}
	for key, value in params.items():
	    stats[key] = {}
	    for v in value:
	        stats[key][v] = {}

	months = []
	start = time2str(start, '%Y-%m')[2:]
	end = time2str(end, '%Y-%m')[2:]
	months.append(get_previous_month(start, '-', False))
	while True:
		months.append(start)
		if start == end:
			break
		else:
			start = get_next_month(start, '-', False)
	months.append(get_next_month(end, '-', False))
	for item in data_dict:
	    month = time2str(item['借款成功时间戳'], '%Y-%m')[2:]

	    if item['年龄'] <= 20:
	        item['年龄区间'] = '10-20'
	    elif item['年龄'] <= 30:
	        item['年龄区间'] = '20-30'
	    elif item['年龄'] <= 40:
	        item['年龄区间'] = '30-40'
	    elif item['年龄'] <= 50:
	        item['年龄区间'] = '40-50'
	    else:
	        item['年龄区间'] = '50-60'
	        
	    if item['借款期限'] <= 4:
	        item['期限区间'] = '1-4'
	    elif item['借款期限'] <= 8:
	        item['期限区间'] = '5-8'
	    elif item['借款期限'] <= 12:
	        item['期限区间'] = '9-12'
	    elif item['借款期限'] <= 16:
	        item['期限区间'] = '13-16'
	    elif item['借款期限'] <= 20:
	        item['期限区间'] = '17-20'
	    else:
	        item['期限区间'] = '21-24'
	    
	    for key, value in params.items():
	        if not stats[key][item[key]].has_key(month):
	            stats[key][item[key]][month] = [0.0, 0.0]
	        stats[key][item[key]][month][0] += 1
	        stats[key][item[key]][month][1] += item['我的投资金额']

	for key, value in params.items():
	    for v in value:
	        for m in months:
	            if not stats[key][v].has_key(m):
	                stats[key][v][m] = [0.0, 0.0]
	        
	flow = {}

	for key, value in stats.items():
	    matrix = []
	    for v in params[key]:
	        matrix.append([stats[key][v][m] for m in months])
	    column_sums = []
	    for x in xrange(0, len(months)):
	        column_sums.append(np.sum([matrix[r][x][0] for r in xrange(0, len(matrix))]))
	    max_column_sum = np.max(column_sums)
	    
	    height = 1 - 0.005 * (len(matrix) - 1)
	    for c in xrange(0, len(months)):
	        current = 0
	        for r in xrange(0, len(matrix)):
	            tmp = [current, current + height * matrix[r][c][0] / max_column_sum]
	            current += height * matrix[r][c][0] / max_column_sum + 0.005
	            matrix[r][c] = tmp
	    
	    for x in xrange(0, len(matrix)):
	        tmp = []
	        for y in xrange(0, len(matrix[x])):
	            tmp.append([float(y) / (len(matrix[x]) - 1), matrix[x][y][0]])
	        for y in xrange(0, len(matrix[x])):
	            tmp.append([float(len(matrix[x]) - y - 1) / (len(matrix[x]) - 1), matrix[x][len(matrix[x]) - y - 1][1]])
	        matrix[x] = tmp
	        
	    flow[key] = []
	    flow[key].append(matrix)
	    
	    matrix = []
	    for v in params[key]:
	        matrix.append([stats[key][v][m] for m in months])
	    column_sums = []
	    for x in xrange(0, len(months)):
	        column_sums.append(np.sum([matrix[r][x][1] for r in xrange(0, len(matrix))]))
	    max_column_sum = np.max(column_sums)
	    height = 1 - 0.005 * len(matrix)
	    for c in xrange(0, len(months)):
	        current = 0
	        for r in xrange(0, len(matrix)):
	            tmp = [current, current + height * matrix[r][c][1] / max_column_sum]
	            current += height * matrix[r][c][1] / max_column_sum + 0.005
	            matrix[r][c] = tmp
	    
	    for x in xrange(0, len(matrix)):
	        tmp = []
	        for y in xrange(0, len(matrix[x])):
	            tmp.append([float(y) / (len(matrix[x]) - 1), matrix[x][y][0]])
	        for y in xrange(0, len(matrix[x])):
	            tmp.append([float(len(matrix[x]) - y - 1) / (len(matrix[x]) - 1), matrix[x][len(matrix[x]) - y - 1][1]])
	        matrix[x] = tmp
	        
	    flow[key].append(matrix)

	profile['bid_flow'] = {'months': months, 'flow': flow, 'params': params}

	terms = [t for t in range(0, int(data['借款期限'].max() + 1))]
	stats = {'借款期限': {t:[0, 0] for t in terms}, '剩余期限': {t:[0, 0] for t in terms}}
	data['剩余期限'] = data['借款期限'] - data['当前还款期数']
	data_dict = data.to_dict('records')

	for item in data_dict:
	    term = item['借款期限']
	    term_left = item['剩余期限']
	    
	    if term_left < 0:
	        term_left = 0
	    
	    stats['借款期限'][term][0] += 1
	    stats['借款期限'][term][1] += item['我的投资金额']
	    
	    stats['剩余期限'][term_left][0] += 1
	    stats['剩余期限'][term_left][1] += item['我的投资金额']

	stats['借款期限'] = [[stats['借款期限'][t][0] for t in terms], [float('%.1f' % (stats['借款期限'][t][1] / 10000)) for t in terms]]
	stats['剩余期限'] = [[stats['剩余期限'][t][0] for t in terms], [float('%.1f' % (stats['剩余期限'][t][1] / 10000)) for t in terms]]
	tmp = []
	tmp.append([{
	        'name': '借款期限',
	        'type': 'bar',
	        'data': stats['借款期限'][0]
	    }, {
	        'name': '剩余期限',
	        'type': 'bar',
	        'data': stats['剩余期限'][0]
	    }])
	tmp.append([{
	        'name': '借款期限',
	        'type': 'bar',
	        'data': stats['借款期限'][1]
	    }, {
	        'name': '剩余期限',
	        'type': 'bar',
	        'data': stats['剩余期限'][1]
	    }])
	stats = tmp
	profile['bid_terms'] = {'terms': terms, 'data': stats}

	ranges = ['0-4', '5-8', '9-12', '13-16', '17-20', '21-24']

	data['剩余期限区间'] = '21-24'
	data.at[data['剩余期限'] < 21, '剩余期限区间'] = '17-20'
	data.at[data['剩余期限'] < 17, '剩余期限区间'] = '13-16'
	data.at[data['剩余期限'] < 13, '剩余期限区间'] = '9-12'
	data.at[data['剩余期限'] < 9, '剩余期限区间'] = '5-8'
	data.at[data['剩余期限'] < 5, '剩余期限区间'] = '0-4'
	stats = {r:{m:[0, 0] for m in months} for r in ranges}

	data_dict = data.to_dict('records')
	for item in data_dict:
	    m = time2str(item['借款成功时间戳'], '%Y-%m')[2:]
	    stats[item['剩余期限区间']][m][0] += 1
	    stats[item['剩余期限区间']][m][1] += item['我的投资金额']

	for r, value in stats.items():
	    stats[r] = [[value[m][0] for m in months], [float('%.1f' % (value[m][1] / 10000)) for m in months]]
	stats = [[{'name': r, 'type': 'bar', 'stack': '总量', 'data': stats[r][0]} for r in ranges], [{'name': r, 'type': 'bar', 'stack': '总量', 'data': stats[r][1]} for r in ranges]]

	profile['bid_terms_history'] = {'months': months, 'ranges': ranges, 'data': stats}

	indicators = []
	keys = [['信用标数量', '投资总金额', '平均利率', '平均期限', '首标比例', '男性比例', '平均年龄'], 
	        ['手机认证', '户口认证', '视频认证', '学历认证', '征信认证', '淘宝认证'], 
	        ['历史成功借款次数', '历史成功借款金额', '总待还本金', '历史正常还款期数', '历史逾期还款期数']]
	rates = ['AAA', 'AA', 'A', 'B', 'C', 'D', 'E', 'F']
	indicators = [[], [], []]
	stats = [[{'name': r, 'value': [0.0 for k in keys[0]]} for r in rates], [{'name': r, 'value': [0.0 for k in keys[1]]} for r in rates], [{'name': r, 'value': [0.0 for k in keys[2]]} for r in rates]]

	for key in keys[0]:
	    indicators[0].append({
	            'name': key,
	            'min': 0,
	            'max': 0
	        })
	for key in keys[1]:
	    indicators[1].append({
	            'name': key,
	            'min': 0,
	            'max': 1
	        })
	for key in keys[2]:
	    indicators[2].append({
	            'name': key,
	            'min': data[key].min(),
	            'max': data[key].max()
	        })

	data_dict = data.to_dict('records')
	weight = [0.0 for r in rates]
	count = [0.0 for r in rates]
	for item in data_dict:
	    rate = item['初始评级']
	    rdx = 0
	    while not rates[rdx] == rate:
	        rdx += 1
	    
	    # 信用标数量
	    stats[0][rdx]['value'][0] += 1
	    # 投资总金额
	    stats[0][rdx]['value'][1] += item['我的投资金额']
	    # 平均利率
	    stats[0][rdx]['value'][2] += item['我的投资金额'] * item['借款利率']
	    # 平均期限
	    stats[0][rdx]['value'][3] += item['我的投资金额'] * item['借款期限']
	    # 首标比例
	    if item['是否首标'] == '是':
	        stats[0][rdx]['value'][4] += 1
	    # 男性比例
	    if item['性别'] == '男':
	        stats[0][rdx]['value'][5] += 1 
	    # 平均年龄
	    stats[0][rdx]['value'][6] += item['我的投资金额'] * item['年龄']
	    weight[rdx] += item['我的投资金额']
	    
	    for x in xrange(0, len(keys[1])):
	        k = keys[1][x]
	        if item[k] == '成功认证':
	            stats[1][rdx]['value'][x] += 1
	    count[rdx] += 1

	for x in xrange(0, len(stats[0])):
		if count[x] == 0:
			stats[0][x]['value'][4] = 0
			stats[0][x]['value'][5] = 0
		else:
		    stats[0][x]['value'][4] = stats[0][x]['value'][4] / count[x]
		    stats[0][x]['value'][5] = stats[0][x]['value'][5] / count[x]

		if weight[x] == 0:
			stats[0][x]['value'][2] = 0
			stats[0][x]['value'][3] = 0
			stats[0][x]['value'][6] = 0
		else:
			stats[0][x]['value'][2] = stats[0][x]['value'][2] / weight[x]
			stats[0][x]['value'][3] = stats[0][x]['value'][3] / weight[x]
			stats[0][x]['value'][6] = stats[0][x]['value'][6] / weight[x]

	for x in xrange(0, len(stats[1])):
	    stats[1][x]['value'] = [0 if count[x] == 0 else d / count[x] for d in stats[1][x]['value']]

	for x in xrange(0, len(stats[2])):
	    stats[2][x]['value'] = [data[data['初始评级'] == rates[x]][k].mean() for k in keys[2]]
	    stats[2][x]['value'] = [0 if str(x) == 'NaN' else x for x in stats[2][x]['value']]
	    
	for x1 in xrange(0, len(stats)):
	    for x2 in xrange(0, len(stats[x1])):
	        if x1 in [0, 2]:
	            stats[x1][x2]['value'] = [float('%.1f' % d) for d in stats[x1][x2]['value']]
	        else:
	            stats[x1][x2]['value'] = [float('%.3f' % d) for d in stats[x1][x2]['value']]

	for k in [0, 1, 2]:
	    for x in xrange(0, len(indicators[k])):
	        indicators[k][x]['max'] = np.ceil(1.2 * np.max([stats[k][i]['value'][x] for i in xrange(0, len(stats[k]))]))
	        # indicators[k][x]['min'] = np.min([stats[k][i]['value'][x] for i in xrange(0, len(stats[k]))])
	        indicators[k][x]['min'] = 0

	keys = [['信用标数量', '投资总金额', '平均利率', '平均期限', '首标比例', '男性比例', '平均年龄'], 
			['手机认证', '户口认证', '视频认证', '学历认证', '征信认证', '淘宝认证'], 
	        ['历史成功借款次数', '历史成功借款金额', '总待还本金', '历史正常还款期数', '历史逾期还款期数']]

	indicators[0][4]['max'] = 1
	indicators[0][5]['max'] = 1

	indicators[1][0]['max'] = 1
	indicators[1][1]['max'] = 0.2
	indicators[1][2]['max'] = 0.15
	indicators[1][3]['max'] = 1
	indicators[1][4]['max'] = 0.12
	indicators[1][5]['max'] = 0.012

	indicators[2][0]['name'] = '成功借款次数'
	indicators[2][1]['name'] = '成功借款金额'
	indicators[2][3]['name'] = '正常还款期数'
	indicators[2][4]['name'] = '逾期还款期数'

	# indicators[1][5]['max'] = 2

	profile['bid_radar'] = {'indicators': indicators, 'data': stats, 'legend': rates}

	params = {}
	params['初始评级'] = ['AAA', 'AA', 'A', 'B', 'C', 'D', 'E', 'F']
	params['借款类型'] = ['应收安全标', '电商', 'APP闪电', '普通', '其他']
	params['期限区间'] = ['1-4', '5-8', '9-12', '13-16', '17-20', '21-24']
	params['是否首标'] = ['是', '否']
	params['年龄区间'] = ['10-20', '20-30', '30-40', '40-50', '50-60']
	params['性别'] = ['男', '女']
	interest = {key:{k:{m:0.0 for m in months} for k in params[key]} for key in params.keys()}
	bad = {key:{k:{m:0.0 for m in months} for k in params[key]} for key in params.keys()}
	total = {key:{k:{m:0.0 for m in months} for k in params[key]} for key in params.keys()}
	rates = {'interest':{key:[] for key in params.keys()}, 'bad':{key:[] for key in params.keys()}}

	data['期限区间'] = '1-4'
	data.at[data['借款期限'] >= 5, '期限区间'] = '5-8'
	data.at[data['借款期限'] >= 9, '期限区间'] = '9-12'
	data.at[data['借款期限'] >= 13, '期限区间'] = '13-16'
	data.at[data['借款期限'] >= 17, '期限区间'] = '17-20'
	data.at[data['借款期限'] >= 21, '期限区间'] = '21-24'

	data['年龄区间'] = '10-20'
	data.at[data['年龄'] > 20, '年龄区间'] = '20-30'
	data.at[data['年龄'] > 30, '年龄区间'] = '30-40'
	data.at[data['年龄'] > 40, '年龄区间'] = '40-50'
	data.at[data['年龄'] > 50, '年龄区间'] = '50-60'

	data = data.sort_values('借款成功时间戳')
	data_dict = data.to_dict('records')
	for item in data_dict:
	    month = time2str(item['借款成功时间戳'], '%Y-%m')[2:]
	    
	    for key in params.keys():
	        total[key][item[key]][month] += float(item['我的投资金额'])
	        interest[key][item[key]][month] += float(item['我的投资金额']) * item['借款利率'] / 100
	    
	    if item['标当前状态'] == '逾期中':
	        bad_month = item['下次计划还款日期'].split('/')
	        year = bad_month[0][2:]
	        month = bad_month[1]
	        if len(month) == 1:
	            month = '0' + month
	        bad_month = year + '-' + month
	        for key in params.keys():
	            bad[key][item[key]][bad_month] += float(item['待还本金'])

	max_values = {'interest': {key: 0 for key in params.keys()}, 'bad': {key: 0 for key in params.keys()}}
	max_values_r = {'interest': {key: 0 for key in params.keys()}, 'bad': {key: 0 for key in params.keys()}}
	lines = {'interest':{key:[] for key in params.keys()}, 'bad':{key:[] for key in params.keys()}}

	for key, value in params.items():
	    for v in value:
	        interest[key][v] = [interest[key][v][m] for m in months]
	        bad[key][v] = [bad[key][v][m] for m in months]
	        total[key][v] = [total[key][v][m] for m in months]
	        
	        if np.max(interest[key][v]) > max_values['interest'][key]:
	            max_values['interest'][key] = np.max(interest[key][v])
	        if np.max(bad[key][v]) > max_values['bad'][key]:
	            max_values['bad'][key] = np.max(bad[key][v])
	            
	        ti = []
	        tb = []
	        si = 0
	        sb = 0
	        st = 0
	        for x in xrange(0, len(months)):
	            si += interest[key][v][x]
	            sb += bad[key][v][x]
	            st += total[key][v][x]
	            if st == 0:
	                ti.append(0)
	                tb.append(0)
	            else:    
	                ti.append(si / st)
	                tb.append(sb / st)
	        rates['interest'][key].append(ti)
	        rates['bad'][key].append(tb)
	        
	        if np.max(ti) > max_values_r['interest'][key]:
	            max_values_r['interest'][key] = np.max(ti)
	        if np.max(tb) > max_values_r['bad'][key]:
	            max_values_r['bad'][key] = np.max(tb)
	    
	    # interest[key] = [interest[key][v] for v in params[key]]
	    # bad[key] = [bad[key][v] for v in params[key]]
	    
	    tmp = []
	    tmpl = []
	    for r in xrange(0, len(rates['interest'][key])):
	        for c in xrange(0, len(rates['interest'][key][r])):
	            tmp.append([r, c, rates['interest'][key][r][c]])
	            if c > 0:
	                tmpl.append([r, c - 1, rates['interest'][key][r][c - 1], c, rates['interest'][key][r][c], key])
	    rates['interest'][key] = tmp
	    lines['interest'][key] = tmpl
	    
	    tmp = []
	    tmpl = []
	    for r in xrange(0, len(rates['bad'][key])):
	        for c in xrange(0, len(rates['bad'][key][r])):
	            tmp.append([r, c, rates['bad'][key][r][c]])
	            if c > 0:
	                tmpl.append([r, c - 1, rates['bad'][key][r][c - 1], c, rates['bad'][key][r][c]])
	    rates['bad'][key] = tmp
	    lines['bad'][key] = tmpl

	profile['bid_bad'] = {'months': months, 'params': params, 'interest': interest, 'bad': bad, 'rates': rates, 'max': max_values, 'max_r': max_values_r, 'lines': lines}

	cursor.execute("update user set data=%s where OpenID=%s", [json.dumps(profile), OpenID])

	closedb(db,cursor)

	return

if __name__ == '__main__':
	app.run(debug=True)