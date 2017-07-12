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

# 个人中心
@app.route('/user')
def user():
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
	
	sys_strategy = cursor.fetchone()['strategy']
	if not sys_strategy == '':
		sys_strategy = sys_strategy.split('-')
		for s in sys_strategy:
			for x in xrange(0, len(dataset['sys'])):
				if int(dataset['sys'][x]['id']) == int(s):
					dataset['sys'][x]['active'] = 1
					break

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
		sys_strategy = cursor.fetchone()['strategy'].split('-')
		tmp = ''
		for s in sys_strategy:
			if not s == data['strategyId']:
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
						if c == '13%以下' and item['Rate'] <= 13:
							cflag = True
						elif c == '22%以上' and item['Rate'] >= 22:
							cflag = True
						else:
							c = c[:-1].split('-')
							if item['Rate'] >= int(c[0]) and item['Rate'] <= int(c[1]):
								cflag = True
					if not cflag:
						flag = False
				if content.has_key('借款期限'):
					cflag = False
					condition = content['借款期限'].split('_')
					for c in condition:
						if c == '3个月以下' and item['Months'] <= 3:
							cflag = True
						elif c == '12个月以上' and item['Months'] >= 12:
							cflag = True
						elif c == '4至6个月' and item['Months'] >= 4 and item['Months'] <= 6:
							cflag = True
						elif c == '6至12个月' and item['Months'] >= 6 and item['Months'] <= 12:
							cflag = True
					if not cflag:
						flag = False
				if flag:
					access_url = "http://gw.open.ppdai.com/invest/BidService/Bidding"
					data = {
						"ListingId": item['ListingId'], 
						"Amount": strategy['amount'],
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
					sys_strategy = cursor.fetchone()['strategy'].split('-')
					if not strategy['id'] in sys_strategy:
						finish = True
						break
				else:
					cursor.execute("select active from strategy where id=%s", [strategy['id']])
					active = cursor.fetchone()['active']
					if active == 0:
						finish = True
						break

			print 1111111, finish
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
		sys_strategy = cursor.fetchone()['strategy'].split('-')
		tmp = ''
		for s in sys_strategy:
			if not s == strategy['id']:
				tmp = tmp + s + '-'
		if not tmp == '':
			tmp = tmp[:-1]
		sys_strategy = tmp
		cursor.execute("update user set strategy=%s where OpenID=%s", [sys_strategy, OpenID])
	else:
		cursor.execute("update strategy set active=%s where id=%s", [0, strategy['id']])

	closedb(db,cursor)

if __name__ == '__main__':
	app.run(debug=True)