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
	data.pop('name')
	data.pop('description')
	(db,cursor) = connectdb()
	cursor.execute("insert into strategy(OpenID, content, weight, active, name, description) values(%s, %s, %s, %s, %s, %s)", [session['OpenID'], json.dumps(data), 1, 0, name, description])
	# cursor.execute("insert into strategy(OpenID, content, weight, active, name, description) values(%s, %s, %s, %s, %s, %s)", [0, json.dumps(data), 1, 0, name, description])
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
		pass
	strategy_autobid.apply_async(args=[data['strategyId'], session['OpenID'], APPID, session['AccessToken']])
	
	closedb(db,cursor)
	
	return json.dumps({'result': 'ok', 'msg': '启用个人策略成功'})

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
		pass
	
	closedb(db,cursor)

	return json.dumps({'result': 'ok', 'msg': '停用个人策略成功'})

# 策略投标
@celery.task
def strategy_autobid(strategyId, OpenID, APPID, AccessToken):
	(db,cursor) = connectdb()

	cursor.execute("select * from strategy where id=%s", [strategyId])
	strategy = cursor.fetchone()

	if strategy['name'] == '信用至上':
		while True:
			access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/LoanList"
			data =  {
			  "PageIndex": 1, 
			}
			sort_data = rsa.sort(data)
			sign = rsa.sign(sort_data)
			list_result = json.loads(client.send(access_url, json.dumps(data), APPID, sign, AccessToken))

			for item in list_result['LoanInfos']:
				if item['CreditCode'] in ['AAA', 'AA']:
					access_url = "http://gw.open.ppdai.com/invest/BidService/Bidding"
					data = {
						"ListingId": item['ListingId'], 
						"Amount": 20,
					}
					sort_data = rsa.sort(data)
					sign = rsa.sign(sort_data)
					list_result = json.loads(client.send(access_url, json.dumps(data), APPID, sign, AccessToken))
					if list_result['Result'] == 0:
						cursor.execute("insert into bidding(OpenID, ListingId, strategyId, amount) values(%s,%s,%s,%s)", [session['OpenID'], list_result['ListingId'], strategy['id'], list_result['Amount']])

			time.sleep(300)
	else:
		pass

	closedb(db,cursor)


if __name__ == '__main__':
	app.run(debug=True)