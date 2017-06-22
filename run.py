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
	return render_template('invest.html', auth=is_auth())

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
	data = {"OpenID": OpenID}
	sort_data = rsa.sort(data)
	sign = rsa.sign(sort_data)
	list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
	Username = rsa.decrypt(list_result[list_result.find('<UserName>') + len('<UserName>'):list_result.find('</UserName>')])

	session['Username'] = Username

	(db,cursor) = connectdb()
	cursor.execute("select count(*) as count from user where OpenID=%s", [OpenID])
	count = cursor.fetchone()['count']
	if count > 0:
		cursor.execute('update user set AccessToken=%s, RefreshToken=%s, ExpiresIn=%s, AuthTimestamp=%s, Username=%s where OpenID=%s', [AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username, OpenID])
	else:
		cursor.execute('insert into user(OpenID, AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username) values(%s, %s, %s, %s, %s, %s)', [OpenID, AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username])
	closedb(db,cursor)

	return redirect(url_for('index'))

if __name__ == '__main__':
	app.run(debug=True)