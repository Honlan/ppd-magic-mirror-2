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
	if 'OpenID' in session:
		result['is_auth'] = True
		result['Username'] = escape(session['Username'])
	else:
		result['is_auth'] = False
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

	return render_template('index.html', dataset=json.dumps(dataset), auth=is_auth())

# 个人中心
@app.route('/user')
def user():
	return render_template('user.html', auth=is_auth())

# 投资顾问
@app.route('/invest')
def invest():
	return render_template('invest.html', auth=is_auth())

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
	list_result = client.send(access_url, json.dumps(data), APPID, sign)
	Username = rsa.decrypt(list_result[list_result.find('<UserName>') + len('<UserName>'):list_result.find('</UserName>')])

	session['Username'] = Username

	(db,cursor) = connectdb()
	cursor.execute("select count(*) as count from user where OpenID=%s", [OpenID])
	count = count = cursor.fetchone()['count']
	if count > 0:
		cursor.execute('update user set AccessToken=%s, RefreshToken=%s, ExpiresIn=%s, AuthTimestamp=%s, Username=%s where OpenID=%s', [AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username, OpenID])
	else:
		cursor.execute('insert into user(OpenID, AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username) values(%s, %s, %s, %s, %s, %s)', [OpenID, AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username])
	closedb(db,cursor)

	return redirect(url_for('index'))

if __name__ == '__main__':
	app.run(debug=True)