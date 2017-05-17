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
import pickle
import json
import datetime
import os
import xmltodict

app = Flask(__name__)
app.config.from_object(__name__)

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

	return render_template('index.html', dataset=json.dumps(dataset))

# 个人中心
@app.route('/user')
def user():
	return render_template('user.html')

# 投资顾问
@app.route('/invest')
def invest():
	return render_template('invest.html')

# 授权登陆
@app.route('/auth')
def auth():
	code = request.values.get('code')
	authorizeStr = client.authorize(appid=APPID, code=code)
	# authorizeObj = pickle.loads(authorizeStr)
	
	(db,cursor) = connectdb()
	cursor.execute('insert into user(OpenId, content) values(%s, %s)', [type(authorizeStr), authorizeStr])
	closedb(db,cursor)
	return redirect(url_for('index'))

if __name__ == '__main__':
	app.run(debug=True)