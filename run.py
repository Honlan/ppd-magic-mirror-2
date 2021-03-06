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
# from celery import Celery
import pandas as pd
import requests
import threading
import logging
import math

from subprocess import Popen

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

# 日志系统配置
handler = logging.FileHandler(FILE_PREFIX + 'app.log', encoding='UTF-8')
logging_format = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)s - %(message)s')
handler.setFormatter(logging_format)
app.logger.addHandler(handler)

from sklearn.feature_extraction.text import TfidfVectorizer # 
from sklearn.feature_extraction.text import TfidfTransformer
from six.moves import cPickle as pickle
import jieba

class TFIDFPredictor:
	def __init__(self):
		self.vectorizer = TfidfVectorizer()

	def train(self, questions):
		fit = self.vectorizer.fit_transform(questions)

	def predict(self, test_question , questions):
		vector_doc = self.vectorizer.transform(questions)
		vector_context = self.vectorizer.transform([' '.join( jieba.cut( test_question ) )])
		result = np.dot(vector_doc, vector_context.T).todense()
		result = np.asarray(result).flatten()
		return np.argsort(result, axis=0)[::-1]


files = open(FILE_PREFIX + 'QA.pickle')
dataset = pickle.load(files)
files.close()
questions = []
answers = []
for i, j in dataset:
	questions.append(' '.join(jieba.cut(i)))
	answers.append(j)
TFIDF = TFIDFPredictor()
TFIDF.train(questions)

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
		result['count'] = count * 2 + count % 2
	else:
		result['is_auth'] = False
		result['count'] = count * 2 + count % 2
	return result

# 刷新AccessToken
def refresh():
	if 'OpenID' in session:
		(db,cursor) = connectdb()
		cursor.execute("select * from user where OpenID=%s", [session['OpenID']])
		d = cursor.fetchone()
		timestamp = d['AuthTimestamp']
		AccessToken = d['AccessToken']
		if (int(time.time()) > int(timestamp) + 3600 * 24 * 7) or (not session['AccessToken'] == AccessToken):
			while True:
				new_token_info = client.refresh_token(APPID, session['OpenID'], session['RefreshToken'])
				if new_token_info == '':
					continue
				new_token_info = json.loads(new_token_info)
				AuthTimestamp = int(time.time())
				AccessToken = new_token_info['AccessToken']
				RefreshToken = new_token_info['RefreshToken']
				session['AccessToken'] = AccessToken
				session['RefreshToken'] = RefreshToken
				session['AuthTimestamp'] = AuthTimestamp
				cursor.execute('update user set AccessToken=%s, RefreshToken=%s, AuthTimestamp=%s where OpenID=%s', [AccessToken, RefreshToken, AuthTimestamp, session['OpenID']])
				break
		closedb(db,cursor)
	return

# 重新生成个人报告
def report():
	if 'OpenID' in session:
		(db,cursor) = connectdb()

		cursor.execute("select timestamp, report from task where name=%s and OpenID=%s", ['bidBasicInfo', session['OpenID']])
		d = cursor.fetchall()
		if len(d) == 0:
			cursor.execute("insert into task(name, OpenID, status) values(%s, %s, %s)", ['bidBasicInfo', session['OpenID'], 'pending'])

			Popen('sudo python ' + FILE_PREFIX + 'history_basic.py ' + session['OpenID'] + ' ' + APPID + ' ' + session['AccessToken'] + ' ' + str(1180627200) + ' ' + session['Username'] + ' ' + FILE_PREFIX, shell=True)
			
			closedb(db,cursor)

			return
		else:
			d = d[0]
			timestamp = d['timestamp']
			s = d['report']
			if int(s) == 0:
				if int(time.time()) > int(timestamp) + 3600 * 24:
					cursor.execute("delete from task where name=%s and OpenID=%s", ['bidBasicInfo', session['OpenID']])
					cursor.execute("insert into task(name, OpenID, status) values(%s, %s, %s)", ['bidBasicInfo', session['OpenID'], 'pending'])

					Popen('sudo python ' + FILE_PREFIX + 'history_basic.py ' + session['OpenID'] + ' ' + APPID + ' ' + session['AccessToken'] + ' ' + str(int(timestamp) - 600) + ' ' + session['Username'] + ' ' + FILE_PREFIX, shell=True)
			
			closedb(db,cursor)
		return

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

# 首页
@app.route('/')
def index():
	if 'OpenID' in session:
		return redirect(url_for('home'))
	else:
		return render_template('index.html')

# 平台透视
@app.route('/home')
def home():
	if not 'OpenID' in session:
		return redirect(url_for('index'))

	refresh()
	# report()

	(db,cursor) = connectdb()

	# 删除较早登陆用户的session
	if 'OpenID' in session:
		cursor.execute("select AuthTimestamp from user where OpenID=%s", [session['OpenID']])
		timestamp = cursor.fetchone()['AuthTimestamp']
		if int(timestamp) < 1500088214:
			closedb(db,cursor)
			return redirect(url_for('logout'))
		

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

	return render_template('home.html', dataset=json.dumps(dataset), auth=is_auth())

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
	if not 'OpenID' in session:
		return redirect(url_for('index'))

	refresh()
	# report()
	
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
	if not 'OpenID' in session:
		return redirect(url_for('index'))

	refresh()
	# report()

	(db,cursor) = connectdb()
	cursor.execute("select * from json_data where page=%s",['user'])
	json_data = cursor.fetchall()
	
	dataset = {}
	dataset['json'] = {item['keyword']: json.loads(item['json']) for item in json_data}

	closedb(db,cursor)

	for key in ['daily_amount_sum', 'daily_amount_sum_back', 'daily_interest', 'daily_rate', 'daily_amount_average', 'daily_term', 'daily_interest_sum', 'daily_interest_sum_total']:
		dataset['json']['bid_stat'][key] = [float('%.1f' % d) for d in dataset['json']['bid_stat'][key]]

	dataset['age'] = '%.1f' % ((float(time.time()) - dataset['json']['bid_stat']['from']) / 3600 / 24 / 365)
	dataset['other'] = '75'
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
	if not 'OpenID' in session:
		return redirect(url_for('index'))

	refresh()
	# report()
	
	dataset = {}

	(db,cursor) = connectdb()
	cursor.execute("select * from json_data where page=%s",['invest'])
	json_data = cursor.fetchall()	
	dataset['json'] = {item['keyword']: json.loads(item['json']) for item in json_data}

	tmp = []
	for key, value in dataset['json']['strategy_stat'].items():
		value['name'] = key
		value[u'平均金额'] = int(value[u'总金额'] / value[u'数量'])
		for k, v in value.items():
			if k in ['平均利率', '平均期限']:
				value[k] = '%.2f' % value[k]
			elif k in ['10日坏标逾期率', '30日坏标逾期率', '90日坏标逾期率']:
				value[k] = '%.3f' % value[k]
		tmp.append(value)
	dataset['json']['strategy_stat'] = tmp

	cursor.execute("select * from strategy where OpenID=%s",['0'])
	dataset['sys'] = cursor.fetchall()
	cursor.execute("select * from strategy where OpenID=%s",[session['OpenID']])
	dataset['my'] = cursor.fetchall()
	cursor.execute("select strategy from user where OpenID=%s", [session['OpenID']])
	if len(dataset['my']) < 4:
		dataset['add'] = True
	else:
		dataset['add'] = False

	dataset['strategy_count'] = 0

	dataset['strategy_weight'] = {u'初始评级': 0, u'借款利率': 0, u'借款期限': 0}
	sys_strategy = cursor.fetchone()['strategy']
	if not sys_strategy == '':
		sys_strategy = sys_strategy.split('-')
		for s in sys_strategy:
			for x in xrange(0, len(dataset['sys'])):
				if int(dataset['sys'][x]['id']) == int(s):
					dataset['sys'][x]['active'] = 1
					dataset['strategy_count'] += 1
					content = json.loads(dataset['sys'][x]['content'])
					for key in content.keys():
						if dataset['strategy_weight'].has_key(key):
							dataset['strategy_weight'][key] += 1
					break

	for item in dataset['my']:
		if int(item['active']) == 1:
			content = json.loads(item['content'])
			for key in content.keys():
				if dataset['strategy_weight'].has_key(key):
					dataset['strategy_weight'][key] += 1

	dataset['strategy_weight'] = {'x': [key for key in dataset['strategy_weight'].keys()], 'data': [value for value in dataset['strategy_weight'].values()]}

	cursor.execute("select balance, balanceBid, balanceWithdraw from user where OpenID=%s", [session['OpenID']])
	dataset['balance'] = cursor.fetchone()

	cursor.execute("select count(*) as count from bidding where OpenID=%s", [session['OpenID']])
	dataset['bidding_count'] = cursor.fetchone()['count']

	cursor.execute("select count(*) as count from strategy where OpenID=%s and active=%s", [session['OpenID'], 1])
	dataset['strategy_count'] += cursor.fetchone()['count']

	dates = []
	start = 1483200000
	while True:
		b = time2str(start, '%Y-%m-%d')
		if b[:4] == '2017':
			dates.append(b)
			start += 3600 * 24
		else:
			break
	dataset['calendar'] = {'count':{d: 0 for d in dates}, 'amount':{d: 0 for d in dates}}

	cursor.execute("select * from bidding where OpenID=%s order by timestamp asc",[session['OpenID']])
	biddings = cursor.fetchall()
	for item in biddings:
		d = time2str(float(item['timestamp']), '%Y-%m-%d')
		dataset['calendar']['count'][d] += 1
		dataset['calendar']['amount'][d] += item['amount']

	dataset['calendar']['max_count'] = np.max([v for v in dataset['calendar']['count'].values()])
	dataset['calendar']['count'] = [[d, dataset['calendar']['count'][d], dataset['calendar']['amount'][d]] for d in dates]

	if len(biddings) > 0:
		cursor.execute("select ListingId, CreditCode, Months, CurrentRate, Title from listing where ListingId in %s", [[x['ListingId'] for x in biddings]])
		tmp = cursor.fetchall()
		tmp = {str(t['ListingId']): t for t in tmp}
		d = []
		for x in range(0, len(biddings)):
			lid = str(biddings[x]['ListingId'])
			if tmp.has_key(lid):
				biddings[x]['CreditCode'] = tmp[lid]['CreditCode']
				biddings[x]['Months'] = tmp[lid]['Months']
				biddings[x]['CurrentRate'] = tmp[lid]['CurrentRate']
				biddings[x]['Title'] = tmp[lid]['Title']
				biddings[x]['timestamp'] = time2str(float(biddings[x]['timestamp']), "%Y-%m-%d %H:%M:%S")
				d.append(biddings[x])
		biddings = d
		cursor.execute("select id, name from strategy where OpenID=%s or OpenID=%s", ['0', session['OpenID']])
		tmp = cursor.fetchall()
		tmp = {str(t['id']): t['name'] for t in tmp}
		for x in range(0, len(biddings)):
			biddings[x]['strategy'] = tmp[str(biddings[x]['strategyId'])]
	dataset['biddings'] = biddings

	while True:
		access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/LoanList"
		data =  {
		  "PageIndex": 1, 
		}
		sort_data = rsa.sort(data)
		sign = rsa.sign(sort_data)
		list_result = client.send(access_url, json.dumps(data), APPID, sign)
		if list_result == '':
			continue
		else:
			list_result = json.loads(list_result)
			break
	mapping = {}
	mapping['AAA'] = 15
	mapping['AA'] = 10
	mapping['A'] = 5
	mapping['B'] = 3
	mapping['C'] = 1
	mapping['D'] = 0
	mapping['E'] = 0
	mapping['F'] = 0
	colorset = {}
	colorset['AAA'] = 'rgba(243, 230, 162, 0.9)'
	colorset['AA'] = 'rgba(234, 126, 83, 0.9)'
	colorset['A'] = 'rgba(221, 107, 102, 0.9)'
	colorset['B'] = 'rgba(230, 157, 135, 0.9)'
	colorset['C'] = 'rgba(215, 135, 230, 0.9)'
	colorset['D'] = 'rgba(110, 113, 199, 0.9)' 
	colorset['E'] = 'rgba(109, 188, 191, 0.9)'
	colorset['F'] = 'rgba(117, 179, 117, 0.9)'
	for x in range(0, len(list_result['LoanInfos'])):
		item = list_result['LoanInfos'][x]
		list_result['LoanInfos'][x]['score'] = mapping[item['CreditCode']] * (int(item['Rate']) - 9) * (6 / float(item['Months']))
		list_result['LoanInfos'][x]['ratio'] = 95 - 95 * item['RemainFunding'] / item['Amount']
		list_result['LoanInfos'][x]['color'] = colorset[item['CreditCode']]
	list_result['LoanInfos'].sort(key=lambda x:x['score'], reverse=True)
	dataset['listings'] = list_result['LoanInfos'][:10]
	if len(dataset['listings']) == 0:
		data['listing_max'] = 0.0000001
	else:
		dataset['listing_max'] = np.max([float(x['score']) for x in dataset['listings']])
	for x in xrange(0, len(dataset['listings'])):
		dataset['listings'][x]['score'] = '%.2f' % (0.98 * dataset['listings'][x]['score'] / dataset['listing_max'])
		dataset['listings'][x]['Amount'] = int(dataset['listings'][x]['Amount'])
		dataset['listings'][x]['RemainFunding'] = int(dataset['listings'][x]['RemainFunding'])

	closedb(db,cursor)

	return render_template('invest.html', auth=is_auth(), datasetJson=json.dumps(dataset), dataset=dataset)

# 交流社区
@app.route('/chat')
def chat():
	if not 'OpenID' in session:
		return redirect(url_for('index'))

	refresh()
	# report()
	
	dataset = {}

	(db,cursor) = connectdb()

	cursor.execute("select * from json_data where page=%s",['chat'])
	json_data = cursor.fetchall()
	
	dataset['json'] = {item['keyword']: json.loads(item['json']) for item in json_data}

	cursor.execute("select * from news where source=%s order by timestamp desc limit 5", ['拍拍贷新闻'])
	dataset['ppdnews'] = cursor.fetchall()
	for x in xrange(0, len(dataset['ppdnews'])):
		dataset['ppdnews'][x]['timestamp'] = time2str(float(dataset['ppdnews'][x]['timestamp']), '%Y-%m-%d %H:%M:%S')

	cursor.execute("select * from news where source=%s order by timestamp desc limit 5", ['拍拍贷公告'])
	dataset['ppdnotes'] = cursor.fetchall()
	for x in xrange(0, len(dataset['ppdnotes'])):
		dataset['ppdnotes'][x]['timestamp'] = time2str(float(dataset['ppdnotes'][x]['timestamp']), '%Y-%m-%d %H:%M:%S')

	cursor.execute("select * from news where source=%s and thumb!=%s order by timestamp desc limit 4", ['今日头条', ''])
	dataset['toutiao'] = cursor.fetchall()
	for x in xrange(0, len(dataset['toutiao'])):
		dataset['toutiao'][x]['timestamp'] = time2str(float(dataset['toutiao'][x]['timestamp']), '%Y-%m-%d')

	cursor.execute("select * from news where source=%s limit 200", ['拍拍贷问答'])
	dataset['ppdqa'] = cursor.fetchall()
	count = 0
	for x in xrange(0, len(dataset['ppdqa'])):
		dataset['ppdqa'][x]['thumb'] = url_for('static',filename='img/avatar/') + str(1 + int(random.random() * 15)) + '.png'
		dataset['ppdqa'][x]['content'] = dataset['ppdqa'][x]['content'].split('^')
		dataset['ppdqa'][x]['q'] = dataset['ppdqa'][x]['content'][0]
		dataset['ppdqa'][x]['a'] = []
		if len(dataset['ppdqa'][x]['content']) > 3:
			N = 3
		else:
			N = len(dataset['ppdqa'][x]['content'])
		for i in range(1, N):
			dataset['ppdqa'][x]['a'].append(dataset['ppdqa'][x]['content'][i])
	tmp = []
	while count < 3:
		tmp.append(dataset['ppdqa'][int(random.random() * len(dataset['ppdqa']))])
		count += 1
	dataset['ppdqa'] = tmp

	cursor.execute("select * from news where source=%s limit 200", ['拍拍贷交流'])
	dataset['ppdchat'] = cursor.fetchall()
	count = 0
	for x in xrange(0, len(dataset['ppdchat'])):
		dataset['ppdchat'][x]['thumb'] = url_for('static',filename='img/avatar/') + str(1 + int(random.random() * 15)) + '.png'
		dataset['ppdchat'][x]['content'] = dataset['ppdchat'][x]['content'].split('^')
		dataset['ppdchat'][x]['q'] = dataset['ppdchat'][x]['content'][0]
		dataset['ppdchat'][x]['a'] = []
		if len(dataset['ppdchat'][x]['content']) > 3:
			N = 3
		else:
			N = len(dataset['ppdchat'][x]['content'])
		for i in range(1, N):
			dataset['ppdchat'][x]['a'].append(dataset['ppdchat'][x]['content'][i])
	tmp = []
	while count < 3:
		tmp.append(dataset['ppdchat'][int(random.random() * len(dataset['ppdchat']))])
		count += 1
	dataset['ppdchat'] = tmp

	cursor.execute("select * from news where source=%s", ['拍拍贷笔记'])
	dataset['ppdshare'] = cursor.fetchall()
	count = 0
	tmp = []
	while count < 8:
		item = dataset['ppdshare'][int(random.random() * len(dataset['ppdshare']))]
		if len(item['content']) < 100:
			continue
		tmp.append(item)
		count += 1
	dataset['ppdshare'] = tmp
	colors = ['rgba(84, 148, 191, 1)','rgba(221, 107, 102, 1)','rgba(230, 157, 135, 1)','rgba(234, 126, 83, 1)','rgba(243, 230, 162, 1)']
	for x in xrange(0, len(dataset['ppdshare'])):
		dataset['ppdshare'][x]['color'] = colors[int(math.floor(random.random() * len(colors)))]

	closedb(db,cursor)
	
	return render_template('chat.html', auth=is_auth(), datasetJson=json.dumps(dataset), dataset=dataset)

# 授权登陆
@app.route('/auth')
def auth():
	code = request.values.get('code')
	while True:
		try:
			authorizeStr = client.authorize(appid=APPID, code=code)
			authorizeObj = json.loads(authorizeStr)

			OpenID = str(authorizeObj['OpenID'])
			AccessToken = authorizeObj['AccessToken']
			RefreshToken = authorizeObj['RefreshToken']
			ExpiresIn = authorizeObj['ExpiresIn']
			AuthTimestamp = int(time.time())
		except Exception, e:
			continue
		else:
			break
		finally:
			pass

	session['OpenID'] = OpenID
	session['AccessToken'] = AccessToken
	session['RefreshToken'] = RefreshToken
	session['AuthTimestamp'] = AuthTimestamp
	session['ExpiresIn'] = ExpiresIn

	while True:
		access_url = "http://gw.open.ppdai.com/open/openApiPublicQueryService/QueryUserNameByOpenID"
		data = {
		  "OpenID": OpenID
		}
		sort_data = rsa.sort(data)
		sign = rsa.sign(sort_data)
		list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
		if list_result == '':
			continue
		else:
			list_result = json.loads(list_result)
			break

	Username = rsa.decrypt(list_result['UserName'])
	session['Username'] = Username

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

	(db,cursor) = connectdb()
	cursor.execute("select count(*) as count from user where OpenID=%s", [OpenID])
	count = cursor.fetchone()['count']
	if count > 0:
		cursor.execute('update user set AccessToken=%s, RefreshToken=%s, ExpiresIn=%s, AuthTimestamp=%s, Username=%s, balance=%s, balanceBid=%s, balanceWithdraw=%s where OpenID=%s', [AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username, balance[1]['Balance'], balance[0]['Balance'], balance[2]['Balance'], OpenID])
	else:
		cursor.execute('insert into user(OpenID, AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username, balance, balanceBid, balanceWithdraw) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)', [OpenID, AccessToken, RefreshToken, ExpiresIn, AuthTimestamp, Username, balance[1]['Balance'], balance[0]['Balance'], balance[2]['Balance']])
	
	# 是否需要获取个人投资记录
	'''
	cursor.execute("select count(*) as count from task where name=%s and OpenID=%s", ['bidBasicInfo', session['OpenID']])
	count = cursor.fetchone()['count']
	if count == 0:
		cursor.execute("insert into task(name, OpenID, status) values(%s, %s, %s)", ['bidBasicInfo', session['OpenID'], 'pending'])

		Popen('sudo python ' + FILE_PREFIX + 'history_basic.py ' + session['OpenID'] + ' ' + APPID + ' ' + session['AccessToken'] + ' ' + str(1180627200) + ' ' + session['Username'] + ' ' + FILE_PREFIX, shell=True)
	'''

	closedb(db,cursor)

	return redirect(url_for('home'))

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
	for key in data.keys():
		data[key] = data[key][0].split('_')
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

	Popen('sudo python ' + FILE_PREFIX + 'strategy_autobid.py ' + str(data['strategyId']) + ' ' + session['OpenID'] + ' ' + APPID + ' ' + session['AccessToken'] + ' ' + session['Username'] + ' ' + FILE_PREFIX, shell=True)
	
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

# 策略投标详情
@app.route('/strategy_content', methods=['POST'])
def strategy_content():
	(db,cursor) = connectdb()

	data = request.form
	cursor.execute("select * from strategy where id=%s", [data['strategyId']])
	strategy = cursor.fetchone()
	strategy['content'] = json.loads(strategy['content'])

	closedb(db,cursor)
	return json.dumps({'result': 'ok', 'msg': '获取策略数据成功', 'strategy': strategy})

# 聊天
@app.route('/chatbot', methods=['POST'])
def chatbot():
	data = request.form
	keys = TULINGKEY.split('-')
	for k in keys:
		data_dict = {
			'key': k,
			'info': data['message'],
			'userid': 'ppd-deep-invest-zhl'
		}
		r = requests.post('http://www.tuling123.com/openapi/api', data=data_dict).json()
		if not str(r['code']) == '40004':
			break
	question = data['message']
	answer = r['text']

	if random.random() < 0.5:
		sort = TFIDF.predict(question, questions)
		match_q = questions[int(sort[0])]
		match_a = answers[int(sort[0])]
		answer = match_a

	(db,cursor) = connectdb()

	if 'OpenID' in session:
		cursor.execute('insert into chatting(post,response,OpenID,timestamp) values(%s, %s, %s, %s)', [question,answer,session['OpenID'],int(time.time())])
	else:
		cursor.execute('insert into chatting(post,response,OpenID,timestamp) values(%s, %s, %s, %s)', [question,answer,'',int(time.time())])

	closedb(db,cursor)
	return json.dumps({'result': 'ok', 'msg': answer})

if __name__ == '__main__':
	app.run(debug=True)