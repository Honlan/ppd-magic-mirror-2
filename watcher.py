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

# ppdai api
from openapi_client import openapi_client as client
from core.rsa_client import rsa_client as rsa
import json
import datetime
import os
from datetime import timedelta

from run import time2str, str2time, get_previous_month, get_next_month, connectdb, closedb

(db,cursor) = connectdb()

cursor.execute("select * from user where data=%s", [''])
users = cursor.fetchall()

for user in users:
	OpenID = user['OpenID']
	AccessToken = user['AccessToken']
	Username = user['Username']
	cursor.execute("select count(*) as count from task where OpenID=%s", [OpenID])
	count = cursor.fetchone()['count']
	if count > 0:
		continue

	print Username
	cursor.execute("insert into task(name, OpenID, status) values(%s, %s, %s)", ['bidBasicInfo', OpenID, 'pending'])
	current = int(time.time()) + 3600 * 24
	StartTime = 1180627200
	access_url = "http://gw.open.ppdai.com/invest/BidService/BidList"
	listings = []
	total = 0
	while current > StartTime:
		PageIndex = 1
		while True:
			if current - 3600 * 24 * 30 > StartTime:
				data = {
					"StartTime": time.strftime('%Y-%m-%d', time.localtime(float(current - 3600 * 24 * 30))), 
					"EndTime": time.strftime('%Y-%m-%d', time.localtime(float(current))), 
					"PageIndex": PageIndex, 
					"PageSize": 500
				}
			else:
				data = {
					"StartTime": time.strftime('%Y-%m-%d', time.localtime(float(StartTime))), 
					"EndTime": time.strftime('%Y-%m-%d', time.localtime(float(current))), 
					"PageIndex": 1, 
					"PageSize": 500
				}

			while True:
				sort_data = rsa.sort(data)
				sign = rsa.sign(sort_data)
				list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)

				if list_result == '':
					continue
				else:
					list_result = json.loads(list_result)
					break

			for item in list_result['BidList']:
				if int(item['ListingId']) == 0:
					continue
				total += 1
				listings.append([item['ListingId'], str(item['Title']), item['Months'], item['Rate'], item['Amount'], OpenID])

			cursor.execute("update task set history_basic=%s where name=%s and OpenID=%s", [time2str(current, '%Y-%m-%d') + '_' + str(PageIndex) + '_' + str(total), 'bidBasicInfo', OpenID])

			PageIndex += 1
			if PageIndex > int(list_result['TotalPages']):
				break

		current -= 3600 * 24 * 30

	if len(listings) > 0:
		cursor.execute("delete from listing where ListingId in %s", [[x[0] for x in listings]])
		cursor.executemany("insert into listing(ListingId, Title, Months, CurrentRate, Amount, OpenID) values(%s, %s, %s, %s, %s, %s)", listings)
		del listings[:]

	while True:
		cursor.execute("select * from listing where LenderCount=%s and OpenID=%s limit 200", ['', OpenID])
		listings = cursor.fetchall()

		# 获取详情
		ListingIds = [x['ListingId'] for x in listings]
		many = []
		for x in range(0, len(ListingIds), 10):
			if x + 10 <= len(ListingIds):
				y = x + 10
			else:
				y = len(ListingIds)
			while True:
				time.sleep(0.5)
				access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/BatchListingInfos"
				data = {"ListingIds": ListingIds[x:y]}
				sort_data = rsa.sort(data)
				sign = rsa.sign(sort_data)
				list_result = client.send(access_url, json.dumps(data), APPID, sign)
				if list_result == '':
					continue
				list_result = json.loads(list_result)
				for item in list_result['LoanInfos']:
					many.append([str(item['FistBidTime']), str(item['LastBidTime']), item['LenderCount'], str(item['AuditingTime']), item['RemainFunding'], item['DeadLineTimeOrRemindTimeStr'], item['CreditCode'], item['Amount'], item['Months'], item['CurrentRate'], item['BorrowName'], item['Gender'], item['EducationDegree'], item['GraduateSchool'], item['StudyStyle'], item['Age'], item['SuccessCount'], item['WasteCount'], item['CancelCount'], item['FailedCount'], item['NormalCount'], item['OverdueLessCount'], item['OverdueMoreCount'], item['OwingPrincipal'], item['OwingAmount'], item['AmountToReceive'], str(item['FirstSuccessBorrowTime']), str(item['RegisterTime']), item['CertificateValidate'], item['NciicIdentityCheck'], item['PhoneValidate'], item['VideoValidate'], item['CreditValidate'], item['EducateValidate'], str(item['LastSuccessBorrowTime']), item['HighestPrincipal'], item['HighestDebt'], item['TotalPrincipal'], item['ListingId']])
				break
		cursor.executemany("update listing set FistBidTime=%s, LastBidTime=%s, LenderCount=%s, AuditingTime=%s, RemainFunding=%s, DeadLineTimeOrRemindTimeStr=%s, CreditCode=%s, Amount=%s, Months=%s, CurrentRate=%s, BorrowName=%s, Gender=%s, EducationDegree=%s, GraduateSchool=%s, StudyStyle=%s, Age=%s, SuccessCount=%s, WasteCount=%s, CancelCount=%s, FailedCount=%s, NormalCount=%s, OverdueLessCount=%s, OverdueMoreCount=%s, OwingPrincipal=%s, OwingAmount=%s, AmountToReceive=%s, FirstSuccessBorrowTime=%s, RegisterTime=%s, CertificateValidate=%s, NciicIdentityCheck=%s, PhoneValidate=%s, VideoValidate=%s, CreditValidate=%s, EducateValidate=%s, LastSuccessBorrowTime=%s, HighestPrincipal=%s, HighestDebt=%s, TotalPrincipal=%s where ListingId=%s", many)
		print time2str(int(time.time()), '%Y-%m-%d %H:%M:%S'), 'detail finished'

		# 获取投资金额
		many = []
		cursor.execute("delete from lender where ListingId in %s", [ListingIds])
		for x in range(0, len(ListingIds), 5):
			if x + 5 <= len(ListingIds):
				y = x + 5
			else:
				y = len(ListingIds)
			while True:
				time.sleep(0.5)
				access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/BatchListingBidInfos"
				data = {"ListingIds": ListingIds[x:y]}
				sort_data = rsa.sort(data)
				sign = rsa.sign(sort_data)
				list_result = client.send(access_url, json.dumps(data), APPID, sign)
				if list_result == '':
					continue
				list_result = json.loads(list_result)
				for item in list_result['ListingBidsInfos']:
					for i in item['Bids']:
						many.append([item['ListingId'], i['LenderName'], i['BidAmount'], i['BidDateTime']])
		cursor.executemany("insert into lender(ListingId, LenderName, BidAmount, BidDateTime) values(%s, %s, %s, %s)", many)					
		print time2str(int(time.time()), '%Y-%m-%d %H:%M:%S'), 'money finished'

		# 获取还款记录
		many = []
		cursor.execute("delete from payback where ListingId in %s", [ListingIds])
		for x in ListingIds:
			while True:
				time.sleep(0.5)
				access_url = "http://gw.open.ppdai.com/invest/RepaymentService/FetchLenderRepayment"
				data = {"ListingId": x}
				sort_data = rsa.sort(data)
				sign = rsa.sign(sort_data)
				list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)
				if list_result == '':
					continue
				list_result = json.loads(list_result)
				for item in list_result['ListingRepayment']:
					many.append([item['ListingId'], item['OrderId'], item['DueDate'], item['RepayDate'], item['RepayPrincipal'], item['RepayInterest'], item['OwingPrincipal'], item['OwingInterest'], item['OwingOverdue'], item['OverdueDays'], item['RepayStatus'], OpenID])
		cursor.executemany("insert into payback(ListingId, OrderId, DueDate, RepayDate, RepayPrincipal, RepayInterest, OwingPrincipal, OwingInterest, OwingOverdue, OverdueDays, RepayStatus, OpenID) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", many)
		print time2str(int(time.time()), '%Y-%m-%d %H:%M:%S'), 'payback finished'

		# 获取标的状态
		many = []
		for x in range(0, len(ListingIds), 20):
			if x + 20 <= len(ListingIds):
				y = x + 20
			else:
				y = len(ListingIds)
			while True:
				time.sleep(0.5)
				access_url = "http://gw.open.ppdai.com/invest/LLoanInfoService/BatchListingStatusInfos"
				data = {"ListingIds": ListingIds[x:y]}
				sort_data = rsa.sort(data)
				sign = rsa.sign(sort_data)
				list_result = client.send(access_url, json.dumps(data), APPID, sign)
				if list_result == '':
					continue
				list_result = json.loads(list_result)
				for item in list_result['Infos']:
					many.append([item['Status'], item['ListingId']])
		cursor.executemany("update listing set Status=%s where ListingId=%s", many)
		print time2str(int(time.time()), '%Y-%m-%d %H:%M:%S'), 'status finished'

		cursor.execute("select count(*) as count from listing where LenderCount=%s and OpenID", ['', OpenID])
		count = cursor.fetchone()['count']
		print time2str(int(time.time()), '%Y-%m-%d %H:%M:%S'), '剩余标的数量：', count
		cursor.execute("update task set history_detail=%s where name=%s and OpenID=%s",['total_' + str(total) + '_left_' + str(count), 'bidBasicInfo', OpenID])
		cursor.execute("update task set history_money=%s where name=%s and OpenID=%s",['total_' + str(total) + '_left_' + str(count), 'bidBasicInfo', OpenID])
		cursor.execute("update task set history_payback=%s where name=%s and OpenID=%s",['total_' + str(total) + '_left_' + str(count), 'bidBasicInfo', OpenID])
		cursor.execute("update task set history_status=%s where name=%s and OpenID=%s",['total_' + str(total) + '_left_' + str(count), 'bidBasicInfo', OpenID])

		if count == 0:
			cursor.execute("update task set d0=%s, m0=%s, p0=%s, s0=%s, timestamp=%s where name=%s and OpenID=%s", [1, 1, 1, 1, int(time.time()), 'bidBasicInfo', OpenID])
			break

	# 生成个人中心
	cursor.execute("select ListingId from lender where LenderName=%s", [Username])
	ListingIds = cursor.fetchall()
	ListingIds = [x['ListingId'] for x in ListingIds]
	
	if len(ListingIds) == 0:
		cursor.execute("update user set data=%s where OpenID=%s", ['', OpenID])
	else:
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

		cursor.execute("update task set history_user=%s where name=%s and OpenID=%s", ['stage1: load data', 'bidBasicInfo', OpenID])

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

		if not time2str(end, '%Y/%m/%d')[2:] in dates:
			dates.append(time2str(end, '%Y/%m/%d')[2:])

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

		cursor.execute("update task set history_user=%s where name=%s and OpenID=%s", ['stage2: bid_stat', 'bidBasicInfo', OpenID])

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
		
		start = np.min([d['借款成功时间戳'] for d in data_dict])
		end = np.max([d['借款成功时间戳'] for d in data_dict])

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

		cursor.execute("update task set history_user=%s where name=%s and OpenID=%s", ['stage3: bid_flow', 'bidBasicInfo', OpenID])

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

		cursor.execute("update task set history_user=%s where name=%s and OpenID=%s", ['stage4: bid_term', 'bidBasicInfo', OpenID])

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

		cursor.execute("update task set history_user=%s where name=%s and OpenID=%s", ['stage5: bid_terms_history', 'bidBasicInfo', OpenID])

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
		    stats[2][x]['value'] = [0 if np.isnan(i) else i for i in stats[2][x]['value']]
		    
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
		indicators[1][1]['max'] = 1
		indicators[1][2]['max'] = 1
		indicators[1][3]['max'] = 1
		indicators[1][4]['max'] = 1
		indicators[1][5]['max'] = 1

		indicators[2][0]['name'] = '成功借款次数'
		indicators[2][1]['name'] = '成功借款金额'
		indicators[2][3]['name'] = '正常还款期数'
		indicators[2][4]['name'] = '逾期还款期数'

		# indicators[1][5]['max'] = 2

		profile['bid_radar'] = {'indicators': indicators, 'data': stats, 'legend': rates}

		cursor.execute("update task set history_user=%s where name=%s and OpenID=%s", ['stage6: bid_radar', 'bidBasicInfo', OpenID])

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

		max_values = {'interest': {key: 0.0000001 for key in params.keys()}, 'bad': {key: 0.0000001 for key in params.keys()}}
		max_values_r = {'interest': {key: 0.0000001 for key in params.keys()}, 'bad': {key: 0.0000001 for key in params.keys()}}
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

		cursor.execute("update task set history_user=%s where name=%s and OpenID=%s", ['stage7: bid_bad finish', 'bidBasicInfo', OpenID])

		cursor.execute("update user set data=%s where OpenID=%s", [json.dumps(profile), OpenID])

	cursor.execute("update task set report=%s where name=%s and OpenID=%s", [0, 'bidBasicInfo', OpenID])
	cursor.execute("update listing set OpenID=%s where OpenID=%s", ['', OpenID])
	print time2str(int(time.time()), '%Y-%m-%d %H:%M:%S'), '完成用户，', Username

