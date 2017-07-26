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

while True:
	cursor.execute("select * from listing where LenderCount=%s limit 500", [''])
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

	cursor.execute("select count(*) as count from listing where LenderCount=%s", [''])
	count = cursor.fetchone()['count']
	print time2str(int(time.time()), '%Y-%m-%d %H:%M:%S'), '剩余标的数量：', count
