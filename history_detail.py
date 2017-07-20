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

# ppdai api
from openapi_client import openapi_client as client
from core.rsa_client import rsa_client as rsa
import json
import datetime
import os
from datetime import timedelta

from run import app, time2str, str2time, get_previous_month, get_next_month, connectdb, closedb

OpenID = sys.argv[1]
APPID = sys.argv[2]
AccessToken = sys.argv[3]

try:
	(db,cursor) = connectdb()

	app.logger.error(str(OpenID) + ' history_detail ready')

	flag = True
	while True:
		cursor.execute("select status from task where name=%s and OpenID=%s", ['bidBasicInfo', OpenID])
		status = cursor.fetchall()
		if len(status) == 0:
			flag = False
			break
		else:
			status = status[0]['status']

		if status == 'finished':
			break
		else:
			time.sleep(2)

	if flag:
		app.logger.error(str(OpenID) + ' history_detail start')

		cursor.execute("select ListingId from listing where OpenID=%s", [OpenID])
		ListingIds = cursor.fetchall()
		ListingIds = [x['ListingId'] for x in ListingIds]
		many = []
		for x in range(0, len(ListingIds), 10):
			if x + 10 <= len(ListingIds):
				y = x + 10
			else:
				y = len(ListingIds)
			while True:
				time.sleep(2)
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

		if len(many) > 0:
			cursor.executemany("update listing set FistBidTime=%s, LastBidTime=%s, LenderCount=%s, AuditingTime=%s, RemainFunding=%s, DeadLineTimeOrRemindTimeStr=%s, CreditCode=%s, Amount=%s, Months=%s, CurrentRate=%s, BorrowName=%s, Gender=%s, EducationDegree=%s, GraduateSchool=%s, StudyStyle=%s, Age=%s, SuccessCount=%s, WasteCount=%s, CancelCount=%s, FailedCount=%s, NormalCount=%s, OverdueLessCount=%s, OverdueMoreCount=%s, OwingPrincipal=%s, OwingAmount=%s, AmountToReceive=%s, FirstSuccessBorrowTime=%s, RegisterTime=%s, CertificateValidate=%s, NciicIdentityCheck=%s, PhoneValidate=%s, VideoValidate=%s, CreditValidate=%s, EducateValidate=%s, LastSuccessBorrowTime=%s, HighestPrincipal=%s, HighestDebt=%s, TotalPrincipal=%s where ListingId=%s", many)

		cursor.execute("update task set d0=%s, timestamp=%s where name=%s and OpenID=%s", [1, int(time.time()), 'bidBasicInfo', OpenID])

		closedb(db,cursor)

		app.logger.error(str(OpenID) + ' history_detail finish')
except Exception, e:
	app.logger.error(e)
else:
	pass
finally:
	pass