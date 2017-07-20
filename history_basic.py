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
from subprocess import Popen

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
StartTime = int(sys.argv[4])
Username = sys.argv[5]

try:
	(db,cursor) = connectdb()
	app.logger.error(str(OpenID) + ' history_basic start')

	access_url = "http://gw.open.ppdai.com/invest/BidService/BidList"
	current = int(time.time()) + 3600 * 24

	listings = []
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
				time.sleep(1)
				sort_data = rsa.sort(data)
				sign = rsa.sign(sort_data)
				list_result = client.send(access_url, json.dumps(data), APPID, sign, AccessToken)

				if list_result == '':
					continue
				else:
					list_result = json.loads(list_result)
					break

			app.logger.error(str(OpenID) + ' history_basic ' + time2str(current, '%Y-%m-%d') + ' ' + str(PageIndex) + ' ' + str(len(list_result['BidList'])))
			for item in list_result['BidList']:
				if int(item['ListingId']) == 0:
					continue
				listings.append([item['ListingId'], str(item['Title']), item['Months'], item['Rate'], item['Amount'], OpenID])

			PageIndex += 1
			if PageIndex > int(list_result['TotalPages']):
				break

		current -= 3600 * 24 * 30

	if len(listings) > 0:
		app.logger.error(str(OpenID) + ' history_basic total records ' + str(len(listings)))
		cursor.execute("delete from listing where ListingId in %s", [[x[0] for x in listings]])
		cursor.executemany("insert into listing(ListingId, Title, Months, CurrentRate, Amount, OpenID) values(%s, %s, %s, %s, %s, %s)", listings)
	
	cursor.execute("update task set status=%s, timestamp=%s where name=%s and OpenID=%s", ['finished', int(time.time()), 'bidBasicInfo', OpenID])
	closedb(db,cursor)

	app.logger.error(str(OpenID) + ' history_basic finish')
except Exception, e:
	app.logger.error(e)
else:
	Popen('python history_detail.py ' + OpenID + ' ' + APPID + ' ' + AccessToken, shell=True)
	Popen('python history_money.py ' + OpenID + ' ' + APPID + ' ' + AccessToken, shell=True)
	Popen('python history_status.py ' + OpenID + ' ' + APPID + ' ' + AccessToken, shell=True)
	Popen('python history_payback.py ' + OpenID + ' ' + APPID + ' ' + AccessToken, shell=True)
	Popen('python history_user.py ' + OpenID + ' ' + Username, shell=True)
finally:
	pass