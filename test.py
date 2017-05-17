#coding=utf-8
from openapi_client import openapi_client as client
from core.rsa_client import rsa_client as rsa
import json
import time
import datetime
import os
import pprint

appid = "2fb2e79ec7914ed99065d5cea99f95e0"

access_url = "http://gw.open.ppdai.com/open/openApiPublicQueryService/QueryUserNameByOpenID"
data = {
  "OpenID": "5588fe70850845bb99c03b429fdd7f65"
}
sort_data = rsa.sort(data)
sign = rsa.sign(sort_data)
list_result = client.send(access_url,json.dumps(data), appid, sign)
Username = list_result
# UserName = rsa.decrypt(list_result[list_result.find('<UserName>') + len('<UserName>'):list_result.find('</UserName>')])
print '111111', Username