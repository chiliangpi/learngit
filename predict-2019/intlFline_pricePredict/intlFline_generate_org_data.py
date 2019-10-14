# coding:utf-8
# -*- coding:utf-8 -*-

from datetime import datetime, timedelta
import os
import re
from io import StringIO
from pymongo import MongoClient
import time
from hdfs import *
# 内网：172.29.1.172
# 外网：120.133.0.172
# 测试内网：10.0.1.212
# 测试外网：123.56.222.127

clientMongo = MongoClient("172.29.1.172:27017", authSource="flight",
                     username='search', password='search@huoli123', authMechanism='MONGODB-CR')
cursor = clientMongo.flight.historyLowPrice_flightLine.find({})
todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
before2_dateStr1 = (datetime.now()-timedelta(days=2)).strftime('%Y%m%d')
fileName = "intlFline_pricePredict_org_data_" + todayStr1 + ".csv"
if fileName in os.listdir('/data/search/predict-2019/'):
    os.remove('/data/search/predict-2019/' + fileName)
f = open('/data/search/predict-2019/' + fileName, 'a')
f.write('flineId,predictDate,price\n')
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Start write Mongo data to Local=====")
for sample in cursor:
    if (not re.match(r'.*-.*', sample['_id'])) or ('DETAIL' in sample['_id']) or (',' in sample['_id']):
        continue
    if sample.__contains__('_id'):
        flineId = sample['_id']
        del sample['_id']
    else:
        flineId = 'None'
    if sample.__contains__('fn'):
        fline_fn = sample['fn']
        del sample['fn']
    else:
        fline_fn = 'None'
    if sample.__contains__('from'):
        fline_from = sample['from']
        del sample['from']
    else:
        fline_from = 'None'
    valueIO = StringIO()
    for k,v in sample.items():
        valueIO.write(str(flineId) + ',' + str(k) + ',' + str(v) + '\n')
    f.write(valueIO.getvalue())
f.close()
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish write Mongo data to Local=====")
fileName = "intlFline_pricePredict_org_data_" + before2_dateStr1 + ".csv"
if fileName in os.listdir('/data/search/predict-2019/'):
    os.remove('/data/search/predict-2019/' + fileName)  #删掉服务器上两天前的数据（只保留两天的数据）
clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search")
fileName = "intlFline_pricePredict_org_data_" + todayStr1 + ".csv"
if fileName in clientHdfs.list('/predict-2019/'):
    clientHdfs.delete("/predict-2019/" + fileName, recursive=True)
clientHdfs.upload('/predict-2019/' + fileName, '/data/search/predict-2019/' + fileName)
fileName = "intlFline_pricePredict_org_data_" + before2_dateStr1 + ".csv"
if fileName in clientHdfs.list('/predict-2019/'):
    clientHdfs.delete("/predict-2019/" + fileName, recursive=True)  #删掉HDFS上两天前的数据（只保留两天的数据）
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish put local data to HDFS=====")
