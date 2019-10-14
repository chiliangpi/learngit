#######################
"""
按照MongoDB中priceRange_statistic_intl表的_id，调出相应_id的价格（仅供将mongo数据转化输出到Excel表中使用）
"""
#######################
import pandas as pd
import numpy as np
import json
from pymongo import *
import os
from io import StringIO

fileName = "intl_pricePercentile.csv"
array_id = pd.read_csv("/data/search/predict-2019/fline_id.csv", header=None).values.reshape(-1,)
clientMongo = MongoClient("10.0.1.212:27017", authSource="flight",
                          username='search', password='search@huoli123', authMechanism='MONGODB-CR')
cursor = clientMongo.flight.priceRange_statistic_intl
valueIO = StringIO()
for id in array_id:
    priceRange = cursor.find({'_id': id}, {'_id': 1, 'priceRange_basePercentile': 1, 'priceRange_baseAvg': 1})
    L = list(priceRange)
    df_percentile = pd.DataFrame(L[0]['priceRange_basePercentile'], columns=['percentile', L[0]['_id']])
    df_percentile[L[0]['_id']] = pd.to_numeric(df_percentile[L[0]['_id']], downcast='integer')
    df_avg = pd.DataFrame(pd.to_numeric(L[0]['priceRange_baseAvg']).reshape(1, 2), index=[L[0]['_id']])
    df = pd.concat([df_avg, df_percentile.T], axis=1, join='inner')
    df.to_csv(valueIO, mode='a', header=False)
valueIO.seek(0)
df = pd.read_csv(valueIO, index_col=0, names=np.arange(1, 13))
if fileName in os.listdir("/data/search/predict-2019/"):
    os.remove("/data/search/predict-2019/" + fileName)
df.to_csv("/data/search/predict-2019/"+fileName, header=True)



