# coding:utf-8
# -*- coding:utf-8 -*-
###########
#首先将historyLowPrice_fn_domestic表中的所有Id保存，便于开启多进程进行遍历
###########

from pymongo import MongoClient
from io import StringIO
clientMongo = MongoClient("172.29.1.172", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR', connect=False)
cursor = clientMongo.flight.historyLowPrice_fn_domestic
stringIO_temp = StringIO()
count_temp = 0
with open('/data/search/predict-2019/idList_temp.txt', 'a') as f_temp:  #遍历mongoDB中的ID，写入文件
    for sample in cursor.find():
        count_temp += 1
        stringIO_temp.write(sample.get('_id') + ',')
        if count_temp % 1000 == 0:
            f_temp.write(stringIO_temp.getvalue())
            stringIO_temp = StringIO()
    f_temp.write(stringIO_temp.getvalue())




###########
#多进程整理国内低价的原始数据
###########
import os, time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient
from functools import lru_cache
from math import radians, sin, cos, asin, sqrt, ceil
from multiprocessing import Pool, Process
from io import StringIO
import logging
from logging import handlers

class Timer():
    def __init__(self):
        self.startTime = None

    def start(self):
        self.startTime = time.time()

    # def stop(self):
    #     self.endTime = time.time()
    #     logger.info('Time taken: {} ms'.format(round((self.endTime - self.startTime)*1000)))

    def cost(self):
        self.endTime = time.time()
        self.costTime = round((self.endTime - self.startTime)*1000)
        # logger.info('costTime is: '.format(self.costTime))
        return self.costTime

def confirm_departDate(df):  #判断起飞日期逻辑
    if df.checkDate[5:] > df.departMonthDay:
        df['departDate'] = str(int(df.checkDate[:4]) + 1) + '-' + df.departMonthDay
    else:
        df['departDate'] = df.checkDate[:4] + '-' + df.departMonthDay
    return df['departDate']


def roll_minPrice(x, priceList):  # 查询日期至起飞日期期间内的最低价格的计算逻辑
    try:
        priceList.pop(0)
        return min(priceList)
    except:
        return None

def trend(df):  #根据查询日期至起飞日期期间内的最低价格，得出当前日期之后的价格趋势
    try:
        if (df['price'] - df['futureMinPrice']) / df['price'] > 0.1:  # 上涨
            return 2
        elif abs(df['price'] - df['futureMinPrice']) / df['price'] <= 0.1:  # 平稳
            return 0
        else:  # 下降
            return 1
    except:  #存在缺失值的情况
        return None


# @lru_cache(maxsize=1024 * 1024 * 512)
def org_price(id, cursor1):
    Airlines = id[:2]
    flag = 0
    for i in range(32):  #在fd_domestic_a(原价)表中，从当前日期往前找60天的数据，如果能找到，flag=1(即mongoDB中距离今天最近的数据作为原价)
        idDate = datetime.strftime(datetime.today() + timedelta(i), '%Y-%m-%d')
        List = id.split('_')[1:3] + [idDate, 'fd']
        orgPrice_id = '_'.join(List)
        if cursor1.find_one({'_id': orgPrice_id}):
            flag = 1
            break
    if flag == 1:
        try:
            orgPrice = int(cursor1.find_one({'_id': orgPrice_id}).get(Airlines).get('Y').get('price'))
        except:
            orgPrice = None
    else:
        orgPrice = None
    return orgPrice

def discount_rate(df):  #根据机票原价和当前价格，计算折扣率
    try:
        discount = round(df.price / df.orgPrice * 100, 2)
    except:
        discount = None
    return discount

'''
给定两个地址的坐标，计算返回两地相距多少公里 
demo:
address1=成都 lon1 = 104.071000 lat1 = 30.670000
address2=宜宾 104.622000 lat2 = 28.765000
'''
# @lru_cache(maxsize=1024 * 1024 * 512)
def get_address_distance(id, cursor2):
    distance_id1 = id.split('_')[1]
    distance_id2 = id.split('_')[2]
    try:
        lon1 = float(cursor2.find_one({'_id': distance_id1}).get('longitude'))
        lat1 = float(cursor2.find_one({'_id': distance_id1}).get('latitude'))
        lon2 = float(cursor2.find_one({'_id': distance_id2}).get('longitude'))
        lat2 = float(cursor2.find_one({'_id': distance_id2}).get('latitude'))
        # 将十进制度数转化为弧度
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine公式
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # 地球平均半径，单位为公里
        # 返回结果除以1000以公里为单位保留两位小数
        distance  = round((c * r * 1000) / 1000, 2)
    except:
        distance = None
    return distance

def isShare_convert(isShareStr):  #原始数据是'true'、'false'布尔型，转换成1或0
    if isShareStr is True:
        return 1
    elif isShareStr is False:
        return 0
    else:
        return None

# @lru_cache(maxsize=1024 * 1024 * 512)
def departTime_isShare(id, cursor4):  #查询flightInfoBase距离今天最近的数据，作为起飞时间（24时制）和isShare
    List_temp = id.split('_')
    flag = 0
    for i in range(32):
        idDate = datetime.strftime(datetime.today() + timedelta(i), '%Y-%m-%d')
        List = List_temp[1:3] + [List_temp[0], idDate]
        infoBase_id = '_'.join(List)
        if cursor4.find_one({'_id': infoBase_id}):
            flag = 1
            break
    if flag == 1:
        departTime = cursor4.find_one({'_id': infoBase_id}).get('origindeparttime')
        isShareStr = cursor4.find_one({'_id': infoBase_id}).get('isshare')
        isShare = isShare_convert(isShareStr)
    else:
        departTime = None
        isShare = None
    return departTime, isShare

def processRun(idList):
    timerTotal = Timer()
    timer = Timer()
    clientMongo = MongoClient("172.29.1.172", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR', connect=False)
    cursor = clientMongo.flight.historyLowPrice_fn_domestic
    cursor1 = clientMongo.flight.fd_domestic_a
    cursor2 = clientMongo.flight.globalAirport
    cursor3 = clientMongo.flight.dynamic_seatleft
    cursor4 = clientMongo.flight.flightInfoBase
    fileName = 'featureAnalysis_orgData_domestic_' + str(os.getpid()) + '_' + todayStr1 + '.csv'
    dirName = '/data/search/predict-2019/featureAnalysis_orgData_domestic_' + todayStr1 + '.file'
    # 根据需求写出列名
    # columnNames = ','.join(
    #     ['checkDate', 'price', 'fn', 'org', 'dst', 'id', 'fc', 'departDate', 'dayofweek', 'futureMinPrice',
    #      'trend', 'orgPrice', 'discount', 'distance', 'seatLeft', 'departTime', 'isShare'])
    # if fileName in os.listdir('/data/search/predict-2019/'):
    #     os.remove('/data/search/predict-2019/' + fileName)
    if not os.path.exists(dirName):
        os.makedirs(dirName)
    f = open(dirName + '/' + fileName, 'a')
    stringIO = StringIO()
    count = 0
    # f.write(columnNames + '\n')
    for lowPrice_id in idList:
        timerTotal.start()
        timer.start()
        sample = cursor.find_one({'_id': lowPrice_id})
        del sample['_id']
        aairport = sample.get('aairport')
        del sample['aairport']
        dairport = sample.get('dairport')
        del sample['dairport']
        idList = lowPrice_id.split('_')
        idList[1] = dairport
        idList[2] = aairport
        id = '_'.join(idList)
        # 构建departDate,dayofweek,futureMinPrice,trend特征
        df = pd.DataFrame.from_dict(sample, orient='index').reset_index().rename(
            columns={'index': 'checkDate', 0: 'price'})
        for name, value in zip(['fn', 'org', 'dst', 'departMonthDay', 'id'], lowPrice_id.split('_') + [lowPrice_id]):
            df[name] = value
        df['fc'] = df.fn.str[:2]
        df['departDate'] = df.apply(confirm_departDate, axis=1)
        #Monday:1, Sunday:7
        df['dayofweek'] = pd.to_datetime(df.departDate).dt.dayofweek + 1
        df['price'] = df['price'].astype('int')
        df['startDate'] = df.departDate.apply(
            lambda x: datetime.strftime(datetime.strptime(x, '%Y-%m-%d') + timedelta(-30), '%Y-%m-%d'))
        df = df[(df.departDate >= '2018-01-01') & (df.checkDate >= df.startDate)]
        columnNamesList = df.columns.to_list()
        columnNamesList.append('futureMinPrice')
        groupDF = pd.DataFrame(columns=columnNamesList)  # 创建空的dataframe，供拼接使用
        for name, group in df.groupby('departDate'):  # 计算查询日期至起飞日期期间内的最低价格
            if len(group) >= 15:
                group = group.sort_values('checkDate')
                priceList = group.price.to_list()
                group['futureMinPrice'] = group.price.apply(roll_minPrice, args=(priceList,))
                group.dropna(axis=0, inplace=True)
                groupDF = pd.concat([groupDF, group], axis=0)
            else:
                continue
        df = groupDF.dropna(axis=0).drop(columns=['departMonthDay', 'startDate'], axis=1)
        if df.empty:  #如果数据量不足，导致dataframe为空，则直接跳至下一次循环
            continue
        logger.info('process {} groupDF costTime {} ms'.format(os.getpid(), timer.cost()))
        timer.start()
        df['trend'] = df.apply(trend, axis=1)
        logger.info('process {} trend costTime {} ms'.format(os.getpid(), timer.cost()))

        # 匹配机票的原价，计算折扣率
        timer.start()
        orgPrice = org_price(id, cursor1)
        df['orgPrice'] = orgPrice
        df['discount'] = df.apply(discount_rate, axis=1)
        logger.info('process {} discount costTime {} ms'.format(os.getpid(), timer.cost()))

        # 匹配航线距离
        timer.start()
        distance = get_address_distance(id, cursor2)
        df['distance'] = distance
        logger.info('process {} distance costTime {} ms'.format(os.getpid(), timer.cost()))

        ####匹配客座率
        timer.start()
        try:
            seatLeft_df = pd.Series(cursor3.find_one({'_id': id}).get('fc')).reset_index().rename(
                columns={'index': 'dateTime', 0: 'seatLeft'})
            seatLeft_df['seatLeft'] = seatLeft_df.seatLeft.str[:-1].astype('float')
            dateTime_df = seatLeft_df.dateTime.str.split(' ', expand=True).rename(columns={0: 'date', 1: 'time'})
            seatLeft_df = pd.concat([seatLeft_df, dateTime_df], axis=1)
            seatLeft_df = seatLeft_df.groupby('date', as_index=False).apply(lambda x: x[x.time == x.time.max()])
            df = df.merge(seatLeft_df, 'left', left_on='checkDate', right_on='date').drop(
                ['dateTime', 'date', 'time'], axis=1)
        except:
            df['seatLeft'] = None
        logger.info('process {} seatLeft costTime {} ms'.format(os.getpid(), timer.cost()))

        # 匹配起飞时间和是否共享（isshare）
        timer.start()
        departTime, isShare = departTime_isShare(id, cursor4)
        df['departTime'] = departTime
        df['isShare'] = isShare
        logger.info('process {} departTime_isShare costTime {} ms'.format(os.getpid(), timer.cost()))

        #每循环100次，将数据写入文件

        df.to_csv(stringIO, header=False, index=False, mode='a')
        count += 1
        if count % 100 == 0:
            timer.start()
            f.write(stringIO.getvalue())
            stringIO = StringIO()
            logger.info('====process {} writeStringIO costTime {} ms===='.format(os.getpid(), timer.cost()))
        logger.info('====process {} one sample timeTotal costTime {} ms===='.format(os.getpid(), timerTotal.cost()))
    f.write(stringIO.getvalue())
    f.close()

if __name__ == '__main__':
    todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
    todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
    logFileName = '/data/search/predict-2019/script/intl_pricePrediction.log'
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='[%(asctime)s] - %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    rotating_file = handlers.RotatingFileHandler(filename=logFileName,  mode='a', maxBytes=1024*1024*1024, backupCount=3)
    rotating_file.setLevel(logging.DEBUG)
    rotating_file.setFormatter(formatter)
    logger.addHandler(rotating_file)
    with open('/data/search/predict-2019/idList_temp.txt', 'r') as f_temp:
        str_L = f_temp.readline()
    L = str.split(str_L, ',')
    L.pop()
    n = ceil(len(L)/100)
    List = [L[i:i+n] for i in range(0, len(L), n)]
    p = Pool(14)
    for i in range(len(List)):
        p.apply_async(processRun, args=(List[i],))
    p.close()
    p.join()



########
#国内低价原始数据增加节假日特征
########
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorIndexer, StringIndexer, VectorAssembler, OneHotEncoder, ChiSqSelector,Bucketizer
from datetime import datetime, timedelta
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql import SparkSession
from hdfs import client
from datetime import datetime, timedelta
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,Row

# spark =SparkSession.builder.master('local[*]').appName("test")\
#                    .getOrCreate()
spark =SparkSession.builder \
                   .appName("domestic_RF_model")\
                   .getOrCreate()
sc = spark.sparkContext

schema = StructType([StructField('queryDate', StringType(), True),
                     StructField('price', StringType(), True),
                    StructField('fn', StringType(), True),
                    StructField('org', StringType(), True),
                    StructField('dst', StringType(), True),
                    StructField('id', StringType(), True),
                    StructField('fc', StringType(), True),
                    StructField('departDate', StringType(), True),
                    StructField('dayofweek', StringType(), True),
                    StructField('futureMinPrice', StringType(), True),
                    StructField('trend', StringType(), True),
                    StructField('orgPrice', StringType(), True),
                    StructField('discount', StringType(), True),
                    StructField('distance', StringType(), True),
                    StructField('seatLeft', StringType(), True),
                    StructField('departTime', StringType(), True),
                    StructField('isShare', StringType(), True)])
fileName = 'merger0822.csv'
df_org = spark.read.format('csv').load('hdfs://10.0.4.217:8020/crawler/' + fileName, schema=schema)\
              .select('fn', 'fc', 'org', 'dst', 'departDate', 'queryDate', 'price', 'orgPrice','futureMinPrice', 'dayofweek', 'distance', 'seatLeft', 'departTime','isShare', 'trend')
df_holiday = spark.read.format('csv').load('hdfs://10.0.4.217:8020/crawler/holiday_list.csv', header=True)\
                .select(concat(substring('calday',1,4), lit('-'), substring('calday',5,2), lit('-'), substring('calday',7,2)).alias('calday'),
                                     'calday_ln', 'festival', 'is_vacation', 'vacation_days','day_of_vacation')
#如果当前价格高于futureMinPrice 4%以上，视为未来降价，trend=1， 其余情况trend=0
df = df_org.dropna(how='any', subset=['orgPrice', 'distance', 'departTime', 'isShare', 'price', 'futureMinPrice'])\
    .filter(col('price').cast('double') < 100000)\
    .withColumn('trend', when((col('price')-col('futureMinPrice'))/col('price') > 0.04, 1.0).otherwise(0.0))\
    .withColumn('discountOff', round((1-col('price')/col('orgPrice')) * 100, 2))\
    .withColumn('distance', col('distance').cast('double'))\
    .na.fill({'seatLeft': '-1'})\
    .withColumn('seatLeft', col('seatLeft').cast('double'))\
    .withColumn('departTime', substring_index('departTime', ':', 1).cast('int'))\
    .withColumn('isShare', col('isShare').cast('int'))\
    .join(df_holiday, df_org.departDate==df_holiday.calday, 'left')\
    .drop('calday') \
    .withColumnRenamed('calday_ln', 'depart_calday_ln')\
    .withColumnRenamed('festival', 'depart_festival')\
    .withColumn('depart_isFestival', when(col('depart_festival').isNotNull(), 1).otherwise(0))\
    .withColumnRenamed('is_vacation', 'depart_isVacation')\
    .withColumn('depart_isVacation', when(col('depart_isVacation').isNotNull(), 1).otherwise(0))\
    .withColumnRenamed('vacation_days', 'depart_vacationDays')\
    .withColumnRenamed('day_of_vacation', 'depart_dayofvacation')\
    .na.fill({'depart_vacationDays': '0', 'depart_dayofvacation': '-1', 'depart_festival': '-1'})\
    .withColumn('depart_vacationDays', col('depart_vacationDays').cast('int'))\
    .withColumn('depart_dayofvacation', col('depart_dayofvacation').cast('int'))\
    .withColumn('departYear', year('departDate')) \
    .withColumn('departMonth', month('departDate')) \
    .withColumn('depart_dayofmonth', dayofmonth('departDate')) \
    .withColumn('depart_weekofyear', weekofyear('departDate')) \
    .withColumn('depart_dayofweek', col('dayofweek').cast('int')) \
    .drop('dayofweek')\
    .withColumn('departQuarter', quarter('departDate')) \
    .withColumn('intervalDays', datediff('departDate', 'queryDate')) \
    .withColumn('intervalMonths', round(months_between('departDate', 'queryDate'))) \
    .withColumn('intervalWeeks', round(col('intervalDays') / 7)) \
    .withColumn('intervalQuarters', round(col('intervalDays') / 120))\
    .join(df_holiday, df_org.queryDate==df_holiday.calday, 'left')\
    .drop('calday') \
    .withColumnRenamed('calday_ln', 'query_calday_ln')\
    .withColumnRenamed('festival', 'query_festival')\
    .withColumn('query_isFestival', when(col('query_festival').isNotNull(), 1).otherwise(0))\
    .withColumnRenamed('is_vacation', 'query_isVacation')\
    .withColumn('query_isVacation', when(col('query_isVacation').isNotNull(), 1).otherwise(0))\
    .withColumnRenamed('vacation_days', 'query_vacationDays')\
    .withColumnRenamed('day_of_vacation', 'query_dayofvacation')\
    .na.fill({'query_vacationDays': '0', 'query_dayofvacation': '-1', 'query_festival': '-1'})\
    .withColumn('query_vacationDays', col('query_vacationDays').cast('int'))\
    .withColumn('query_dayofvacation', col('query_dayofvacation').cast('int'))\
    .withColumn('queryYear', year('queryDate')) \
    .withColumn('queryMonth', month('queryDate')) \
    .withColumn('query_dayofmonth', dayofmonth('queryDate')) \
    .withColumn('query_weekofyear', weekofyear('queryDate')) \
    .withColumn('query_dayofweek', datediff('queryDate', to_date(lit('2017-01-09')))%7 + 1)\
    .withColumn('queryQuarter', quarter('queryDate'))


stringIndexerCols = ['fn', 'fc', 'org', 'dst', 'depart_festival', 'query_festival']
bucketizerCols = ['discountOff', 'distance', 'seatLeft']
discountOffSplits = [-float('inf')] + list(range(0, 101,1))
distanceSplits = list(range(0, 6001, 300)) + [float('inf')]
seatLeftSplits = [-float('inf')] + list(range(0, 105, 1))
splits = [discountOffSplits, distanceSplits, seatLeftSplits]

stages = []
for colName in stringIndexerCols:
    stringIndexer = StringIndexer(inputCol=colName, outputCol='stringIndexer_%s' %colName)
    stages.append(stringIndexer)
for split, colName in zip(splits, bucketizerCols):
    bucketizer = Bucketizer(splits=split, inputCol=colName, outputCol='%sBucket' %colName)
    stages.append(bucketizer)
pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(df)
df = pipelineModel.transform(df)
for colName in stringIndexerCols:
    df = df.withColumn('stringIndexer_%s' %colName, col('stringIndexer_%s' %colName).cast('double'))
df_shuffled = df.orderBy(rand())
df_train_DNN = df.filter(df.departDate < '2019-07-25').orderBy(rand())
df_val_DNN = df.filter(df.departDate >= '2019-07-25')
df_train_DNN_sample = df_train_DNN.sample(False, 0.0002)
df_val_DNN_sample = df_val_DNN.sample(False, 0.003)
todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
featureData_fileName = 'domestic_shuffled_featureData_' + todayStr1 +'.csv'
DNN_trainData_fileName = 'deomestic_DNN_trainData_' + todayStr1 + '.csv'
DNN_valData_fileName = 'deomestic_DNN_valData_' + todayStr1 + '.csv'
DNN_trainSampleData_fileName = 'deomestic_trainSampleData_' + todayStr1 + '.csv'
DNN_valSampleData_fileName = 'deomestic_valSampleData_' + todayStr1 + '.csv'
clientHdfs = client.InsecureClient('http://10.0.4.217:9870', user="search") #跟HDFS建立连接
fileNames = [featureData_fileName, DNN_trainData_fileName, DNN_valData_fileName, DNN_trainSampleData_fileName, DNN_valSampleData_fileName]
df_subs = [df_shuffled, df_train_DNN, df_val_DNN, df_train_DNN_sample, df_val_DNN_sample]
for fileName, df_sub in zip(fileNames, df_subs):
    if fileName in clientHdfs.list('/crawler/'):
        clientHdfs.delete("/crawler/" + fileName, recursive=True)
    df_sub.write.format('csv').save("hdfs://10.0.4.217:8020/crawler/" + fileName)







dropFeatures = ['orgPrice', 'org', 'query_calday_ln', 'fn', 'query_festival', 'dst', 'depart_calday_ln', 'futureMinPrice', 'fc', 'queryDate', 'price', 'depart_festival', 'departDate', 'trend']
assembleFeatures = list(set(df.columns) - set(dropFeatures))
assembler = VectorAssembler(inputCols=assembleFeatures, outputCol='features')
df_total = assembler.transform(df).select('fn', 'org', 'dst', 'departDate', 'queryDate', 'features', 'trend')
df_train = df_total.filter(df_total.departDate < '2019-07-25')
df_val = df_total.filter(df_total.departDate >= '2019-07-25')
df_train.cache()
df_val.cache()
predict_model = RandomForestClassifier(labelCol='trend', numTrees=50, featureSubsetStrategy='auto', subsamplingRate=0.8)
RF_model = predict_model.fit(df_train)
df_prediction = RF_model.transform(df_val)

todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
predict_fileName = 'domestic_RF_model_predictResult_' + todayStr1 + '.parquet'
clientHdfs = client.InsecureClient('http://10.0.4.217:9870', user="search") #跟HDFS建立连接
if predict_fileName in clientHdfs.list('/crawler/'):
    clientHdfs.delete("/crawler/" + predict_fileName, recursive=True)
df_prediction.write.format('parquet').save('hdfs://10.0.4.217:8020/crawler/' + predict_fileName)
sc.stop()


TP = df_prediction.filter(col('trend')==1).filter(col('prediction')==1).count()
FP = df_prediction.filter(col('trend') != 1).filter(col('prediction')==1).count()
FN = df_prediction.filter(col('trend') == 1).filter(col('prediction')!=1).count()
P = TP/(TP+FP)
R = TP/(TP+FN)
accuracy = df_prediction.filter(col('trend') == col('prediction')).count()/df_prediction.count()


20190830 RF_model 特征重要度
[('intervalDays', 0.31234249),
 ('intervalWeeks', 0.267170309),
 ('intervalMonths', 0.175820382),
 ('isShare', 0.124279829),
 ('discountOff', 0.036162603),
 ('seatLeft', 0.0163577307),
 ('stringIndexer_fn', 0.012743711),
 ('query_weekofyear', 0.0117681576),
 ('departMonth', 0.0102766346),
 ('departQuarter', 0.00610656848),
 ('queryMonth', 0.00605134208),
 ('stringIndexer_fc', 0.00372439833),
 ('stringIndexer_dst', 0.00371136772),
 ('stringIndexer_org', 0.00362113485),
 ('depart_weekofyear', 0.00291458252),
 ('departYear', 0.00244127291),
 ('queryQuarter', 0.00181939762),
 ('query_dayofmonth', 0.000913186575),
 ('distance', 0.000739179751),
 ('queryYear', 0.000554840003),
 ('depart_dayofmonth', 0.000434970784),
 ('stringIndexer_depart_festival', 3.9847674e-05),
 ('query_dayofweek', 2.20540103e-06),
 ('depart_vacationDays', 2.16096762e-06),
 ('query_isFestival', 1.69805813e-06),
 ('departTime', 0.0),
 ('depart_isVacation', 0.0),
 ('depart_dayofvacation', 0.0),
 ('depart_isFestival', 0.0),
 ('depart_dayofweek', 0.0),
 ('intervalQuarters', 0.0),
 ('query_isVacation', 0.0),
 ('query_vacationDays', 0.0),
 ('query_dayofvacation', 0.0),
 ('stringIndexer_query_festival', 0.0)]
###########

###########
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.ml.feature import VectorIndexer, StringIndexer, VectorAssembler, OneHotEncoder, ChiSqSelector
# from datetime import datetime, timedelta
# from pyspark.ml import Pipeline
# from pyspark.ml.classification import RandomForestClassifier, OneVsRest
# from pyspark.sql import SparkSession
# from hdfs import client
# from datetime import datetime, timedelta
# from pyspark import SparkConf,SparkContext
# from pyspark.sql import SparkSession,Row
#
# # spark =SparkSession.builder.master('local[*]').appName("test")\
# #                    .getOrCreate()
# spark =SparkSession.builder \
#                    .appName("domestic_RF_model")\
#                    .getOrCreate()
# sc = spark.sparkContext
# featureData_fileName = 'domestic_featureData_' + '.parquet'
# df = spark.read.format('parquet').load('hdfs://10.0.4.217:8020/crawler/' + featureData_fileName)
# stringIndexerCols = ['fn', 'fc', 'org', 'dst', 'depart_festival', 'query_festival']
# for colName in stringIndexerCols:
#     df = df.withColumn('stringIndexer_%s' %colName, col('stringIndexer_%s' %colName).cast('double'))
# df.withColumn('trend', when(col('trend')==))
#
# assembleFeatures = ['distance', 'seatLeft', 'departTime', 'isShare', 'discountOff','depart_isVacation', 'depart_vacationDays',
#      'depart_dayofvacation', 'depart_isFestival', 'departYear','departMonth', 'depart_dayofmonth', 'depart_weekofyear',
#      'depart_dayofweek', 'departQuarter', 'intervalDays', 'intervalMonths', 'intervalWeeks', 'intervalQuarters',
#      'query_isVacation', 'query_vacationDays', 'query_dayofvacation', 'query_isFestival', 'queryYear', 'queryMonth',
#      'query_dayofmonth', 'query_weekofyear', 'query_dayofweek', 'queryQuarter', 'stringIndexer_fn', 'stringIndexer_fc',
#      'stringIndexer_org', 'stringIndexer_dst', 'stringIndexer_depart_festival', 'stringIndexer_query_festival']
# assembler = VectorAssembler(inputCols=assembleFeatures, outputCol='features')
# df_total = assembler.transform(df).select('fn', 'org', 'dst', 'departDate', 'queryDate', 'features', 'trend')
# df_train = df_total.filter(df_total.departDate < '2019-07-30')
# df_val = df_total.filter(df_total.departDate >= '2019-07-30')
# df_train.cache()
# df_val.cache()
# RF_model = RandomForestClassifier(labelCol='trend', numTrees=50, featureSubsetStrategy='auto', subsamplingRate=0.8)
# # predict_model = OneVsRest(featuresCol='features', labelCol='trend', classifier=RF_model, parallelism=2)
# predict_model = RF_model.fit(df_train)
# df_prediction = predict_model.transform(df_val)
# todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
# todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
# predict_fileName = 'domestic_RF_model_predictResult_' + todayStr1 + '.parquet'
# # clientHdfs = client.InsecureClient("hdfs://10.0.4.217:8020", user="search") #跟HDFS建立连接
# # if predict_fileName in clientHdfs.list('/crawler/'):
# #     clientHdfs.delete("/crawler/" + predict_fileName, recursive=True)
# df_predict.write.format('parquet').save('hdfs://10.0.4.217:8020/crawler/' + predict_fileName)





predict_fileName = 'domestic_RF_model_predictResult_' + '20190830' + '.parquet'
df_prediction = spark.read.format('parquet').load('hdfs://10.0.4.217:8020/crawler/')


TP = df_prediction.filter(col('trend')==1).filter(col('prediction')==1).count()
FP = df_prediction.filter(col('trend') != 1).filter(col('prediction')==1).count()
FN = df_prediction.filter(col('trend') == 1).filter(col('prediction')!=1).count()
P = TP/(TP+FP)
R = TP/(TP+FN)
accuracy = df_prediction.filter(col('trend') == col('prediction')).count()/df_prediction.count()



TP = df_prediction.filter(col('trend')==0.0).filter(col('prediction')==0.0).count()
FP = df_prediction.filter(col('trend') != 0.0).filter(col('prediction')==0.0).count()
FN = df_prediction.filter(col('trend') == 0.0).filter(col('prediction')!=0.0).count()
P = TP/(TP+FP)
R = TP/(TP+FN)




==================
#spark 进行卡方检验
==================

from pyspark.ml.feature import StringIndexer, VectorAssembler, Bucketizer
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorIndexer,StringIndexer,VectorAssembler,OneHotEncoder,ChiSqSelector
from datetime import datetime, timedelta
from pyspark.ml import Pipeline
from hdfs import *
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql import SparkSession
# ['checkDate', 'price', 'fn', 'org', 'dst', 'id', 'fc', 'departDate', 'dayofweek', 'futureMinPrice',
#                         'trend', 'orgPrice', 'discount', 'distance', 'seatLeft', 'departTime', 'isShare']
spark =SparkSession.builder \
                   .appName("select_features")\
                   .getOrCreate()
sc = spark.sparkContext
# schema = StructType([StructField('checkDate', StringType(), True),
#                      StructField('price', IntegerType(), True),
#                     StructField('fn', StringType(), True),
#                     StructField('org', StringType(), True),
#                     StructField('dst', StringType(), True),
#                     StructField('id', StringType(), True),
#                     StructField('fc', StringType(), True),
#                     StructField('departDate', StringType(), True),
#                     StructField('dayofweek', IntegerType(), True),
#                     StructField('futureMinPrice', DoubleType(), True),
#                     StructField('trend', IntegerType(), True),
#                     StructField('orgPrice', StringType(), True),
#                     StructField('discount', StringType(), True),
#                     StructField('distance', DoubleType(), True),
#                     StructField('seatLeft', DoubleType(), True),
#                     StructField('departTime', StringType(), True),
#                     StructField('isShare', IntegerType(), True)])

schema = StructType([StructField('checkDate', StringType(), True),
                     StructField('price', StringType(), True),
                    StructField('fn', StringType(), True),
                    StructField('org', StringType(), True),
                    StructField('dst', StringType(), True),
                    StructField('id', StringType(), True),
                    StructField('fc', StringType(), True),
                    StructField('departDate', StringType(), True),
                    StructField('dayofweek', StringType(), True),
                    StructField('futureMinPrice', StringType(), True),
                    StructField('trend', StringType(), True),
                    StructField('orgPrice', StringType(), True),
                    StructField('discount', StringType(), True),
                    StructField('distance', StringType(), True),
                    StructField('seatLeft', StringType(), True),
                    StructField('departTime', StringType(), True),
                    StructField('isShare', StringType(), True)])
fileName = 'merger0822.csv'
# df_org = spark.read.format('csv').load('hdfs://10.0.1.218:9000/predict-2019/' + fileName, schema=schema)\
#               .select('fn', 'fc', 'departDate', 'checkDate', 'price', 'orgPrice', 'dayofweek', 'distance', 'seatLeft', 'departTime','isShare', 'trend')

#新版spark读取
df_org = spark.read.format('csv').load('hdfs://10.0.4.217:8020/crawler/' + fileName, schema=schema)\
              .select('fn', 'fc', 'departDate', 'checkDate', 'price', 'orgPrice', 'dayofweek', 'distance', 'seatLeft', 'departTime','isShare', 'trend')
#生成原始discount有误，使用spark重新计算


#historyLowPrice_fn_domestic表中存在价格为0的情况，导致trend字段存在空值，例如_id='MU5416_CTU_SHA_06-15',在2019-05-17，price=0
df = df_org.dropna(how='any', subset=['orgPrice', 'distance', 'departTime','isShare', 'trend'])\
    .withColumn('departTimeBucket', substring_index(col('departTime'), ':', 1).cast('int'))\
    .withColumn('fnHashBucket', hash('fn') % 60)\
    .withColumn('fcHashBucket', hash('fc') % 60)\
    .withColumn('intervalDays', datediff('departDate', 'checkDate').cast('double'))\
    .withColumn('discount', round(col('price')/col('orgPrice') * 100, 1))\
    .withColumn('distance', col('distance').cast('double'))\
    .withColumn('seatLeft', col('seatLeft').cast('double'))\
    .withColumn('dayofweek', col('dayofweek').cast('int'))\
    .withColumn('isShare', col('isShare').cast('int'))\
    .withColumn('trend', col('trend').cast('int'))\
    .fillna({'seatLeft': 0.0})\
    .filter(col('discount') < 150)

# stringIndexer = StringIndexer(inputCol='fn', outputCol='fnIndex')  #fn使用hash分桶，hash有负值
discountSplits = list(range(0, 151, 5))
discountBucketizer = Bucketizer(splits=discountSplits, inputCol='discount', outputCol='discountBucket')
distanceSplits = list(range(0, 10001, 500)) + [float('inf')]
distanceBucketizer = Bucketizer(splits=distanceSplits, inputCol='distance', outputCol='distanceBucket')
seatLeftSplits = list(range(0, 101, 5))
seatLeftBucketizer = Bucketizer(splits=seatLeftSplits, inputCol='seatLeft', outputCol='seatLeftBucket')
intervalDaysSplits = [0, 7, 14, 21, 28, 35]
intervalDaysBucketizer = Bucketizer(splits=intervalDaysSplits, inputCol='intervalDays', outputCol='intervalDaysBucket')
pipeline = Pipeline(stages=[discountBucketizer, distanceBucketizer, seatLeftBucketizer, intervalDaysBucketizer])
pipelineModel = pipeline.fit(df)
featuresName = ['fnHashBucket', 'fcHashBucket', 'dayofweek', 'discountBucket', 'distanceBucket', 'seatLeftBucket', 'departTimeBucket','isShare', 'intervalDays', 'intervalDaysBucket']
dfData_temp = pipelineModel.transform(df).select(featuresName + ['trend'])

assembler = VectorAssembler(inputCols=featuresName, outputCol='features')
dfData = assembler.transform(dfData_temp).select('features', 'trend')

selector = ChiSqSelector(percentile=1, selectorType="percentile", featuresCol='features', labelCol='trend')
selectorModel = selector.fit(dfData)
featuresSequence = selectorModel.selectedFeatures
L = []
for index in featuresSequence:
    t = featuresName[index]
    L.append(t)
print('====Full Data Result Features Sequence is {}===='.format(L))

sc.stop()
























#########
#使用模型预测
########
featuresName = ['fnHashBucket', 'fcHashBucket', 'dayofweek', 'discountBucket', 'distanceBucket', 'seatLeftBucket', 'departTimeBucket','isShare', 'intervalDays', 'intervalDaysBucket']
dfData_temp = pipelineModel.transform(df).select(featuresName + ['checkDate', 'trend'])
df_train_temp = dfData_temp.filter(col('checkDate')<'2019-07-25').drop('checkDate')
df_val_temp = dfData_temp.filter(col('checkDate')>='2019-07-25').drop('checkDate')
assembler = VectorAssembler(inputCols=featuresName, outputCol='features')
df_train = assembler.transform(df_train_temp).select('features', 'trend')
df_val = assembler.transform(df_val_temp).select('features', 'trend')
rf = RandomForestClassifier(numTrees=100, maxDepth=6, labelCol="trend", seed=42)
rfModel = rf.fit(df_train)
dfResult = rfModel.transform(df_val)
ratio = dfResult.filter(col('trend')==col('prediction')).count()/dfResult.count()
print(ratio)