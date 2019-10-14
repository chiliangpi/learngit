# coding:utf-8
# -*- coding:utf-8 -*-
################
#基于historyLowPrice_flightLine表，生成计算的源数据
################
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


#################
"""
从MongoDB取出国际航线平均价格数据
"""
#################
import re
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
from hdfs import *

clientMongo = MongoClient("172.29.1.172:27017", authSource="flight",
                     username='search', password='search@huoli123', authMechanism='MONGODB-CR')
cursor = clientMongo.flight.historyAvgPrice_flightLine.find({})
todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
fileName = "intl_priceRange_statistic_" + todayStr1 + "_org_avg_data.csv"
if fileName in os.listdir('/data/search/predict-2019/'):
    os.remove('/data/search/predict-2019/' + fileName)
f = open('/data/search/predict-2019/' + fileName, 'a')
f.write('flineId,departDate,avgPrice\n')
nowStr = datetime.now().strftime('%H:%M:%S')
print(nowStr + "=====Start write Mongo intl_historyAvgPrice data to Local=====")
for sample in cursor:
    for key in list(sample.keys()):
        if key == '_id':
            flineId = sample[key]
            del sample[key]
        elif not re.match(r'\d+\-\d+', key):
            del sample[key]
        else:
            continue
    for k, v in sample.items():
        f.write(str(flineId) + ',' + str(k) + ',' + str(v) + '\n')
f.close()
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish write Mongo intl_historyAvgPrice data to Local=====")
fileName = "intl_priceRange_statistic_" + before2_dateStr1 + "_org_avg_data.csv"
if fileName in os.listdir('/data/search/predict-2019/'):
    os.remove('/data/search/predict-2019/' + fileName)  #删掉服务器上两天前的数据（只保留两天的数据）
clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search")
fileName = "intl_priceRange_statistic_" + todayStr1 + "_org_avg_data.csv"
if fileName in clientHdfs.list('/predict-2019/'):
    clientHdfs.delete("/predict-2019/" + fileName, recursive=True)
clientHdfs.upload('/predict-2019/' + fileName, '/data/search/predict-2019/' + fileName)
fileName = "intl_priceRange_statistic_" + before2_dateStr1 + "_org_avg_data.csv"
if fileName in clientHdfs.list('/predict-2019/'):
    clientHdfs.delete("/predict-2019/" + fileName, recursive=True)
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish put local intl_historyAvgPrice data to HDFS=====")


###################################################################
"""
基于平均价格的方式，统计价格区间
"""
###################################################################
from pyspark.sql import SparkSession,Row,Window
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.regression import RandomForestRegressor,LinearRegression
from pyspark.ml.feature import VectorIndexer,StringIndexer,VectorAssembler,OneHotEncoder,StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime, timedelta
from pyspark.ml import Pipeline, PipelineModel
from hdfs import *
import os
import shutil

mongodb_url = "mongodb://search:search%40huoli123@10.0.1.212/flight.intl_priceRange_statistic"


spark =SparkSession.builder \
                   .config("spark.mongodb.input.uri", mongodb_url) \
                   .config("spark.mongodb.output.uri", mongodb_url) \
                   .appName("intl_priceRange_statistic")\
                   .getOrCreate()
sc = spark.sparkContext

def spark_readData():
    df = spark.read.format('csv').load('hdfs://10.0.1.218:9000/predict-2019/' + fileName, header=True)
    return df
def priceRange_statistic(df):
    df_priceRange = df.filter((length('departDate') == 5) & (col('avgPrice') > 0)) \
                      .withColumn('flineId', concat('flineId', lit('_'), substring_index('departDate', '-', 1)))\
                      .groupBy('flineId')\
                      .agg(min(col('avgPrice').cast('int')).alias('min_avgPrice'), max(col('avgPrice').cast('int')).alias('max_avgPrice'))\
                      .withColumn('priceRange_baseAvg', array('min_avgPrice', 'max_avgPrice'))\
                      .withColumnRenamed('flineId', '_id')\
                      .select('_id', 'priceRange_baseAvg')
    return df_priceRange


if __name__ == "__main__":
    todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
    fileName = "intl_priceRange_statistic_" + todayStr1 + "_org_avg_data.csv"
    before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
    clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search")
    df = spark_readData()
    df_priceRange = priceRange_statistic(df)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ': =======df_priceRange is OK!=========')
    sparkDirName = 'intl_priceRange_statistic_baseAvg_' + todayStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    df_priceRange.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
    sparkDirName = 'intl_priceRange_statistic_baseAvg_' + before2_dateStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + "=====df_priceRange toSpark is OK!=====")
    sparkDirName = 'intl_priceRange_statistic_baseAvg_' + todayStr1 + '.parquet'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    clientHdfs.download('/predict-2019/' + sparkDirName, '/data/search/predict-2019/' + sparkDirName)
    sparkDirName = 'intl_priceRange_statistic_baseAvg_' + before2_dateStr1 + '.parquet'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ": ========df_priceRange toLocal is OK!=============")
    sc.stop()



##################################################################################################
"""
基于百分比的方式，统计价格区间
"""
##################################################################################################
import pandas as pd
import numpy as np
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,Row,Window
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.regression import RandomForestRegressor,LinearRegression
from pyspark.ml.feature import VectorIndexer,StringIndexer,VectorAssembler,OneHotEncoder,StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime, timedelta
from pyspark.ml import Pipeline, PipelineModel
from hdfs import *
import os
import shutil

mongodb_url = "mongodb://search:search%40huoli123@10.0.1.212/flight.sparkTest"
spark =SparkSession.builder \
                   .config("spark.mongodb.input.uri", mongodb_url) \
                   .config("spark.mongodb.output.uri", mongodb_url) \
                   .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.1.1") \
                   .appName("pricePredict")\
                   .getOrCreate()
sc = spark.sparkContext

def spark_readData():
       fileName = "intlFline_pricePredict_org_data_" + todayStr1 + ".csv"
       df_org = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/'+fileName, header=True)\
                     .filter(col('price') > 0)
       df_holidayList = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/holiday_list.csv', header=True)\
                             .select(concat(substring('calday', 1, 4), lit('-'), substring('calday', 5, 2), lit('-'), substring('calday', 7, 2)).alias('dateList'),\
                                     'is_vacation', 'vacation_days', 'day_of_vacation')
       df_holidayTOdate = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/holiday_to_date.csv', header=True)
       return df_org, df_holidayList, df_holidayTOdate


def org_features(df_org, df_holidayList, df_holidayTOdate):
    """
    起飞日期判断逻辑：如果起飞日期的departDate_monthDay_tmp大于predictDate_monthDay_tmp，起飞的年份就是原始数据中predictDate的年份，\
                   如果起飞日期的departDate_monthDay_tmp小于predictDate_monthDay_tmp，起飞的年份就是原始数据中predictDate的年份+1
    """
    """
    原始数据flineId中含有'2018-Spring-8'样式，利用df_holidayTOdate将其转化成具体的日期
    """
    df_orgFeatures = df_org.withColumn('departDate_monthDay_tmp', substring_index(df_org.flineId, '_', -1)) \
        .withColumn('predictDate_year_tmp', substring_index(df_org.predictDate, '-', 1)) \
        .withColumn('predictDate_monthDay_tmp', substring_index(df_org.predictDate, '-', -2)) \
        .withColumn('departDate_tmp', when(col('departDate_monthDay_tmp') > col('predictDate_monthDay_tmp'),
                                           concat(col('predictDate_year_tmp'), lit('-'),
                                                  col('departDate_monthDay_tmp')))
                    .otherwise(concat(substring_index((col('predictDate_year_tmp') + 1).cast('string'), '.', 1),
                                      lit('-'), col('departDate_monthDay_tmp')))) \
        .drop('departDate_monthDay_tmp', 'predictDate_year_tmp', 'predictDate_monthDay_tmp') \
        .join(df_holidayTOdate, col('departDate_tmp') == col('dateDesc'), 'left') \
        .withColumn('departDate', when(df_holidayTOdate.date.isNull(), col('departDate_tmp'))
                    .otherwise(df_holidayTOdate.date)) \
        .drop('dateDesc', 'departDate_tmp', 'date') \
        .join(df_holidayList, col('departDate') == col('dateList'), 'left') \
        .drop('dateList') \
        .na.fill({'is_vacation': 'no', 'vacation_days': 0, 'day_of_vacation': 0}) \
        .withColumn('flineId_noDate', substring_index(df_org.flineId, '_', 5)) \
        .withColumn('org', split(df_org.flineId, '_').getItem(0)) \
        .withColumn('dst', split(df_org.flineId, '_').getItem(1)) \
        .withColumn('isReturn_type', split(df_org.flineId, '_').getItem(2)) \
        .withColumn('cabin', split(df_org.flineId, '_').getItem(3)) \
        .withColumn('isDirect_type', split(df_org.flineId, '_').getItem(4)) \
        .withColumn('departDate', to_date('departDate')) \
        .withColumn('predictDate', to_date('predictDate')) \
        .withColumn('price', col('price').cast('int')) \
        .filter("cabin == 'Y'") \
        .drop('flineId', 'cabin') \
        .dropna(how='any')
    return df_orgFeatures

def priceRange_statistic(df_orgFeatures):
    df_filter = df_orgFeatures.select('flineId_noDate', 'departDate', 'predictDate', 'price')\
                       .withColumn('startDate', date_sub('departDate', 180))\
                       .filter((col('predictDate') > col('startDate')) & (col('predictDate') <= col('departDate')))\
                       .withColumn('flineId', concat('flineId_noDate', lit('_'),
                                                     split(col('departDate').cast('string'), '-')[1]))\
                       .withColumn('flineId', concat(substring_index('flineId', '_', 3), lit('_'),
                                                     substring_index('flineId', '_', -2)))\
                       .select('flineId', 'price')
    df_filter.createOrReplaceTempView('df_filter')
    df_priceRange = spark.sql("select flineId, percentile_approx(price,array(0.05,0.1,0.15,0.2,0.25,0.75,0.8,0.85,0.9,0.95)) as percentArray\
                                   from df_filter group by flineId")
    str = "5%, 10%, 15%, 20%, 25%, 75%, 80%, 85%, 90%, 95%"

    def array_zip(array1, array2):
        return [x for x in zip(array1, array2)]

    zip_udf = udf(array_zip, ArrayType(ArrayType(StringType())))
    df_priceRange = df_priceRange.select(col('flineId').alias('_id'),
                                         zip_udf(split(lit(str), ','), col('percentArray')).alias('priceRange_basePercentile'))
    return df_priceRange


if __name__ == "__main__":
    todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
    before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
    clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search") #跟HDFS建立连接

    df_org, df_holidayList, df_holidayTOdate = spark_readData()
    df_orgFeatures = org_features(df_org, df_holidayList, df_holidayTOdate)
    df_priceRange = priceRange_statistic(df_orgFeatures)
    sparkDirName = 'intl_priceRange_statistic_basePercentile_' + todayStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    df_priceRange.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
    sparkDirName = 'intl_priceRange_statistic_basePercentile_' + before2_dateStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ": ==============df_priceRange toSpark is OK!============")
    sparkDirName = 'intl_priceRange_statistic_basePercentile_' + todayStr1 + '.parquet'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    clientHdfs.download('/predict-2019/' + sparkDirName, '/data/search/predict-2019/' + sparkDirName)
    sparkDirName = 'intl_priceRange_statistic_basePercentile_' + before2_dateStr1 + '.parquet'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + "========df_priceRange toLocal is OK!=============")
    sc.stop()


##################################################################################################
"""
均价和百分比两种方式统计的结果进行合并，写入mongoDB中
"""
##################################################################################################
import pandas as pd
import json
from pymongo import *
from datetime import datetime, timedelta
todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
avg_FileName = 'intl_priceRange_statistic_baseAvg_' + todayStr1 + '.parquet'
percent_FileName = 'intl_priceRange_statistic_basePercentile_' + todayStr1 + '.parquet'
df_avg = pd.read_parquet('/data/search/predict-2019/' + avg_FileName)
df_percentile = pd.read_parquet('/data/search/predict-2019/' + percent_FileName)
df = df_percentile.merge(df_avg, on='_id', how='outer')
clientMongo = MongoClient("172.29.1.172:27017", authSource="flight",
                          username='search', password='search@huoli123', authMechanism='MONGODB-CR')
cursor = clientMongo.flight.priceRange_statistic_intl
data = json.loads(df.T.to_json()).values()
# if "priceRange_statistic_intl" in clientMongo.flight.collection_names():
#     cursor.delete_many({})
for sample in data:
    cursor.update({'_id': sample['_id']}, sample, upsert=True)
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish avg_and_percentdata priceRange data insert MongoDB=====")



# import pandas as pd
# import json
# from pymongo import *
# from datetime import datetime, timedelta
# todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
# before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
# avg_FileName = 'intl_priceRange_statistic_baseAvg' + '.parquet'
# percent_FileName = 'intl_priceRange_statistic_basePercentile' + '.parquet'
# df_avg = pd.read_parquet('/data/search/predict-2019/' + avg_FileName)
# df_percentile = pd.read_parquet('/data/search/predict-2019/' + percent_FileName)
# df = df_percentile.merge(df_avg, on='_id', how='outer')
# clientMongo = MongoClient("172.29.1.172:27017", authSource="flight",
#                           username='search', password='search@huoli123', authMechanism='MONGODB-CR')
# cursor = clientMongo.flight.priceRange_statistic_intl
# data = json.loads(df.T.to_json()).values()
# # if "priceRange_statistic_intl" in clientMongo.flight.collection_names():
# #     cursor.delete_many({})
# for sample in data:
#     cursor.update({'_id': sample['_id']}, sample, upsert=True)
# nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# print(nowStr + "=====Finish avg_and_percentdata priceRange data insert MongoDB=====")




