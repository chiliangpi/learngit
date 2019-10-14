# coding:utf-8
# -*- coding:utf-8 -*-
################################################################
"""
从MongoDB取出国内航班价格数据
"""
################################################################
from hdfs import *
import re
from pymongo import MongoClient
from datetime import datetime, timedelta
import os

clientMongo = MongoClient("172.29.1.172:27017", authSource="flight",
                     username='search', password='search@huoli123', authMechanism='MONGODB-CR')
cursor = clientMongo.flight.historyLowPrice_fn_domestic.find({})
todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
fileName = "domestic_priceRange_statistic_" + todayStr1 + "_org_data.csv"
if fileName in os.listdir('/data/search/predict-2019/'):
    os.remove('/data/search/predict-2019/' + fileName)
f = open('/data/search/predict-2019/' + fileName, 'a')
f.write('flineId,predictDate,Price\n')
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Start write Mongo domestic_historyPrice data to Local=====")
for sample in cursor:
    for key in list(sample.keys()):
        if key == '_id':
            flineId = sample[key]
            del sample[key]
        elif not re.match(r'\d{4}\-\d{2}\-\d{2}', key):
            del sample[key]
        else:
            continue
    for k, v in sample.items():
        f.write(str(flineId) + ',' + str(k) + ',' + str(v) + '\n')
f.close()
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish write Mongo domestic_historyPrice data to Local=====")
fileName = "domestic_priceRange_statistic_" + before2_dateStr1 + "_org_data.csv"
if fileName in os.listdir('/data/search/predict-2019/'):
    os.remove('/data/search/predict-2019/' + fileName)
clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search")
fileName = "domestic_priceRange_statistic_" + todayStr1 + "_org_data.csv"
if fileName in clientHdfs.list('/predict-2019/'):
    clientHdfs.delete("/predict-2019/" + fileName, recursive=True)
clientHdfs.upload('/predict-2019/' + fileName, '/data/search/predict-2019/' + fileName)
fileName = "domestic_priceRange_statistic_" + before2_dateStr1 + "_org_data.csv"
if fileName in clientHdfs.list('/predict-2019/'):
    clientHdfs.delete("/predict-2019/" + fileName, recursive=True)
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish put local domestic_historyPrice data to HDFS=====")


################################################################
"""
基于平均价格方式，统计价格区间
"""
################################################################
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

mongodb_url = "mongodb://search:search%40huoli123@10.0.1.212/flight.domestic_priceRange_statistic_baseAvg"
todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
fileName = "domestic_priceRange_statistic_" + todayStr1 + "_org_data.csv"
spark =SparkSession.builder \
                   .config("spark.mongodb.input.uri", mongodb_url) \
                   .config("spark.mongodb.output.uri", mongodb_url) \
                   .appName("domestic_priceRange_statistic")\
                   .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.1.1") \
                   .getOrCreate()
sc = spark.sparkContext

def spark_readData():
    df = spark.read.format('csv').load('hdfs://10.0.1.218:9000/predict-2019/' + fileName, header=True)
    return df

def priceRange_statistic(df):
    """
    df_avgPrice计算逻辑：取起飞日期前50天的价格计算均值，如果起飞前50天价格记录数目小于15，则去掉该flineId数据
    :param df:
    :return:
    """
    df_filter = df.withColumn('departDate_monthDay_tmp', substring_index(df.flineId, '_', -1)) \
        .withColumn('predictDate_year_tmp', substring_index(df.predictDate, '-', 1)) \
        .withColumn('predictDate_monthDay_tmp', substring_index(df.predictDate, '-', -2)) \
        .withColumn('departDate', when(col('departDate_monthDay_tmp') > col('predictDate_monthDay_tmp'),
                                       concat(col('predictDate_year_tmp'), lit('-'),
                                              col('departDate_monthDay_tmp')))
                    .otherwise(concat(substring_index((col('predictDate_year_tmp') + 1).cast('string'), '.', 1),
                                      lit('-'), col('departDate_monthDay_tmp')))) \
        .drop('departDate_monthDay_tmp', 'predictDate_year_tmp', 'predictDate_monthDay_tmp') \
        .withColumn('startDate', date_sub('departDate', 50)) \
        .filter((col('predictDate') > col('startDate')) & (col('predictDate') <= col('departDate'))) \
        .groupBy('flineId') \
        .agg(round(avg('price')).alias('avgPrice'), count('flineId').alias('count')) \
        .filter(col('count') >= lit(15)) \
        .select('flineId', 'avgPrice')
    df_rangePrice = df_filter.withColumn('flineId', substring_index('flineId', '-', 1))\
                             .groupBy('flineId')\
                             .agg(max('avgPrice').alias('max_avgPrice'), min('avgPrice').alias('min_avgPrice')) \
                             .withColumn('priceRange_baseAvg', array('min_avgPrice', 'max_avgPrice')) \
                             .withColumnRenamed('flineId', '_id') \
                             .select('_id', 'priceRange_baseAvg')
    return df_rangePrice


if __name__ == "__main__":
    clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search")
    df = spark_readData()
    df_priceRange = priceRange_statistic(df)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ': =======df_priceRange is OK!=========')
    sparkDirName = 'domestic_priceRange_statistic_baseAvg_' + todayStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    df_priceRange.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
    sparkDirName = 'domestic_priceRange_statistic_baseAvg_' + before2_dateStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + "=====df_priceRange toSpark is OK!=====")
    sparkDirName = 'domestic_priceRange_statistic_baseAvg_' + todayStr1 + '.parquet'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    clientHdfs.download('/predict-2019/' + sparkDirName, '/data/search/predict-2019/' + sparkDirName)
    sparkDirName = 'domestic_priceRange_statistic_baseAvg_' + before2_dateStr1 + '.parquet'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ":========df_priceRange toLocal is OK!=============")
    sc.stop()


###############################
"""
基于百分比的方式，统计价格区间
"""
###############################
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

mongodb_url = "mongodb://search:search%40huoli123@10.0.1.212/flight.domestic_priceRange_statistic_basePercentile"
todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
fileName = "domestic_priceRange_statistic_" + todayStr1 + "_org_data.csv"
spark =SparkSession.builder \
                   .config("spark.mongodb.input.uri", mongodb_url) \
                   .config("spark.mongodb.output.uri", mongodb_url) \
                   .appName("domestic_priceRange_statistic")\
                   .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.1.1") \
                   .getOrCreate()
sc = spark.sparkContext

def spark_readData():
    df = spark.read.format('csv').load('hdfs://10.0.1.218:9000/predict-2019/' + fileName, header=True)
    return df

def priceRange_statistic(df):
    df_filter = df.withColumn('departDate_monthDay_tmp', substring_index(df.flineId, '_', -1)) \
        .withColumn('predictDate_year_tmp', substring_index(df.predictDate, '-', 1)) \
        .withColumn('predictDate_monthDay_tmp', substring_index(df.predictDate, '-', -2)) \
        .withColumn('departDate', when(col('departDate_monthDay_tmp') > col('predictDate_monthDay_tmp'),
                                       concat(col('predictDate_year_tmp'), lit('-'),
                                              col('departDate_monthDay_tmp')))
                    .otherwise(concat(substring_index((col('predictDate_year_tmp') + 1).cast('string'), '.', 1),
                                      lit('-'), col('departDate_monthDay_tmp')))) \
        .drop('departDate_monthDay_tmp', 'predictDate_year_tmp', 'predictDate_monthDay_tmp') \
        .withColumn('startDate', date_sub('departDate', 50)) \
        .filter((col('predictDate') > col('startDate')) & (col('predictDate') <= col('departDate'))) \
        .withColumn('flineId', substring_index('flineId', '-', 1))\
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
    clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search")
    df = spark_readData()
    df_priceRange = priceRange_statistic(df)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ': =======df_priceRange is OK!=========')
    sparkDirName = 'domestic_priceRange_statistic_basePercentile_' + todayStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    df_priceRange.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
    sparkDirName = 'domestic_priceRange_statistic_basePercentile_' + before2_dateStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + "==============df_priceRange to HDFS is OK!============")
    sparkDirName = 'domestic_priceRange_statistic_basePercentile_' + todayStr1 + '.parquet'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    clientHdfs.download('/predict-2019/' + sparkDirName, '/data/search/predict-2019/' + sparkDirName)
    sparkDirName = 'domestic_priceRange_statistic_basePercentile_' + before2_dateStr1 + '.parquet'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + "========df_priceRange toLocal is OK!=============")
    sc.stop()



###############################
"""
均价和百分比两种方式统计的结果进行合并，写入mongoDB中
"""
###############################
import pandas as pd
import json
from pymongo import *
from datetime import datetime, timedelta
todayStr1 = datetime.strftime(datetime.now(), "%Y%m%d")
before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
avg_FileName = 'domestic_priceRange_statistic_baseAvg_' + todayStr1 + '.parquet'
percent_FileName = 'domestic_priceRange_statistic_basePercentile_' + todayStr1 + '.parquet'
df_avg = pd.read_parquet('/data/search/predict-2019/' + avg_FileName)
df_percentile = pd.read_parquet('/data/search/predict-2019/' + percent_FileName)
df = df_percentile.merge(df_avg, on='_id', how='outer')
clientMongo = MongoClient("172.29.1.172:27017", authSource="flight",
                          username='search', password='search@huoli123', authMechanism='MONGODB-CR')
cursor = clientMongo.flight.priceRange_statistic_domestic
data = json.loads(df.T.to_json()).values()
# if "priceRange_statistic_domestic" in clientMongo.flight.collection_names():
#     cursor.delete_many({})
for sample in data:
    cursor.update({'_id': sample['_id']}, sample, upsert=True)
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish avg_and_percentdata priceRange data insert MongoDB=====")





