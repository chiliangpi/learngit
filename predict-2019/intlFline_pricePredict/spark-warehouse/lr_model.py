# coding:utf-8
# -*- coding:utf-8 -*-
##############################
"""
将特征工程后的数据喂入LR线性模型中，得到预测数据集，并将数据转存到服务器
"""
##############################
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
                   .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.0") \
                   .appName("lr_model_prediction")\
                   .getOrCreate()
sc = spark.sparkContext

def spark_readData():
    oneHot_historyData_file = 'hdfs://10.0.1.218:9000/predict-2019/oneHot_history_data_' + todayStr1 + '.parquet'
    oneHot_predictData_file = 'hdfs://10.0.1.218:9000/predict-2019/oneHot_predict_data_' + todayStr1 + '.parquet'
    oneHot_historyData = spark.read.format('parquet').load(oneHot_historyData_file)
    oneHot_predictData = spark.read.format('parquet').load(oneHot_predictData_file)
    return oneHot_historyData, oneHot_predictData

def train_val_data(oneHot_historyData):
    train_data = oneHot_historyData.filter("departDate > '2019-04-01'")
    val_data = oneHot_historyData.filter("departDate <= '2019-04-01'")
    return train_data, val_data

def mseError(pred):
    mseError_DF = pred.select((sum(abs(pred.prediction - pred.price) / pred.price) / pred.count()).alias('MSE'))
    return mseError_DF

def lr_predict_result(oneHot_historyData, oneHot_predictData):
    lr = LinearRegression(maxIter=100, standardization=False, featuresCol="features", labelCol="price",
                          predictionCol="prediction")
    lrModel = lr.fit(oneHot_historyData)
    pred = lrModel.transform(oneHot_predictData)
    predictResult = pred.select(concat('flineId_noDate', lit('_'), col('departDate').cast('string')).alias('_id'),
                       col('predictDate').cast('string').alias('predictDate'),
                       col('prediction').cast('int').alias('prediction'))
    return predictResult

def mongoData(predictResult):
    departDate_data = predictResult.filter(substring_index('_id', '_', -1) == col('predictDate'))\
                                   .withColumn('priceTag', lit('departPrice'))\
                                   .withColumn('dateTag', lit('departDate'))
    departPrice_date = departDate_data.select('_id', col('predictDate').alias('value'), col('dateTag').alias('key'))
    departPrice_price = departDate_data.select('_id', col('prediction').cast('string').alias('value'), col('priceTag').alias('key'))
    window1 = Window.partitionBy('_id').orderBy(['prediction', desc('predictDate')])
    window2 = Window.partitionBy('_id').orderBy([desc('prediction'), desc('predictDate')])
    lowAndHighPrice_tag = predictResult.withColumn('lowPrice', row_number().over(window1))\
                                 .withColumn('highPrice', row_number().over(window2))\
                                 .filter((col('lowPrice') == 1) | (col('highPrice') == 1))\
                                 .withColumn('priceTag', when(col('lowPrice') == 1, 'lowPrice').otherwise('highPrice'))\
                                 .withColumn('dateTag', when(col('lowPrice') == 1, 'lowPriceDate').otherwise('highPriceDate'))
    lowAndHighPrice_date = lowAndHighPrice_tag.select('_id', col('predictDate').alias('value'), col('dateTag').alias('key'))
    lowAndHighPrice_price = lowAndHighPrice_tag.select('_id', col('prediction').cast('string').alias('value'), col('priceTag').alias('key'))
    mongoDataframe = departPrice_date.union(departPrice_price).union(lowAndHighPrice_date).union(lowAndHighPrice_price)
    mongoDataframe = mongoDataframe.groupBy('_id').pivot('key').agg(max('value'))
    return mongoDataframe





    # def strDict(day, price):
    #     return str({day: price})
    # strDict_udf = udf(strDict, StringType())
    # mongoDataframe = lowAndHighPrice_data.union(departDate_data)\
    #                                     .withColumn('tmp_strDict', strDict_udf(col('predictDate'), col('prediction')))\
    #                                     .drop('prediction', 'predictDate')\
    #                                     .groupBy('_id').pivot('tag').agg(max('tmp_strDict'))


# def dataframeToMongoDict(predictResult):
#     """
#     将dataframe转换成字典的形式，存入mongo(集群资源无法满足，弃用)
#     :param predictResult:
#     :return:
#     """
#     def array_zip(array1, array2):
#         return [x for x in zip(array1, array2)]
#     def insert_dictKey(x, y):
#         yDict = dict(y)
#         yDict.update({'_id': x})
#         return yDict
#     zip_udf = udf(array_zip, ArrayType(ArrayType(StringType())))
#     insert_udf = udf(insert_dictKey, MapType(StringType(), StringType()))
#     predictResult = predictResult.repartition(200, col('_id'))
#     mongoDict = predictResult.groupBy('_id').agg(zip_udf(collect_list('predictDate'), collect_list('prediction')).alias('tmpArray'))\
#                              .select(insert_udf('_id', 'tmpArray').alias('mongoDict'))
#     return mongoDict

if __name__ == "__main__":
    todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
    todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
    before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
    clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search") #跟HDFS建立连接
    oneHot_historyData, oneHot_predictData = spark_readData()
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + '=====read data is OK!!!=====')
    predictResult = lr_predict_result(oneHot_historyData, oneHot_predictData)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ' =====lr_predictResult_dataframe calculation is OK!!!=====')
    sparkDirName = 'lr_predictResult_dataframe_' + todayStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    predictResult.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
    sparkDirName = 'lr_predictResult_dataframe_' + before2_dateStr1 + '.parquet'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ' =====lr_predictResult_dataframe to HDFS is OK!!!=====')
    mongoDataframe = mongoData(predictResult)
    sparkDirName = 'lr_predictResult_mongoDict_' + todayStr1 + '.json'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    mongoDataframe.write.format('json').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
    sparkDirName = 'lr_predictResult_mongoDict_' + before2_dateStr1 + '.json'
    if sparkDirName in clientHdfs.list('/predict-2019/'):
        clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ' =====Finish lr_predictResult_mongoDict to HDFS=====')
    sparkDirName = 'lr_predictResult_mongoDict_' + todayStr1 + '.json'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    clientHdfs.download('/predict-2019/' + sparkDirName, '/data/search/predict-2019/' + sparkDirName)
    sparkDirName = 'lr_predictResult_mongoDict_' + before2_dateStr1 + '.json'
    if sparkDirName in os.listdir('/data/search/predict-2019/'):
        shutil.rmtree('/data/search/predict-2019/' + sparkDirName)
    nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(nowStr + ": =====Finish lr_predictResult_mongoDict to Local=====")
    sc.stop()




#############
"""
遍历的spark生成的json文件，将数据存入MongoDB（使用此方法）
"""
#############
import os
import json
from pymongo import *
from datetime import datetime, timedelta

nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
print(nowStr + "=====Start insert mongoDict to MongoDB=====")
clientMongo = MongoClient("10.0.1.212:27017", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR')
cursor = clientMongo.flight.test_insert
# if "test_insert" in clientMongo.flight.collection_names():
#     cursor.delete_many({})
dirName = 'lr_predictResult_mongoDict_' + todayStr1 + '.json'
listDir = os.listdir("/data/search/predict-2019/" + dirName)
listDir.remove('_SUCCESS')
for fileName in listDir:
    with open('/data/search/predict-2019/' + dirName + '/' + fileName, 'r') as f:
        try:
            while True:
                txt = f.readline()
                Dict = json.loads(txt)
                for key in ['lowPrice', 'highPrice', 'departPrice']:  #['lowPrice', 'highPrice', 'departPrice']的值为字符串，转换为数值型
                    if Dict.get(key):
                        Dict[key] = json.loads(Dict[key])
                    else:
                        continue
                cursor.update({'_id': Dict['_id']}, Dict, upsert=True)
        except:
            continue
for sample in cursor.find({}):  #mongo数据更新完成后，删除起飞时间小于今天的数据
    departDate_str = sample['_id'][-10:]
    if departDate_str < todayStr2:
        cursor.delete_one(sample)
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish insert json to MongoDB=====")














#############
"""
遍历的方式将数据存入MongoDB（速度太慢，弃用）
"""
#############

import os
from pymongo import *
import csv
from datetime import datetime, timedelta
"""
将dataframe转换成csv文件，然后通过遍历的方式，存入mongoDB(速度太慢，弃用)
"""
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
print(nowStr + "=====Start insert csv to MongoDB=====")
clientMongo = MongoClient("10.0.1.212:27017", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR')
cursor = clientMongo.flight.test_insert
if "test_insert" in clientMongo.flight.collection_names():
    cursor.delete_many({})
dirName = 'lr_prediction_data_' + todayStr1 + '.csv'
listDir = os.listdir("/data/search/predict-2019/" + dirName)
listDir.remove('_SUCCESS')
for fileName in listDir:
    with open("/data/search/predict-2019/" + dirName + '/' + fileName, 'r') as f:
        content = csv.reader(f)
        for line in content:
            cursor.update({'_id': line[0]}, {'$set': {line[1]: int(line[2])}}, upsert=True)
nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(nowStr + "=====Finish insert csv to MongoDB=====")








