# coding:utf-8
# -*- coding:utf-8 -*-

# coding:utf-8
# -*- coding:utf-8 -*-

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.ml.feature import VectorIndexer, StringIndexer, VectorAssembler, OneHotEncoder, ChiSqSelector,Bucketizer
from datetime import datetime, timedelta
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql import SparkSession
from hdfs import client
from datetime import datetime, timedelta
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,Row
import config
from logConfig import logger
import pickle
import os
import re
import shutil
import utils


def load_data(params):
    df_org_train_union = spark.read.format('csv').load(params["sparkHost"] + params["sparkDirName_org_trainData_union"], header=True)\
                                .dropna(how='any', subset=list(set(params["org_columnNames"])-set(['seatLeft']))) \
                                .filter(col('price').cast('double') < 100000)

    #此处是预测用的online数据
    df_org_online = spark.read.format('csv').load(params["sparkHost"] + params["sparkDirName_org_onlineData"], header=True)\
            .dropna(how='any', subset=list(set(params["org_columnNames"])-set(['seatLeft', 'trend', 'futureMinPrice']))) \
            .filter(col('price').cast('double') < 100000)

    #此处是验证集数据
    # df_org_online = spark.read.format('csv').load(params["sparkHost"] + params["sparkDirName_valSampleData"], header=True) \
    #     .dropna(how='any', subset=list(set(params["org_columnNames"]) - set(['seatLeft', 'trend', 'futureMinPrice']))) \
    #     .filter(col('price').cast('double') < 100000)

    df_holiday = spark.read.format('csv').load('hdfs://10.0.4.217:8020/crawler/holiday_list.csv', header=True)\
                    .select(concat(substring('calday',1,4), lit('-'), substring('calday',5,2), lit('-'), substring('calday',7,2)).alias('calday'),
                                         'calday_ln', 'festival', 'is_vacation', 'vacation_days','day_of_vacation')
    #将day_of_vacation（假期第几天）分别向前和向后扩展一天，分别标注为-1，1001
    window = Window.partitionBy('festival').orderBy(desc('day_of_vacation'))
    df_day_of_vacation_future1 = df_holiday.select(date_add('calday', 1).cast('string').alias('calday_future1'), lit(1001).alias('day_of_vacation_future1'), row_number().over(window).alias('rank'))\
                                    .filter((col('festival').isNotNull()) & (col('rank')==1))\
                                    .drop('rank')
    df_day_of_vacation_pre1 = df_holiday.filter(col('day_of_vacation')=='1')\
        .select(date_add('calday', -1).cast('string').alias('calday_pre1'), lit(-1).alias('day_of_vacation_pre1'))
    return df_org_train_union, df_org_online, df_holiday, df_day_of_vacation_future1, df_day_of_vacation_pre1



def data_preprocess(df_org_train_union, df_org_online, df_holiday, df_day_of_vacation_future1, df_day_of_vacation_pre1):
    df_org_all = df_org_train_union.unionByName(df_org_online)
    df_all = df_org_all.withColumn('discountOff', round(100-col('discount'), 2)) \
        .withColumn('distance', col('distance').cast('double')) \
        .na.fill({'seatLeft': '-999'}) \
        .withColumn('seatLeft', col('seatLeft').cast('double')) \
        .withColumn('departTime', substring_index('departTime', ':', 1).cast('int')) \
        .withColumn('arriveTime', substring_index('arriveTime', ':', 1).cast('int')) \
        .withColumn('isShare', col('isShare').cast('int')) \
        .join(df_holiday, df_org_all.departDate == df_holiday.calday, 'left') \
        .drop('calday') \
        .withColumnRenamed('calday_ln', 'depart_calday_ln') \
        .withColumnRenamed('festival', 'depart_festival') \
        .withColumn('depart_isFestival', when(col('depart_festival').isNotNull(), 1).otherwise(0)) \
        .withColumnRenamed('is_vacation', 'depart_isVacation') \
        .withColumn('depart_isVacation', when(col('depart_isVacation').isNotNull(), 1).otherwise(0)) \
        .withColumnRenamed('vacation_days', 'depart_vacationDays') \
        .withColumnRenamed('day_of_vacation', 'depart_dayofvacation') \
        .join(df_day_of_vacation_future1, df_org_all.departDate == df_day_of_vacation_future1.calday_future1, 'left') \
        .drop('calday_future1') \
        .withColumnRenamed('day_of_vacation_future1', 'day_of_vacation_future1_departDate') \
        .join(df_day_of_vacation_pre1, df_org_all.departDate == df_day_of_vacation_pre1.calday_pre1, 'left') \
        .drop('calday_pre1') \
        .withColumnRenamed('day_of_vacation_pre1', 'day_of_vacation_pre1_departDate') \
        .withColumn('depart_dayofvacation_extend',
                    coalesce('depart_dayofvacation', 'day_of_vacation_future1_departDate',
                             'day_of_vacation_pre1_departDate').cast('int')) \
        .drop('day_of_vacation_future1_departDate', 'day_of_vacation_pre1_departDate') \
        .na.fill({'depart_vacationDays': '-999', 'depart_dayofvacation': '-999', 'depart_dayofvacation_extend': '-999',
                  'depart_festival': '-999'}) \
        .withColumn('depart_vacationDays', col('depart_vacationDays').cast('int')) \
        .withColumn('depart_dayofvacation', col('depart_dayofvacation').cast('int')) \
        .withColumn('departYear', year('departDate')) \
        .withColumn('departMonth', month('departDate')) \
        .withColumn('depart_dayofmonth', dayofmonth('departDate')) \
        .withColumn('depart_weekofyear', weekofyear('departDate')) \
        .withColumn('depart_dayofweek', datediff('departDate', lit('2017-01-09')) % 7 + 1)\
        .withColumn('depart_dayofweek_bucket', when(col('depart_dayofweek') < 5, 0).otherwise(col('depart_dayofweek'))) \
        .drop('dayofweek') \
        .withColumn('departQuarter', quarter('departDate')) \
        .withColumn('intervalDays', datediff('departDate', 'queryDate')) \
        .withColumn('intervalMonths', round(months_between('departDate', 'queryDate'))) \
        .withColumn('intervalWeeks', round(col('intervalDays') / 7)) \
        .withColumn('intervalQuarters', round(col('intervalDays') / 120)) \
        .join(df_holiday, df_org_all.queryDate == df_holiday.calday, 'left') \
        .drop('calday') \
        .withColumnRenamed('calday_ln', 'query_calday_ln') \
        .withColumnRenamed('festival', 'query_festival') \
        .withColumn('query_isFestival', when(col('query_festival').isNotNull(), 1).otherwise(0)) \
        .withColumnRenamed('is_vacation', 'query_isVacation') \
        .withColumn('query_isVacation', when(col('query_isVacation').isNotNull(), 1).otherwise(0)) \
        .withColumnRenamed('vacation_days', 'query_vacationDays') \
        .withColumnRenamed('day_of_vacation', 'query_dayofvacation') \
        .join(df_day_of_vacation_future1, df_org_all.queryDate == df_day_of_vacation_future1.calday_future1, 'left') \
        .drop('calday_future1') \
        .withColumnRenamed('day_of_vacation_future1', 'day_of_vacation_future1_queryDate') \
        .join(df_day_of_vacation_pre1, df_org_all.queryDate == df_day_of_vacation_pre1.calday_pre1, 'left') \
        .drop('calday_pre1') \
        .withColumnRenamed('day_of_vacation_pre1', 'day_of_vacation_pre1_queryDate') \
        .withColumn('query_dayofvacation_extend', coalesce('query_dayofvacation', 'day_of_vacation_future1_queryDate',
                                                           'day_of_vacation_pre1_queryDate').cast('int')) \
        .drop('day_of_vacation_future1_queryDate', 'day_of_vacation_pre1_queryDate') \
        .na.fill({'query_vacationDays': '-999', 'query_dayofvacation': '-999', 'query_dayofvacation_extend': '-999',
                  'query_festival': '-999'}) \
        .withColumn('query_vacationDays', col('query_vacationDays').cast('int')) \
        .withColumn('query_dayofvacation', col('query_dayofvacation').cast('int')) \
        .withColumn('queryYear', year('queryDate')) \
        .withColumn('queryMonth', month('queryDate')) \
        .withColumn('query_dayofmonth', dayofmonth('queryDate')) \
        .withColumn('query_weekofyear', weekofyear('queryDate')) \
        .withColumn('query_dayofweek', datediff('queryDate', lit('2017-01-09')) % 7 + 1) \
        .withColumn('query_dayofweek_bucket', when(col('query_dayofweek') < 5, 0).otherwise(col('query_dayofweek'))) \
        .withColumn('queryQuarter', quarter('queryDate'))

    stringIndexerCols = ['fn', 'fc', 'org', 'dst', 'depart_festival', 'query_festival']
    bucketizerCols = ['discountOff', 'distance', 'seatLeft']
    discountOffSplits = [-float('inf')] + list(range(0, 101, 1))
    distanceSplits = list(range(0, 6001, 300)) + [float('inf')]
    seatLeftSplits = [-float('inf')] + list(range(0, 105, 1))
    splits = [discountOffSplits, distanceSplits, seatLeftSplits]

    stages = []
    for colName in stringIndexerCols:
        stringIndexer = StringIndexer(inputCol=colName, outputCol='stringIndexer_%s' % colName)
        stages.append(stringIndexer)
    for split, colName in zip(splits, bucketizerCols):
        bucketizer = Bucketizer(splits=split, inputCol=colName, outputCol='%sBucket' % colName)
        stages.append(bucketizer)
    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(df_all)
    df_all = pipelineModel.transform(df_all)
    for colName in stringIndexerCols:
        df_all = df_all.withColumn('stringIndexer_%s' % colName, col('stringIndexer_%s' % colName).cast('double'))
    df_all.cache()
    df_org_train_union.unpersist()
    return df_all


def write_to_HDFS(df_all, params):

    #起飞时间大于generateDate为需要预测的线上数据，起飞时间小于generateDate为训练数据，起飞时间等于generateDate的数据不需要保留
    #从所有数据中随机抽取4千万的数据，作为训练集
    df_train_DNN = df_all.filter(df_all.departDate < params["generateDate_str2"])\
                     .drop(*list(set(params['dropFeatures'])-set(params['baseColumns'])))\
                     .sample(False, float("%.4f" % (6e7/df_all.count())))\
                     .orderBy(rand())
    # df_train_DNN.cache()
    df_train_DNN.write.format('parquet').save(params["sparkHost"] + params["sparkDirName_trainData"], mode='overwrite')
    logger.info("====\"{}\" write to HDFS finished ====".format(params["sparkDirName_trainData"]))
    utils.delete_before4_sparkData(params["sparkDirName_trainData"], params)

    # df_trainSample_DNN = df_train_DNN.sample(False, float("%.4f" % (2e5/df_train_DNN.count())))
    # df_trainSample_DNN.write.format('parquet').save(params["sparkHost"] + params["sparkDirName_trainSampleData"], mode='overwrite')
    # logger.info("====\"{}\" write to HDFS finished ====".format(params["sparkDirName_trainSampleData"]))
    # utils.delete_before4_sparkData(params["sparkDirName_trainSampleData"], params)

    df_online_DNN = df_all.filter(df_all.departDate > params["generateDate_str2"])\
                     .drop(*list(set(params['dropFeatures']) - set(params['baseColumns'])))
    df_online_DNN.write.format('parquet').save(params["sparkHost"] + params["sparkDirName_onlineData"], mode='overwrite')
    logger.info("====\"{}\" write to HDFS finished ====".format(params["sparkDirName_onlineData"]))
    utils.delete_before4_sparkData(params["sparkDirName_onlineData"], params)


#deepFM和deep&cross生成featureDict的方式不同
def write_deepFM_featureDict(df_all, params):
    df = df_all.drop(*params['dropFeatures']).drop(params['label'])
    featureDict = {}
    tc = 0
    for colName in df.columns:
        if colName in params["numericCols"]:
            featureDict[colName] = tc
            tc += 1
        else:  # colName in categoryCols
            uniqueFeature = df.select(colName).distinct().toPandas()[colName].astype('float').values
            featureDict[colName] = dict(zip(uniqueFeature, range(tc, len(uniqueFeature) + tc)))
            tc = tc + len(uniqueFeature)
    with open(params["featureDict_fileName"], 'wb') as f:
        pickle.dump(featureDict, f)
        logger.info("====\"{}\" finished ====".format(params["featureDict_fileName"].split('/')[-1]))
    utils.delete_before4_localData(params["featureDict_fileName"], params)

#deepFM和deep&cross生成featureDict的方式不同
def write_DCN_featureDict(df_all, params):
    df = df_all.drop(*params["dropFeatures"]).drop(params['label'])
    featureDict = {}
    tc = 0
    for colName in df.columns:
        if colName in params["numericCols"]:
            continue
        else:  # colName in categoryCols
            uniqueFeature = df.select(colName).distinct().toPandas()[colName].astype('float').values
            featureDict[colName] = dict(zip(uniqueFeature, range(tc, len(uniqueFeature) + tc)))
            tc = tc + len(uniqueFeature)
    with open(params["featureDict_fileName"], 'wb') as f:
        pickle.dump(featureDict, f)
        logger.info("====\"{}\" finished ====".format(params["featureDict_fileName"].split('/')[-1]))
    utils.delete_before4_localData(params["featureDict_fileName"], params)


if __name__ == "__main__":
    # spark =SparkSession.builder.master('local[*]').appName("test")\
    #                    .getOrCreate()
    spark = SparkSession.builder \
        .appName("domestic_featuresProcess") \
        .getOrCreate()
    sc = spark.sparkContext

    params = config.params
    clientHdfs = client.InsecureClient(params["hdfsHost"], user="search")
    df_org_train_union, df_org_online, df_holiday, df_day_of_vacation_future1, df_day_of_vacation_pre1 = load_data(params)
    df_all = data_preprocess(df_org_train_union, df_org_online, df_holiday, df_day_of_vacation_future1, df_day_of_vacation_pre1)
    write_to_HDFS(df_all, params)
    write_deepFM_featureDict(df_all, params)













