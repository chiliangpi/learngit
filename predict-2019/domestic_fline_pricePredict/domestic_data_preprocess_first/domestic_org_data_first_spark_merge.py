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
import math
import shutil
import utils


# sparkDirName_org_lowPrice_data = "/Users/changfu/Desktop/domestic_org_data/lowPrice_data.csv"
# sparkDirName_org_orgPrice_data = "/Users/changfu/Desktop/domestic_org_data/orgPrice_data.csv"
# sparkDirName_org_Airport_data = "/Users/changfu/Desktop/domestic_org_data/Airport_data.csv"
# sparkDirName_org_seatLeft_data = "/Users/changfu/Desktop/domestic_org_data/seatLeft_data.csv"
# sparkDirName_org_infoBase_data = "/Users/changfu/Desktop/domestic_org_data/infoBase_data.csv"
# generateDate = datetime.today()
# generateDate_str2 = datetime.strftime(generateDate, "%Y-%m-%d")
# params = {"sparkDirName_org_lowPrice_data": sparkDirName_org_lowPrice_data,
#           "sparkDirName_org_orgPrice_data": sparkDirName_org_orgPrice_data,
#           "sparkDirName_org_Airport_data": sparkDirName_org_Airport_data,
#           "sparkDirName_org_seatLeft_data": sparkDirName_org_seatLeft_data,
#           "sparkDirName_org_infoBase_data": sparkDirName_org_infoBase_data,
#           "generateDate_str2": generateDate_str2
#           }
#
# spark =SparkSession.builder.master('local[*]').appName("test")\
#                        .getOrCreate()


def load_data(params):
    # columns = ["queryDate", 'price', 'id', 'org', 'dst']
    df_org_lowPrice = spark.read.format('csv').load(params["sparkDirName_org_lowPrice_data"], header=True)\
                        .dropna(how='any')\
                        .withColumn('price', col('price').cast('int'))\
                        .filter(col('price') < 100000)

    # columns = ["orgPrice_id", "fc", "orgPrice"]
    df_org_orgPrice = spark.read.format('csv').load(params["sparkDirName_org_orgPrice_data"], header=True)\
                        .dropna(how='any')


    # columns = ["Airport_code", "latitude", "longitude"]
    df_org_Airport = spark.read.format("csv").load(params["sparkDirName_org_Airport_data"], header=True)\
                        .dropna(how='any')\
                        .withColumn("latitude", col("latitude").cast('double'))\
                        .withColumn("longitude", col("longitude").cast('double'))

    # columns =['queryDatetime', 'seatLeft', 'seatLeft_id']
    df_org_seatLeft = spark.read.format('csv').load(params["sparkDirName_org_seatLeft_data"], header=True)\
                        .dropna(how='any')

    # columns =["infoBase_id", "departtime", "arrivetime", "isShare"]
    df_org_infoBase = spark.read.format('csv').load(params["sparkDirName_org_infoBase_data"], header=True) \
                        .dropna(how='any')

    #columns = ["queryDate", 'price', 'id', 'org', 'dst']
    #如果df_org_lowPrice_online中的queryDate距离今天超过14天，说明该样本在最近的14天都没有价格记录，则不对该样本进行预测
    df_org_lowPrice_online = spark.read.format('csv').load(params["sparkDirName_org_lowPrice_onlineData"], header=True) \
                        .dropna(how='any') \
                        .withColumn('price', col('price').cast('int')) \
                        .filter(col('price') < 100000) \
                        .filter(datediff(lit(params['generateDate_str2']), 'queryDate') <= 14)

    return df_org_lowPrice, df_org_orgPrice, df_org_Airport, df_org_seatLeft, df_org_infoBase, df_org_lowPrice_online


def confirm_departDate(x, y):  #x表示queryDate， y表示departMonthDay
    if x[5:] > y:
        return str(int(x[:4])+1) + '-' + y
    else:
        return x[:4] + '-' + y


def calculate_distance(org_longitude, org_latitude, dst_longitude, dst_latitude):
    # 将十进制度数转化为弧度
    # lon1, lat1, lon2, lat2 = map(math.radians, [float(org_longitude), float(org_latitude), float(dst_longitude), float(dst_latitude)])
    lon1, lat1, lon2, lat2 = map(math.radians,
                                 [org_longitude, org_latitude, dst_longitude, dst_latitude])
    # haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # 地球平均半径，单位为公里
    # 返回结果以公里为单位向上取整
    distance = math.ceil((c * r * 1000) / 1000)
    return distance


udf_confirm_departDate = udf(confirm_departDate, StringType())
udf_calculate_distance = udf(calculate_distance, IntegerType())


def cache_data(df_org_orgPrice, df_org_seatLeft, df_org_infoBase):
    id_splits = split(col('orgPrice_id'), '_')
    window_orgPrice = Window.partitionBy('fc', 'org', 'dst').orderBy('departDate')
    df_orgPrice = df_org_orgPrice.withColumn('org', id_splits.getItem(0)) \
        .withColumn('dst', id_splits.getItem(1)) \
        .withColumn('departDate', id_splits.getItem(2)) \
        .filter(col('departDate') >= lit(datetime.today().strftime("%Y-%m-%d"))) \
        .withColumn('rank', row_number().over(window_orgPrice)) \
        .filter(col('rank') == 1) \
        .withColumn('orgPrice', col('orgPrice').cast('int')) \
        .drop('orgPrice_id', 'rank', 'departDate') \
        .dropna(how='any')

    id_splits = split(col('seatLeft_id'), '_')
    window_seatLeft = Window.partitionBy('seatLeft_id', 'queryDate').orderBy(col('queryTime').desc())
    df_seatLeft = df_org_seatLeft.withColumn('queryDate', date_format('queryDatetime', "YYYY-MM-dd")) \
        .withColumn('queryTime', date_format('queryDatetime', "HH:mm")) \
        .withColumn('rank', row_number().over(window_seatLeft)) \
        .filter(col('rank') == 1) \
        .withColumn('fn', id_splits.getItem(0)) \
        .withColumn('org', id_splits.getItem(1)) \
        .withColumn('dst', id_splits.getItem(2)) \
        .withColumn('departDate', id_splits.getItem(3)) \
        .drop('seatLeft_id', 'rank') \
        .withColumn('departDate', udf_confirm_departDate('queryDate', 'departDate')) \
        .withColumn('seatLeft', substring_index('seatLeft', '%', 1).cast('double')) \
        .drop('queryDatetime', 'queryTime') \
        .dropna(how='any')

    id_splits = split(col('infoBase_id'), '_')
    window_infoBase = Window.partitionBy('fn', 'org', 'dst').orderBy('departDate')
    df_infoBase = df_org_infoBase.withColumn('fn', id_splits.getItem(2)) \
        .withColumn('org', id_splits.getItem(0)) \
        .withColumn('dst', id_splits.getItem(1)) \
        .withColumn('departDate', id_splits.getItem(3)) \
        .filter(col('departDate') >= lit(datetime.today().strftime("%Y-%m-%d"))) \
        .withColumn('rank', row_number().over(window_infoBase)) \
        .filter(col('rank') == 1) \
        .withColumn('isShare', when(col('isShare') == 'True', 1).otherwise(0)) \
        .drop("infoBase_id", "rank", "departDate") \
        .dropna(how='any')
    return df_orgPrice, df_seatLeft, df_infoBase


def merge_data(df_org_lowPrice, df_org_Airport, df_org_lowPrice_online, df_orgPrice, df_seatLeft, df_infoBase, params, is_online=False):
    if is_online == False:
        id_splits = split(col('id'), '_')
        window_groupCount = Window.partitionBy('fn', 'city_code', 'departDate').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        #划窗边界为：当前日期的下一天至最后一天
        window_minPrice = Window.partitionBy('fn', 'city_code', 'departDate').orderBy('queryDate').rowsBetween(2, Window.unboundedFollowing)
        window_nextQueryPrice = Window.partitionBy('fn', 'city_code', 'departDate').orderBy('queryDate')
        df_lowPrice = df_org_lowPrice.withColumn('fn', id_splits.getItem(0))\
                            .withColumn('fc', substring('fn', 1, 2))\
                            .withColumn('city_code', substring_index(substring_index(col('id'), '_', 3), '_', -2))\
                            .withColumn('departDate', id_splits.getItem(3))\
                            .drop('id')\
                            .withColumn('departDate', udf_confirm_departDate(col('queryDate'), col('departDate')))\
                            .filter(col('departDate') < params["generateDate_str2"])\
                            .filter(datediff('departDate', 'queryDate') <= 30)\
                            .withColumn('group_count', count('price').over(window_groupCount))\
                            .filter(col('group_count') >= 15)\
                            .drop('group_count')\
                            .withColumn('futureMinPrice', min('price').over(window_minPrice))\
                            .withColumn('nextQueryPrice', lead('price', 1).over(window_nextQueryPrice))\
                            .dropna(how='any')\
                            .withColumn('trend', (col('futureMinPrice')/col('nextQueryPrice')).between(0, 0.96).cast('int'))\
                            .drop('nextQueryPrice')
    elif is_online == True:
        id_splits = split(col('id'), '_')
        df_lowPrice = df_org_lowPrice_online.withColumn('fn', id_splits.getItem(0)) \
            .withColumn('fc', substring('fn', 1, 2)) \
            .withColumn('city_code', substring_index(substring_index(col('id'), '_', 3), '_', -2)) \
            .withColumn('departDate', id_splits.getItem(3)) \
            .drop('id') \
            .withColumn('departDate', udf_confirm_departDate(col('queryDate'), col('departDate'))) \
            .withColumn('queryDate', lit(params['yesterday_str2']))\
            .dropna(how='any')\
            .withColumn('futureMinPrice', lit('null')) \
            .withColumn('trend', lit('null'))

    df = df_lowPrice.join(df_org_Airport, df_lowPrice.org == df_org_Airport.Airport_code, 'inner') \
        .drop('Airport_code') \
        .withColumnRenamed("longitude", "org_longitude") \
        .withColumnRenamed("latitude", "org_latitude") \
        .join(df_org_Airport, df_lowPrice.dst == df_org_Airport.Airport_code, 'inner') \
        .drop('Airport_code') \
        .withColumnRenamed("longitude", "dst_longitude") \
        .withColumnRenamed("latitude", "dst_latitude") \
        .dropna(how='any', subset=["org_longitude", "org_latitude", "dst_longitude", "dst_latitude"])\
        .withColumn("distance",
                    udf_calculate_distance("org_longitude", "org_latitude", "dst_longitude", "dst_latitude"))\
        .join(df_orgPrice, ['fc', 'org', 'dst'], 'inner') \
        .withColumn('discount', round(col('price') / col('orgPrice'), 2)) \
        .join(df_seatLeft, ["fn", "org", "dst", "departDate", "queryDate"], "left") \
        .join(df_infoBase, ["fn", "org", "dst"], "inner")
    return df


if __name__ == "__main__":
    # spark =SparkSession.builder.master('local[*]').appName("test")\
    #                    .getOrCreate()
    spark = SparkSession.builder \
        .appName("domestic_org_data_merge") \
        .getOrCreate()
    sc = spark.sparkContext
    params = config.params
    df_org_lowPrice, df_org_orgPrice, df_org_Airport, df_org_seatLeft, df_org_infoBase, df_org_lowPrice_online = load_data(params)
    # logger.info("====load org data finished=====")
    df_orgPrice, df_seatLeft, df_infoBase = cache_data(df_org_orgPrice, df_org_seatLeft, df_org_infoBase)
    df_orgPrice.cache()
    df_seatLeft.cache()
    df_infoBase.cache()
    df_trainData = merge_data(df_org_lowPrice, df_org_Airport, df_org_lowPrice_online, df_orgPrice, df_seatLeft, df_infoBase, params, is_online=False)
    # logger.info("====df_train sample is: {}====".format(df_trainData.rdd.take(3)))
    df_trainData.write.format('csv').save(params["sparkDirName_org_trainData_union"], header=True, mode="overwrite")
    logger.info("====\"{}\" write to HDFS finished ====".format(params["sparkDirName_org_trainData_union"]))
    utils.delete_before4_sparkData(params["sparkDirName_org_trainData_union"], params)
    df_onlineData = merge_data(df_org_lowPrice, df_org_Airport, df_org_lowPrice_online, df_orgPrice, df_seatLeft, df_infoBase, params, is_online=True)
    # logger.info("====df_online sample is: {}====".format(df_onlineData.rdd.take(3)))
    df_onlineData.write.format('csv').save(params["sparkDirName_org_onlineData"], header=True, mode="overwrite")
    logger.info("====\"{}\" write to HDFS finished ====".format(params["sparkDirName_org_onlineData"]))
    utils.delete_before4_sparkData(params["sparkDirName_org_onlineData"], params)


