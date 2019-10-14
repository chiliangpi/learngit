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
import math
import os
import re
import shutil
import utils


def load_data(params):
    # columns = ["queryDate", 'price', 'id', 'org', 'dst']
    #如果queryDate距离今天超过100天，说明该queryDate的数据不是今年产生的（非最近的航班）
    df_org_lowPrice_add = spark.read.format('csv').load(params["sparkDirName_org_lowPrice_data_add"], header=True) \
        .dropna(how='any') \
        .withColumn('price', col('price').cast('int')) \
        .filter(col('price') < 100000)\
        .filter(datediff(lit(params["generateDate_str2"]), 'queryDate') < 100)

    # columns = ["orgPrice_id", "fc", "orgPrice"]
    df_org_orgPrice = spark.read.format('csv').load(params["sparkDirName_org_orgPrice_data"], header=True) \
        .dropna(how='any')

    # columns = ["Airport_code", "latitude", "longitude"]
    df_org_Airport = spark.read.format("csv").load(params["sparkDirName_org_Airport_data"], header=True) \
        .dropna(how='any') \
        .withColumn("latitude", col("latitude").cast('double')) \
        .withColumn("longitude", col("longitude").cast('double'))

    # columns =['queryDatetime', 'seatLeft', 'seatLeft_id']
    # 如果queryDate距离今天超过100天，说明该queryDate的数据不是今年产生的（非最近的航班）
    df_org_seatLeft_add = spark.read.format('csv').load(params["sparkDirName_org_seatLeft_data_add"], header=True) \
        .dropna(how='any')\
        .filter(datediff(lit(params["generateDate_str2"]), 'queryDatetime') < 100)

    # columns =["infoBase_id", "departtime", "arrivetime", "isShare"]
    df_org_infoBase = spark.read.format('csv').load(params["sparkDirName_org_infoBase_data"], header=True) \
        .dropna(how='any')

    df_org_train_yesterday = spark.read.format('csv').load(params["sparkHost"] + params["sparkDirName_org_trainData_yesterday"], header=True)\
                                .dropna(how='any', subset=list(set(params["org_columnNames"])-set(['seatLeft']))) \
                                .filter(col('price').cast('double') < 100000)

    #columns = ["queryDate", 'price', 'id', 'org', 'dst']
    # 如果df_org_lowPrice_online中的queryDate距离今天超过14天，说明该样本在最近的14天都没有价格记录，则不对该样本进行预测
    df_org_lowPrice_online = spark.read.format('csv').load(params["sparkHost"] + params["sparkDirName_org_lowPrice_onlineData"], header=True)\
                                .dropna(how='any') \
                                .withColumn('price', col('price').cast('int')) \
                                .filter(col('price') < 100000) \
                                .filter(datediff(lit(params['generateDate_str2']), 'queryDate') <= 14)

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

    return df_org_lowPrice_add, df_org_orgPrice, df_org_Airport, df_org_seatLeft_add, df_org_infoBase, df_org_train_yesterday, \
           df_org_lowPrice_online, df_holiday, df_day_of_vacation_future1, df_day_of_vacation_pre1



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


def cache_data(df_org_orgPrice, df_org_seatLeft_add, df_org_infoBase):
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
    df_seatLeft_add = df_org_seatLeft_add.withColumn('queryDate', date_format('queryDatetime', "YYYY-MM-dd")) \
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
    return df_orgPrice, df_seatLeft_add, df_infoBase


def merge_data(df_org_lowPrice_add, df_org_Airport, df_org_lowPrice_online, df_orgPrice, df_seatLeft_add, df_infoBase, params, is_online=False):
    if is_online == False:
        id_splits = split(col('id'), '_')
        window_groupCount = Window.partitionBy('fn', 'city_code', 'departDate').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        #划窗边界为：当前日期的下一天至最后一天
        window_minPrice = Window.partitionBy('fn', 'city_code', 'departDate').orderBy('queryDate').rowsBetween(2, Window.unboundedFollowing)
        window_nextQueryPrice = Window.partitionBy('fn', 'city_code', 'departDate').orderBy('queryDate')
        df_lowPrice_add = df_org_lowPrice_add.withColumn('fn', id_splits.getItem(0))\
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
        df_lowPrice_add = df_org_lowPrice_online.withColumn('fn', id_splits.getItem(0)) \
            .withColumn('fc', substring('fn', 1, 2)) \
            .withColumn('city_code', substring_index(substring_index(col('id'), '_', 3), '_', -2)) \
            .withColumn('departDate', id_splits.getItem(3)) \
            .drop('id') \
            .withColumn('departDate', udf_confirm_departDate(col('queryDate'), col('departDate'))) \
            .withColumn('queryDate', lit(params['yesterday_str2']))\
            .dropna(how='any')\
            .withColumn('futureMinPrice', lit('null')) \
            .withColumn('trend', lit('null'))

    df = df_lowPrice_add.join(df_org_Airport, df_lowPrice_add.org == df_org_Airport.Airport_code, 'inner') \
        .drop('Airport_code') \
        .withColumnRenamed("longitude", "org_longitude") \
        .withColumnRenamed("latitude", "org_latitude") \
        .join(df_org_Airport, df_lowPrice_add.dst == df_org_Airport.Airport_code, 'inner') \
        .drop('Airport_code') \
        .withColumnRenamed("longitude", "dst_longitude") \
        .withColumnRenamed("latitude", "dst_latitude") \
        .dropna(how='any', subset=["org_longitude", "org_latitude", "dst_longitude", "dst_latitude"])\
        .withColumn("distance",
                    udf_calculate_distance("org_longitude", "org_latitude", "dst_longitude", "dst_latitude"))\
        .join(df_orgPrice, ['fc', 'org', 'dst'], 'inner') \
        .withColumn('discount', round(col('price') / col('orgPrice'), 2)) \
        .join(df_seatLeft_add, ["fn", "org", "dst", "departDate", "queryDate"], "left") \
        .join(df_infoBase, ["fn", "org", "dst"], "inner")
    return df



# def merge_data(df_org_lowPrice_add, df_org_orgPrice, df_org_Airport, df_org_seatLeft_add, df_org_infoBase, df_org_lowPrice_online, params, is_online=False):
#     if is_online == False:
#         id_splits = split(col('id'), '_')
#         window_groupCount = Window.partitionBy('fn', 'city_code', 'departDate').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
#         #划窗边界为：当前日期的下一天至最后一天
#         window_minPrice = Window.partitionBy('fn', 'city_code', 'departDate').orderBy('queryDate').rowsBetween(2, Window.unboundedFollowing)
#         window_nextQueryPrice = Window.partitionBy('fn', 'city_code', 'departDate').orderBy('queryDate')
#         df_lowPrice = df_org_lowPrice_add.withColumn('fn', id_splits.getItem(0))\
#                             .withColumn('fc', substring('fn', 1, 2))\
#                             .withColumn('city_code', substring_index(substring_index(col('id'), '_', 3), '_', -2))\
#                             .withColumn('departDate', id_splits.getItem(3))\
#                             .drop('id')\
#                             .withColumn('departDate', udf_confirm_departDate(col('queryDate'), col('departDate')))\
#                             .filter(col('departDate') < params["generateDate_str2"])\
#                             .filter(datediff('departDate', 'queryDate') <= 30)\
#                             .withColumn('group_count', count('price').over(window_groupCount))\
#                             .filter(col('group_count') >= 15)\
#                             .drop('group_count')\
#                             .withColumn('futureMinPrice', min('price').over(window_minPrice))\
#                             .withColumn('nextQueryPrice', lead('price', 1).over(window_nextQueryPrice))\
#                             .dropna(how='any')\
#                             .withColumn('trend', (col('futureMinPrice')/col('nextQueryPrice')).between(0, 0.96).cast('int'))\
#                             .drop('nextQueryPrice')
#     elif is_online == True:
#         id_splits = split(col('id'), '_')
#         df_lowPrice = df_org_lowPrice_online.withColumn('fn', id_splits.getItem(0)) \
#             .withColumn('fc', substring('fn', 1, 2)) \
#             .withColumn('city_code', substring_index(substring_index(col('id'), '_', 3), '_', -2)) \
#             .withColumn('departDate', id_splits.getItem(3)) \
#             .drop('id') \
#             .withColumn('departDate', udf_confirm_departDate(col('queryDate'), col('departDate'))) \
#             .withColumn('queryDate', lit(params['yesterday_str2']))\
#             .dropna(how='any')\
#             .withColumn('futureMinPrice', lit('')) \
#             .withColumn('trend', lit(''))
#
#     id_splits = split(col('orgPrice_id'), '_')
#     window_orgPrice = Window.partitionBy('fc', 'org', 'dst').orderBy('departDate')
#     df_orgPrice = df_org_orgPrice.withColumn('org', id_splits.getItem(0))\
#                     .withColumn('dst', id_splits.getItem(1))\
#                     .withColumn('departDate', id_splits.getItem(2))\
#                     .filter(col('departDate') >= lit(datetime.today().strftime("%Y-%m-%d")))\
#                     .withColumn('rank', row_number().over(window_orgPrice))\
#                     .filter(col('rank') == 1)\
#                     .withColumn('orgPrice', col('orgPrice').cast('int'))\
#                     .drop('orgPrice_id', 'rank', 'departDate')\
#                     .dropna(how='any')
#
#     id_splits = split(col('seatLeft_id'), '_')
#     window_seatLeft = Window.partitionBy('seatLeft_id', 'queryDate').orderBy(col('queryTime').desc())
#     df_seatLeft = df_org_seatLeft_add.withColumn('queryDate', date_format('queryDatetime', "YYYY-MM-dd"))\
#                     .withColumn('queryTime', date_format('queryDatetime', "HH:mm"))\
#                     .withColumn('rank', row_number().over(window_seatLeft))\
#                     .filter(col('rank') == 1)\
#                     .withColumn('fn', id_splits.getItem(0))\
#                     .withColumn('org', id_splits.getItem(1))\
#                     .withColumn('dst', id_splits.getItem(2))\
#                     .withColumn('departDate', id_splits.getItem(3))\
#                     .drop('seatLeft_id', 'rank')\
#                     .withColumn('departDate', udf_confirm_departDate('queryDate', 'departDate'))\
#                     .withColumn('seatLeft', substring_index('seatLeft', '%', 1).cast('double'))\
#                     .drop('queryDatetime', 'queryTime')\
#                     .dropna(how='any')
#
#     id_splits = split(col('infoBase_id'), '_')
#     window_infoBase = Window.partitionBy('fn', 'org', 'dst').orderBy('departDate')
#     df_infoBase = df_org_infoBase.withColumn('fn', id_splits.getItem(2))\
#                     .withColumn('org', id_splits.getItem(0))\
#                     .withColumn('dst', id_splits.getItem(1))\
#                     .withColumn('departDate', id_splits.getItem(3))\
#                     .filter(col('departDate') >= lit(datetime.today().strftime("%Y-%m-%d")))\
#                     .withColumn('rank', row_number().over(window_infoBase))\
#                     .filter(col('rank') == 1)\
#                     .withColumn('isShare', when(col('isShare') == 'True', 1).otherwise(0))\
#                     .drop("infoBase_id", "rank", "departDate")\
#                     .dropna(how='any')
#
#     df = df_lowPrice.join(df_org_Airport, df_lowPrice.org == df_org_Airport.Airport_code, 'inner') \
#         .drop('Airport_code') \
#         .withColumnRenamed("longitude", "org_longitude") \
#         .withColumnRenamed("latitude", "org_latitude") \
#         .join(df_org_Airport, df_lowPrice.dst == df_org_Airport.Airport_code, 'inner') \
#         .drop('Airport_code') \
#         .withColumnRenamed("longitude", "dst_longitude") \
#         .withColumnRenamed("latitude", "dst_latitude") \
#         .dropna(how='any', subset=["org_longitude", "org_latitude", "dst_longitude", "dst_latitude"])\
#         .withColumn("distance",
#                     udf_calculate_distance("org_longitude", "org_latitude", "dst_longitude", "dst_latitude"))\
#         .join(df_orgPrice, ['fc', 'org', 'dst'], 'inner') \
#         .withColumn('discount', round(col('price') / col('orgPrice'), 2)) \
#         .join(df_seatLeft, ["fn", "org", "dst", "departDate", "queryDate"], "left") \
#         .join(df_infoBase, ["fn", "org", "dst"], "inner")
#     return df










# def merge_data(df_org_lowPrice, df_org_orgPrice, df_org_Airport, df_org_seatLeft, df_org_infoBase, params):
#     def confirm_departDate(x, y):  # x表示queryDate， y表示departMonthDay
#         if x[5:] > y:
#             return str(int(x[:4]) + 1) + '-' + y
#         else:
#             return x[:4] + '-' + y
#
#     def calculate_distance(org_longitude, org_latitude, dst_longitude, dst_latitude):
#         # 将十进制度数转化为弧度
#         lon1, lat1, lon2, lat2 = map(math.radians, [org_longitude, org_latitude, dst_longitude, dst_latitude])
#         # haversine公式
#         dlon = lon2 - lon1
#         dlat = lat2 - lat1
#         a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
#         c = 2 * math.asin(math.sqrt(a))
#         r = 6371  # 地球平均半径，单位为公里
#         # 返回结果以公里为单位向上取整
#         distance = math.ceil((c * r * 1000) / 1000)
#         return distance
#
#     udf_confirm_departDate = udf(confirm_departDate, StringType())
#     udf_calculate_distance = udf(calculate_distance, DoubleType())
#
#     id_splits = split(col('id'), '_')
#     window_groupCount = Window.partitionBy('fn', 'city_code', 'departDate').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
#     #划窗边界为：当前日期的下一天至最后一天
#     window_minPrice = Window.partitionBy('fn', 'city_code', 'departDate').orderBy('queryDate').rowsBetween(2, Window.unboundedFollowing)
#     window_nextQueryPrice = Window.partitionBy('fn', 'city_code', 'departDate').orderBy('queryDate')
#     df_lowPrice = df_org_lowPrice.withColumn('fn', id_splits.getItem(0))\
#                         .withColumn('fc', substring('fn', 1, 2))\
#                         .withColumn('city_code', substring_index(substring_index(col('id'), '_', 3), '_', -2))\
#                         .withColumn('departDate', id_splits.getItem(3))\
#                         .drop('id')\
#                         .withColumn('departDate', udf_confirm_departDate(col('queryDate'), col('departDate')))\
#                         .filter(col('departDate') < params["generateDate_str2"])\
#                         .filter(col('queryDate') >= date_add(col('departDate'), -30).cast('string'))\
#                         .withColumn('group_count', count('price').over(window_groupCount))\
#                         .filter(col('group_count') >= 15)\
#                         .drop('group_count')\
#                         .withColumn('futureMinPrice', min('price').over(window_minPrice))\
#                         .withColumn('nextQueryPrice', lead('price', 1).over(window_nextQueryPrice))\
#                         .dropna(how='any')\
#                         .withColumn('trend', (col('futureMinPrice')/col('nextQueryPrice')).between(0, 0.96).cast('int'))\
#                         .drop('nextQueryPrice')
#
#     id_splits = split(col('orgPrice_id'), '_')
#     window_orgPrice = Window.partitionBy('fc', 'org', 'dst').orderBy('departDate')
#     df_orgPrice = df_org_orgPrice.withColumn('org', id_splits.getItem(0))\
#                     .withColumn('dst', id_splits.getItem(1))\
#                     .withColumn('dst', id_splits.getItem(1))\
#                     .withColumn('departDate', id_splits.getItem(2))\
#                     .filter(col('departDate') >= lit(datetime.today().strftime("%Y-%m-%d")))\
#                     .withColumn('rank', row_number().over(window_orgPrice))\
#                     .filter(col('rank') == 1)\
#                     .withColumn('orgPrice', col('orgPrice').cast('int'))\
#                     .drop('orgPrice_id', 'rank', 'departDate')\
#                     .dropna(how='any')
#
#     id_splits = split(col('seatLeft_id'), '_')
#     window_seatLeft = Window.partitionBy('seatLeft_id', 'queryDate').orderBy(col('queryTime').desc())
#     df_seatLeft = df_org_seatLeft.withColumn('queryDate', date_format('queryDatetime', "YYYY-MM-dd"))\
#                     .withColumn('queryTime', date_format('queryDatetime', "HH:mm"))\
#                     .withColumn('rank', row_number().over(window_seatLeft))\
#                     .filter(col('rank') == 1)\
#                     .withColumn('fn', id_splits.getItem(0))\
#                     .withColumn('org', id_splits.getItem(1))\
#                     .withColumn('dst', id_splits.getItem(2))\
#                     .withColumn('departDate', id_splits.getItem(3))\
#                     .drop('seatLeft_id', 'rank')\
#                     .withColumn('departDate', udf_confirm_departDate('queryDate', 'departDate'))\
#                     .withColumn('seatLeft', substring_index('seatLeft', '%', 1).cast('double'))\
#                     .drop('queryDatetime', 'queryTime')\
#                     .dropna(how='any')
#
#     id_splits = split(col('infoBase_id'), '_')
#     window_infoBase = Window.partitionBy('fn', 'org', 'dst').orderBy('departDate')
#     df_infoBase = df_org_infoBase.withColumn('fn', id_splits.getItem(2))\
#                     .withColumn('org', id_splits.getItem(0))\
#                     .withColumn('dst', id_splits.getItem(1))\
#                     .withColumn('departDate', id_splits.getItem(3))\
#                     .filter(col('departDate') >= lit(datetime.today().strftime("%Y-%m-%d")))\
#                     .withColumn('rank', row_number().over(window_infoBase))\
#                     .filter(col('rank') == 1)\
#                     .withColumn('isShare', col('isShare').cast('int'))\
#                     .drop("infoBase_id", "rank", "departDate")\
#                     .dropna(how='any')
#
#     df_org_train_add = df_lowPrice.join(df_org_Airport, df_lowPrice.org == df_org_Airport.Airport_code, 'inner') \
#         .drop('Airport_code') \
#         .withColumnRenamed("longitude", "org_longitude") \
#         .withColumnRenamed("latitude", "org_latitude") \
#         .join(df_org_Airport, df_lowPrice.org == df_org_Airport.Airport_code, 'inner') \
#         .drop('Airport_code') \
#         .withColumnRenamed("longitude", "dst_longitude") \
#         .withColumnRenamed("latitude", "dst_latitude") \
#         .withColumn("distance",
#                     udf_calculate_distance("org_longitude", "org_latitude", "dst_longitude", "dst_latitude")) \
#         .join(df_orgPrice, ['fc', 'org', 'dst'], 'inner') \
#         .withColumn('discount', round(col('price') / col('orgPrice'), 2)) \
#         .join(df_seatLeft, ["fn", "org", "dst", "departDate", "queryDate"], "left") \
#         .join(df_infoBase, ["fn", "org", "dst"], "inner")
#
#     df_org_train_add.write.format('csv').save(params["sparkDirName_org_trainData_add"], header=True, mode="overwrite")
#     logger.info("====\"{}\" finished====".format(params["sparkDirName_org_trainData_add"].split('/')[-1]))
#     utils.delete_before4_sparkData(params["sparkDirName_org_trainData_add"], params)
#     return df_org_train_add


def data_preprocess(df_org_train_yesterday, df_org_train_add, df_org_online, df_holiday, df_day_of_vacation_future1, df_day_of_vacation_pre1):
    # 每天增加训练数据，并更新
    df_org_train_union = df_org_train_yesterday.unionByName(df_org_train_add)
    df_org_train_union.cache()
    df_org_train_union.write.format('csv').save(params["sparkHost"] + params["sparkDirName_org_trainData_union"],
                                                header=True, mode='overwrite')
    utils.delete_before4_sparkData(params["sparkDirName_org_trainData_union"], params)

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
        .withColumn('depart_dayofweek', datediff('departDate', lit('2017-01-09')) % 7 + 1) \
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
    df_org_train_union.unpersist()
    return df_all


def write_to_HDFS(df_all, params):

    #起飞时间大于generateDate为需要预测的线上数据，起飞时间小于generateDate为训练数据，起飞时间等于generateDate的数据不需要保留
    #从所有数据中随机抽取约4千万的数据，作为训练集
    df_train_DNN = df_all.filter(df_all.departDate < params["generateDate_str2"])\
                     .drop(*list(set(params['dropFeatures'])-set(params['baseColumns'])))\
                     .sample(False, float("%.4f" % (6e7/df_all.count())))\
                     .orderBy(rand())

    # df_train_DNN = df_all.filter(df_all.departDate < params["generateDate_str2"]) \
    #     .drop(*list(set(params['dropFeatures']) - set(params['baseColumns']))) \
    #     .orderBy(rand())

    # df_train_DNN.cache()

    # logger.info('df_all count is: {}'.format(df_all.count()))
    # logger.info('df_train_DNN count is: {}'.format(df_train_DNN.count()))

    df_train_DNN.repartition(200).write.format('parquet').save(params["sparkHost"] + params["sparkDirName_trainData"], mode='overwrite')
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
    df_org_lowPrice_add, df_org_orgPrice, df_org_Airport, df_org_seatLeft_add, df_org_infoBase, df_org_train_yesterday, \
    df_org_lowPrice_online, df_holiday, df_day_of_vacation_future1, df_day_of_vacation_pre1 = load_data(params)
    df_orgPrice, df_seatLeft_add, df_infoBase = cache_data(df_org_orgPrice, df_org_seatLeft_add, df_org_infoBase)
    df_orgPrice.cache()
    df_seatLeft_add.cache()
    df_infoBase.cache()
    df_org_train_add = merge_data(df_org_lowPrice_add, df_org_Airport, df_org_lowPrice_online, df_orgPrice,
                                  df_seatLeft_add, df_infoBase, params, is_online=False)
    df_org_online = merge_data(df_org_lowPrice_add, df_org_Airport, df_org_lowPrice_online, df_orgPrice,
                                  df_seatLeft_add, df_infoBase, params, is_online=True)
    df_orgPrice.unpersist()
    df_seatLeft_add.unpersist()
    df_infoBase.unpersist()
    df_all = data_preprocess(df_org_train_yesterday, df_org_train_add, df_org_online, df_holiday,
                             df_day_of_vacation_future1, df_day_of_vacation_pre1)
    df_all.cache()
    write_to_HDFS(df_all, params)
    write_deepFM_featureDict(df_all, params)














