# coding:utf-8
# -*- coding:utf-8 -*-
"""

"""

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

mongodb_url = "mongodb://search:search%40huoli123@10.0.1.212/flight.sparkTest"
spark =SparkSession.builder \
                   .config("spark.mongodb.input.uri", mongodb_url) \
                   .config("spark.mongodb.output.uri", mongodb_url) \
                   .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.1.1") \
                   .appName("intlFline_spark_feature")\
                   .getOrCreate()
sc = spark.sparkContext
# #本地测试数据
# df_org = spark.read.parquet('/Users/changfu/Desktop/test_10000_data.parquet')\
#               .select('flightline_id','check_date','price')\
#               .withColumnRenamed('flightline_id', 'flineId')\
#               .withColumnRenamed('check_date', 'predictDate')
# df_holidayList  = spark.read.csv('/Users/changfu/Desktop/time_base（2019）.csv',header=True)\
#                        .select(concat(substring('calday',1,4), lit('-'), substring('calday',5,2), lit('-'), substring('calday',7,2)).alias('dateList'),\
#                                'is_vacation', 'vacation_days', 'day_of_vacation')
# df_holidayTOdate = spark.read.csv('/Users/changfu/Desktop/df_holiday.csv',header=True)


def spark_readData():
       fileName = "intlFline_pricePredict_org_data_" + todayStr1 + ".csv"
       df_org = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/' + fileName, header=True).filter(col('price') > 0)
       # df_org = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/intlFline_pricePredict_org_data_20190621.csv', header=True)\
       #               .filter(col('price') > 0)
       df_holidayList = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/holiday_list.csv', header=True)\
                             .select(concat(substring('calday',1,4), lit('-'), substring('calday',5,2), lit('-'), substring('calday',7,2)).alias('dateList'),\
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
       df_orgFeatures = df_org.withColumn('departDate_monthDay_tmp', substring_index(df_org.flineId, '_', -1))\
                              .withColumn('predictDate_year_tmp', substring_index(df_org.predictDate, '-', 1))\
                              .withColumn('predictDate_monthDay_tmp', substring_index(df_org.predictDate, '-', -2)) \
                              .withColumn('departDate_tmp', when(col('departDate_monthDay_tmp') > col('predictDate_monthDay_tmp'),
                                                                 concat(col('predictDate_year_tmp'), lit('-'), col('departDate_monthDay_tmp')))
                                          .otherwise(concat(substring_index((col('predictDate_year_tmp') + 1).cast('string'), '.', 1),
                                                            lit('-'), col('departDate_monthDay_tmp'))))\
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


def build_features(df_orgFeatures):
       """
       如果某个分区内只有一个样本，rowsBetween(-4,-1)会造成Null，用当前样本的price补充Null
       """
       window = Window.partitionBy('flineId_noDate', 'intervalDays').orderBy('departDate').rowsBetween(-4, -1)
       df_buildFeatures = df_orgFeatures.withColumn('departYear', year('departDate'))\
                                     .withColumn('departMonth', month('departDate'))\
                                     .withColumn('depart_dayofmonth', dayofmonth('departDate'))\
                                     .withColumn('depart_weekofyear', weekofyear('departDate'))\
                                     .withColumn('depart_dayofweek', datediff('departDate', to_date(lit('2017-01-09')))%7 + 1)\
                                     .withColumn('departQuarter', quarter('departDate'))\
                                     .withColumn('intervalDays', datediff('departDate', 'predictDate'))\
                                     .withColumn('intervalMonths', round(months_between('departDate', 'predictDate')))\
                                     .withColumn('intervalWeeks', round(col('intervalDays')/7))\
                                     .withColumn('intervalQuarters', round(col('intervalDays')/120))\
                                     .withColumn('preceding3day_price', coalesce(avg('price').over(window), 'price'))
       return df_buildFeatures


def oneHot_history_data(df_buildFeatures):
       categary_features = ['org', 'dst', 'isReturn_type', 'isDirect_type', 'departYear', 'is_vacation', 'vacation_days', 'day_of_vacation']
       stringIndexer_stages = []
       onehotEncoder_stages = []
       for cateIndexer in categary_features:
              stringIndexer = StringIndexer(inputCol=cateIndexer, outputCol='stringIndexer_'+cateIndexer)
              onehotEncoder = OneHotEncoder(inputCol=stringIndexer.getOutputCol(), outputCol='onehotEncoder_'+cateIndexer, dropLast=False)
              stringIndexer_stages.append(stringIndexer)
              onehotEncoder_stages.append(onehotEncoder)
       scaler_features = ['departMonth', 'depart_dayofmonth', 'depart_weekofyear', 'depart_dayofweek', 'departQuarter',
                          'intervalDays', 'intervalMonths', 'intervalWeeks', 'intervalQuarters', 'preceding3day_price']
       scaler_assembler = VectorAssembler(inputCols=scaler_features, outputCol='scalerAssembler')
       scaler = StandardScaler(inputCol='scalerAssembler', outputCol='scalerFeatures', withMean=True, withStd=True)
       features = []
       for cateIndexer in categary_features:
              features.append(("onehotEncoder_"+cateIndexer))
       features.append('scalerFeatures')
       featrues_assembler = VectorAssembler(inputCols=features, outputCol='features')
       stages = stringIndexer_stages + onehotEncoder_stages + [scaler_assembler, scaler, featrues_assembler]
       pipeline = Pipeline(stages=stages)
       pipelineModel = pipeline.fit(df_buildFeatures)
       sparkDirName = 'pipelineModel_lr_' + todayStr1
       if sparkDirName in clientHdfs.list('/predict-2019/'):
           clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
       pipelineModel.save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
       oneHot_historyData = pipelineModel.transform(df_buildFeatures)\
                              .select('predictDate', 'departDate', 'flineId_noDate', 'price', 'features')
       return oneHot_historyData


def org_predictData(df_buildFeatures, df_holidayList):
       """
       每天凌晨开始生成下一天需要预测的数据，预测的日期从'今天'开始；
       预测数据集中preceding3day_price处理逻辑：将现有数据partitionBy('flineId_noDate', 'intervalDays').orderBy(desc('departDate'))进行分区排序，\
                                            并取最新的3个价格的均值，作为预测数据集相对应的('flineId_noDate', 'intervalDays')的参考价格数据
       """
       window = Window.partitionBy('flineId_noDate', 'intervalDays').orderBy(desc('departDate'))
       df_preceding3day_priceFeatures = df_buildFeatures.withColumn('rank', row_number().over(window)) \
              .filter("rank <= 3") \
              .groupBy('flineId_noDate', 'intervalDays') \
              .agg(avg('price').alias('preceding3day_price'))
       df_predictData = df_buildFeatures.filter((col('departDate') >= to_date(lit(todayStr2))) & (col('departDate') <= date_add(to_date(lit(todayStr2)), 90))) \
              .select('departDate', 'flineId_noDate') \
              .distinct()\
              .withColumn('todayStr2', lit(todayStr2))
       df_predictData.createOrReplaceTempView('predict_data')
       df_predictData = spark.sql("select t.flineId_noDate,t.departDate,date_add(date_add(todayStr2, 1), p.k) as predictDate from predict_data t\
                                  lateral view posexplode(split(space(datediff(t.departDate,date_add(todayStr2,1))),'')) p as k,v")\
                             .join(df_holidayList, col('departDate') == col('dateList'), 'left') \
                             .drop('dateList') \
                             .na.fill({'is_vacation': 'no', 'vacation_days': 0, 'day_of_vacation': 0}) \
                             .withColumn('org', split(col('flineId_noDate'), '_').getItem(0)) \
                             .withColumn('dst', split(col('flineId_noDate'), '_').getItem(1)) \
                             .withColumn('isReturn_type', split(col('flineId_noDate'), '_').getItem(2)) \
                             .withColumn('isDirect_type', split(col('flineId_noDate'), '_').getItem(4)) \
                             .withColumn('departYear', year('departDate')) \
                             .withColumn('departMonth', month('departDate')) \
                             .withColumn('depart_dayofmonth', dayofmonth('departDate')) \
                             .withColumn('depart_weekofyear', weekofyear('departDate')) \
                             .withColumn('depart_dayofweek', datediff('departDate', to_date(lit('2017-01-09'))) % 7 + 1)\
                             .withColumn('departQuarter', quarter('departDate')) \
                             .withColumn('intervalDays', datediff('departDate', 'predictDate')) \
                             .withColumn('intervalMonths', round(months_between('departDate', 'predictDate')))\
                             .withColumn('intervalWeeks', round(col('intervalDays') / 7)) \
                             .withColumn('intervalQuarters', round(col('intervalDays') / 120))\
                             .join(df_preceding3day_priceFeatures, ['flineId_noDate', 'intervalDays'], 'left')\
                             .dropna(how='any')
                             #col('preceding3day_price')有空值，所以要删除,空缺4363499个
       return df_predictData


def oneHot_predictData(df_org_predictData):
       sparkDirName = 'pipelineModel_lr_' + todayStr1
       pipelineModel = PipelineModel.load('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
       df_oneHot_predictData = pipelineModel.transform(df_org_predictData) \
              .select('predictDate', 'departDate', 'flineId_noDate', 'features')
       return df_oneHot_predictData


if __name__ == "__main__":
       todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
       todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
       before2_dateStr1 = (datetime.now() - timedelta(days=2)).strftime('%Y%m%d')
       clientHdfs = client.InsecureClient("http://10.0.1.218:50070", user="search") #跟HDFS建立连接
       df_org, df_holidayList, df_holidayTOdate = spark_readData()
       nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       print(nowStr + '=====read data is OK!!!=====')
       df_orgFeatures = org_features(df_org, df_holidayList, df_holidayTOdate)
       nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       print(nowStr + '=====df_orgFeatures is OK!!!=====')
       df_buildFeatures = build_features(df_orgFeatures)
       # sparkDirName = 'buildFeatures_data_' + todayStr1 + '.parquet'
       # if sparkDirName in clientHdfs.list('/predict-2019/'):
       #     clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
       # df_buildFeatures.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
       nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       print(nowStr + '=====df_buildFeatures is OK!!!=====')
       oneHot_historyData = oneHot_history_data(df_buildFeatures)
       sparkDirName = 'oneHot_history_data_' + todayStr1 + '.parquet'
       if sparkDirName in clientHdfs.list('/predict-2019/'):
           clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
       oneHot_historyData.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
       sparkDirName = 'oneHot_history_data_' + before2_dateStr1 + '.parquet'
       if sparkDirName in clientHdfs.list('/predict-2019/'):
           clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
       nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       print(nowStr + '=====Finish write oneHot_historyData to HDFS=====')
       org_predictData = org_predictData(df_buildFeatures, df_holidayList)
       nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       print(nowStr + "===== Finish org_predictData Calculation =====")
       oneHot_predictData = oneHot_predictData(org_predictData)
       sparkDirName = 'oneHot_predict_data_' + todayStr1 + '.parquet'
       if sparkDirName in clientHdfs.list('/predict-2019/'):
           clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
       oneHot_predictData.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/' + sparkDirName)
       sparkDirName = 'oneHot_predict_data_' + before2_dateStr1 + '.parquet'
       if sparkDirName in clientHdfs.list('/predict-2019/'):
           clientHdfs.delete("/predict-2019/" + sparkDirName, recursive=True)
       nowStr = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       print(nowStr + "=====Finish write oneHot_predictData to HDFS=====")
       sc.stop()

