#!/usr/bin/python
# coding:utf-8
# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession,Row,Window
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.ml.regression import RandomForestRegressor,LinearRegression
from pyspark.ml.feature import VectorIndexer,StringIndexer,VectorAssembler,OneHotEncoder,StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
from datetime import datetime
from pyspark.ml import Pipeline, PipelineModel
from hdfs3 import HDFileSystem

mongodb_url = "mongodb://search:search%40huoli123@123.56.222.127/flight.sparkTest"
spark =SparkSession.builder \
                   .config("spark.mongodb.input.uri", mongodb_url) \
                   .config("spark.mongodb.output.uri", mongodb_url) \
                   .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.0") \
                   .appName("pricePredict")\
                   .getOrCreate()
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
       # fileName = "intlFline_pricePredict_org_data_" + todayStr1 + ".csv"
       # df_org = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/' + fileName,header=True)
       df_org = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/intlFline_pricePredict_org_data_20190621.csv', header=True)\
                     .filter(col('price') > 0)
       df_holidayList = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/holiday_list.csv',header=True)\
                             .select(concat(substring('calday',1,4), lit('-'), substring('calday',5,2), lit('-'), substring('calday',7,2)).alias('dateList'),\
                                     'is_vacation', 'vacation_days', 'day_of_vacation')
       df_holidayTOdate = spark.read.csv('hdfs://10.0.1.218:9000/predict-2019/holiday_to_date.csv',header=True)
       return df_org, df_holidayList, df_holidayTOdate

def org_features(df_org, df_holidayList, df_holidayTOdate):
       df = df_org.withColumn('departDate_monthDay_tmp', substring_index(df_org.flineId, '_', -1))\
                  .withColumn('predictDate_year_tmp', substring_index(df_org.predictDate, '-', 1))\
                  .withColumn('predictDate_monthDay_tmp', substring_index(df_org.predictDate, '-', -2))
       """
       起飞日期判断逻辑：如果起飞日期的departDate_monthDay_tmp大于predictDate_monthDay_tmp，起飞的年份就是原始数据中predictDate的年份，\
                      如果起飞日期的departDate_monthDay_tmp小于predictDate_monthDay_tmp，起飞的年份就是原始数据中predictDate的年份+1
       """
       df = df.withColumn('departDate_tmp', when(df.departDate_monthDay_tmp>df.predictDate_monthDay_tmp, concat(df.predictDate_year_tmp,lit('-'),df.departDate_monthDay_tmp))\
              .otherwise(concat(substring_index((df.predictDate_year_tmp+1).cast('string'), '.', 1), lit('-'), df.departDate_monthDay_tmp)))\
              .drop('departDate_monthDay_tmp','predictDate_year_tmp','predictDate_monthDay_tmp')
       """
       原始数据flineId中含有'2018-Spring-8'样式，利用df_holidayTOdate将其转化成具体的日期
       """
       df = df.join(df_holidayTOdate, df.departDate_tmp==df_holidayTOdate.dateDesc, 'left')\
              .withColumn('departDate', when(df_holidayTOdate.date.isNull(), df.departDate_tmp).otherwise(df_holidayTOdate.date))\
              .drop('dateDesc','departDate_tmp','date')
       df = df.join(df_holidayList, df.departDate==df_holidayList.dateList, 'left')\
              .drop('dateList')\
              .na.fill({'is_vacation':'no', 'vacation_days':0, 'day_of_vacation':0})\
              ##打开房间卡的积分的框架来告诉大概
              .withColumn('flineId_noDate', substring_index(df_org.flineId, '_', 5))\
       ##金大福跨境电商
              .withColumn('org', split(df_org.flineId, '_').getItem(0))\
              .withColumn('dst', split(df_org.flineId, '_').getItem(1))\
              .withColumn('isReturn_type', split(df_org.flineId, '_').getItem(2))\
              .withColumn('cabin', split(df_org.flineId, '_').getItem(3))\
              .withColumn('isDirect_type', split(df_org.flineId, '_').getItem(4))\
              .withColumn('departDate', to_date('departDate'))\
              .withColumn('predictDate', to_date('predictDate'))\
              .withColumn('price', df.price.cast('int'))\
              .filter("cabin == 'Y'")\
              .drop('flineId', 'cabin')\
              .dropna(how='any')
       return df

def build_features(df_org):
       df_buildFeatures = df_org.withColumn('departYear', year('departDate'))\
                                 .withColumn('departMonth', month('departDate'))\
                                 .withColumn('depart_dayofmonth', dayofmonth('departDate'))\
                                 .withColumn('depart_weekofyear', weekofyear('departDate'))\
                                 .withColumn('depart_dayofweek', datediff('departDate', to_date(lit('2017-01-09')))%7 + 1)\
                                 .withColumn('departQuarter', quarter('departDate'))\
                                 .withColumn('intervalDays', datediff('departDate', 'predictDate'))\
                                 .withColumn('intervalMonths', round(months_between('departDate', 'predictDate')))
       df_buildFeatures = df_buildFeatures.withColumn('intervalWeeks', round(df_buildFeatures.intervalDays/7))\
                                            .withColumn('intervalQuarters', round(df_buildFeatures.intervalDays/120))
       """
       如果某个分区内只有一个样本，rowsBetween(-4,-1)会造成Null，用当前样本的price补充Null
       """
       window = Window.partitionBy('flineId_noDate', 'intervalDays').orderBy('departDate').rowsBetween(-4, -1)
       df_buildFeatures = df_buildFeatures.withColumn('preceding3day_price', coalesce(avg('price').over(window), 'price'))
       return df_buildFeatures

def lr_history_data(df_buildFeatures):
       categary_features = ['org', 'dst', 'isReturn_type', 'isDirect_type', 'departYear', 'is_vacation', 'vacation_days', 'day_of_vacation']
       stringIndexer_stages = []
       onehotEncoder_stages = []
       for cateIndexer in categary_features:
              stringIndexer = StringIndexer(inputCol=cateIndexer, outputCol='stringIndexer_'+cateIndexer)
              onehotEncoder = OneHotEncoder(inputCol=stringIndexer.getOutputCol(), outputCol='onehotEncoder_'+cateIndexer, dropLast=False)
              stringIndexer_stages.append(stringIndexer)
              onehotEncoder_stages.append(onehotEncoder)
       scaler_features = ['departMonth', 'depart_dayofmonth', 'depart_weekofyear', 'depart_dayofweek', 'departQuarter',\
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
       if hdfs.exists('/predict-2019/pipelineModel_' + todayStr1):
              hdfs.rm('/predict-2019/pipelineModel_' + todayStr1)
       pipelineModel.save('hdfs://10.0.1.218:9000/predict-2019/pipelineModel_' + todayStr1)
       lr_historyData = pipelineModel.transform(df_buildFeatures)\
                              .select('predictDate', 'departDate', 'flineId_noDate', 'price', 'features')
       # if hdfs.exists('/predict-2019/lr_data_20190624.parquet'):
       #        hdfs.rm('/predict-2019/lr_data_20190624.parquet')
       # lr_data.write.save('hdfs://10.0.1.218:9000/predict-2019/lr_data_20190624.parquet')
       return lr_historyData

def train_val_data(df):
       train_data = df.filter("departDate > '2019-04-01'")
       val_data = df.filter("departDate <= '2019-04-01'")
       return train_data, val_data

def lr_trainModel(train_data, val_data):
       lr = LinearRegression(maxIter=100, standardization=False, featuresCol="features", labelCol="price", predictionCol="prediction")
       lrModel = lr.fit(train_data)
       pred = lrModel.transform(val_data)
       if hdfs.exist('hdfs://10.0.1.218:9000/predict-2019/pred_result_20190624.parquet'):
              hdfs.rm('hdfs://10.0.1.218:9000/predict-2019/pred_result_20190624.parquet')
       # pred.write.save('hdfs://10.0.1.218:9000/predict-2019/pred_result_20190624.parquet')
       mseError_DF = pred.select((sum(abs(pred.prediction-pred.price)/pred.price) / pred.count()).alias('MSE'))
       return mseError_DF

"""
每天凌晨开始生成下一天需要预测的数据，预测的日期从'今天'开始
"""
def org_predictData(df_buildFeatures, df_holidayList):
       df_predictData = df_buildFeatures.filter(df_buildFeatures.departDate > to_date(lit(todayStr2))) \
              .select('departDate', 'flineId_noDate') \
              .distinct()\
              .withColumn('todayStr2', lit(todayStr2))
       df_predictData.createOrReplaceTempView('predict_data')
       df_predictData = spark.sql("select t.flineId_noDate,t.departDate,date_add(todayStr2,p.k) as predictDate from predict_data t\
                                  lateral view posexplode(split(space(datediff(t.departDate,todayStr2)),'')) p as k,v")
       df_predictData = df_predictData.join(df_holidayList, df_predictData.departDate == df_holidayList.dateList, 'left') \
                                      .drop('dateList') \
                                      .na.fill({'is_vacation': 'no', 'vacation_days': 0, 'day_of_vacation': 0}) \
                                      .withColumn('org', split(df_predictData.flineId_noDate, '_').getItem(0)) \
                                      .withColumn('dst', split(df_predictData.flineId_noDate, '_').getItem(1)) \
                                      .withColumn('isReturn_type', split(df_predictData.flineId_noDate, '_').getItem(2)) \
                                      .withColumn('isDirect_type', split(df_predictData.flineId_noDate, '_').getItem(4)) \
                                      .withColumn('departYear', year('departDate')) \
                                      .withColumn('departMonth', month('departDate')) \
                                      .withColumn('depart_dayofmonth', dayofmonth('departDate')) \
                                      .withColumn('depart_weekofyear', weekofyear('departDate')) \
                                      .withColumn('depart_dayofweek', datediff('departDate', to_date(lit('2017-01-09'))) % 7 + 1) \
                                      .withColumn('departQuarter', quarter('departDate')) \
                                      .withColumn('intervalDays', datediff('departDate', 'predictDate')) \
                                      .withColumn('intervalMonths', round(months_between('departDate', 'predictDate')))
       df_predictData = df_predictData.withColumn('intervalWeeks', round(df_predictData.intervalDays / 7)) \
                                      .withColumn('intervalQuarters', round(df_predictData.intervalDays / 120))
       """
       预测数据集中preceding3day_price处理逻辑：将现有数据partitionBy('flineId_noDate', 'intervalDays').orderBy(desc('departDate'))进行分区排序，\
                                             并取最新的3个价格的均值，作为预测数据集相对应的'flineId_noDate', 'intervalDays'的参考价格数据
       """
       window = Window.partitionBy('flineId_noDate', 'intervalDays').orderBy(desc('departDate'))
       df_preceding3day_priceFeatures = df_buildFeatures.withColumn('rank', row_number().over(window)) \
                                                        .filter("rank <= 3") \
                                                        .groupBy('flineId_noDate', 'intervalDays') \
                                                        .agg(avg('price').alias('preceding3day_price'))
       df_predictData = df_predictData.join(df_preceding3day_priceFeatures, ['flineId_noDate', 'intervalDays'], 'left')\
                                      .dropna(how='any')
       return df_predictData


def lr_predictData(df_org_predictData):
       pipelineModel = PipelineModel.load('hdfs://10.0.1.218:9000/predict-2019/pipelineModel_' + todayStr1)
       df_lr_predictData = pipelineModel.transform(df_org_predictData) \
              .select('predictDate', 'departDate', 'flineId_noDate', 'features')
       # if hdfs.exists('hdfs://10.0.1.218:9000/predict-2019/predict_data_20190624.parquet'):
       #        hdfs.rm('hdfs://10.0.1.218:9000/predict-2019/predict_data_20190624.parquet')
       # df_lr_predictDate.write.save('hdfs://10.0.1.218:9000/predict-2019/predict_data_20190624.parquet')
       return df_lr_predictData

def lr_predict_result(lr_historyData, lr_predictData):
       lr = LinearRegression(maxIter=100, standardization=False, featuresCol="features", labelCol="price", predictionCol="prediction")
       lrModel = lr.fit(lr_historyData)
       pred = lrModel.transform(lr_predictData)
       pred = pred.select('predictDate', 'departDate', 'flineId_noDate', 'prediction')
       return pred

def sparkTOmongo_data(predictResult):
       window = Window.partitionBy('flineId_noDate', 'departDate').orderBy('predictDate')
       mongoData = predictResult.groupBy('flineId_noDate', 'departDate')\
                                .agg(concat('flineId_noDate', lit('_'), col('departDate').cast('string')).alias('_id'), \
                                     array(collect_list('predictDate'), collect_list('prediction')).alias('content'))\
                                .drop('flineId_noDate', 'departDate')
       mongData.write.format('com.mongodb.spark.sql.DefaultSource').mode("overwrite").save()


if __name__ == "__main__":
       todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
       todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
       hdfs = HDFileSystem(host='10.0.1.218', port=9000)
       # df_org_features = org_features(df_org, df_holidayList, df_holidayTOdate)
       df_org, df_holidayList, df_holidayTOdate = spark_readData()
       print('read data is OK!!!')
       df_orgFeatures = org_features(df_org, df_holidayList, df_holidayTOdate)
       print('df_orgFeatures is OK!!!')
       df_buildFeatures = build_features(df_orgFeatures)
       print('df_buildFeatures is OK!!!')
       lr_historyData = lr_history_data(df_buildFeatures)
       print('lr_historyData is OK!!!')
       train_data, val_data = train_val_data(lr_historyData)
       print("train_data and val_data is OK!!!")
       # mseError_DF = lr_trainModel(train_data, val_data)
       # mseError_DF.show()
       # print('LR_Model is OK!!!')
       org_predictData = org_predictData(df_buildFeatures, df_holidayList)
       print("org_predictData is OK!!!")
       lr_predictData = lr_predictData(org_predictData)
       if hdfs.exists('hdfs://10.0.1.218:9000/predict-2019/predict_data_20190624.parquet'):
              hdfs.rm('hdfs://10.0.1.218:9000/predict-2019/predict_data_20190624.parquet')
       lr_predictData.write.format('parquet').save('hdfs://10.0.1.218:9000/predict-2019/predict_data_20190624.parquet')
       print("lr_predictData is OK!!!")





