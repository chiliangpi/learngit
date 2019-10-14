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
from logConfig import log
import pickle
import os
import shutil

logger = log()
params = {"train_val_splitDate": '2019-07-25',
          "columnNames": config.columnNames,
          "dropFeatures": config.dropFeatures,
          "numericCols": config.numericCols,
          "categoryCols": config.categoryCols,
          "label": config.label,
          "featureDict_fileName": config.featureDict_fileName
          }

# spark =SparkSession.builder.master('local[*]').appName("test")\
#                    .getOrCreate()
spark =SparkSession.builder \
                   .appName("domestic_featuresProcess")\
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
end_departDate = '2019-08-19'
df_org = spark.read.format('csv').load('hdfs://10.0.4.217:8020/crawler/' + fileName, schema=schema)\
              .select('fn', 'fc', 'org', 'dst', 'departDate', 'queryDate', 'price', 'orgPrice','futureMinPrice', 'dayofweek', 'distance', 'seatLeft', 'departTime','isShare', 'trend')\
              .filter(col('departDate') <= end_departDate)
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

logger.info("====spark read data finished====")

#如果当前价格高于futureMinPrice 4%以上，视为未来降价，trend=1， 其余情况trend=0
df = df_org.dropna(how='any', subset=['orgPrice', 'distance', 'departTime', 'isShare', 'price', 'futureMinPrice'])\
    .filter(col('price').cast('double') < 100000)\
    .withColumn('trend', when((col('price')-col('futureMinPrice'))/col('price') > 0.04, 1.0).otherwise(0.0))\
    .withColumn('discountOff', round((1-col('price')/col('orgPrice')) * 100, 2))\
    .withColumn('distance', col('distance').cast('double'))\
    .na.fill({'seatLeft': '-999'})\
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
    .join(df_day_of_vacation_future1, df_org.departDate==df_day_of_vacation_future1.calday_future1, 'left')\
    .drop('calday_future1')\
    .withColumnRenamed('day_of_vacation_future1', 'day_of_vacation_future1_departDate')\
    .join(df_day_of_vacation_pre1, df_org.departDate == df_day_of_vacation_pre1.calday_pre1, 'left') \
    .drop('calday_pre1') \
    .withColumnRenamed('day_of_vacation_pre1', 'day_of_vacation_pre1_departDate') \
    .withColumn('depart_dayofvacation_extend', coalesce('depart_dayofvacation', 'day_of_vacation_future1_departDate', 'day_of_vacation_pre1_departDate').cast('int'))\
    .drop('day_of_vacation_future1_departDate', 'day_of_vacation_pre1_departDate')\
    .na.fill({'depart_vacationDays': '-999', 'depart_dayofvacation': '-999', 'depart_dayofvacation_extend': '-999', 'depart_festival': '-999'})\
    .withColumn('depart_vacationDays', col('depart_vacationDays').cast('int'))\
    .withColumn('depart_dayofvacation', col('depart_dayofvacation').cast('int'))\
    .withColumn('departYear', year('departDate')) \
    .withColumn('departMonth', month('departDate')) \
    .withColumn('depart_dayofmonth', dayofmonth('departDate')) \
    .withColumn('depart_weekofyear', weekofyear('departDate')) \
    .withColumn('depart_dayofweek', col('dayofweek').cast('int')) \
    .withColumn('depart_dayofweek_bucket', when(col('depart_dayofweek') < 5, 0).otherwise(col('depart_dayofweek')))\
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
    .join(df_day_of_vacation_future1, df_org.queryDate==df_day_of_vacation_future1.calday_future1, 'left')\
    .drop('calday_future1')\
    .withColumnRenamed('day_of_vacation_future1', 'day_of_vacation_future1_queryDate')\
    .join(df_day_of_vacation_pre1, df_org.queryDate==df_day_of_vacation_pre1.calday_pre1, 'left')\
    .drop('calday_pre1')\
    .withColumnRenamed('day_of_vacation_pre1', 'day_of_vacation_pre1_queryDate') \
    .withColumn('query_dayofvacation_extend', coalesce('query_dayofvacation', 'day_of_vacation_future1_queryDate', 'day_of_vacation_pre1_queryDate').cast('int'))\
    .drop('day_of_vacation_future1_queryDate', 'day_of_vacation_pre1_queryDate')\
    .na.fill({'query_vacationDays': '-999', 'query_dayofvacation': '-999', 'query_dayofvacation_extend': '-999', 'query_festival': '-999'})\
    .withColumn('query_vacationDays', col('query_vacationDays').cast('int'))\
    .withColumn('query_dayofvacation', col('query_dayofvacation').cast('int'))\
    .withColumn('queryYear', year('queryDate')) \
    .withColumn('queryMonth', month('queryDate')) \
    .withColumn('query_dayofmonth', dayofmonth('queryDate')) \
    .withColumn('query_weekofyear', weekofyear('queryDate')) \
    .withColumn('query_dayofweek', datediff('queryDate', to_date(lit('2017-01-09')))%7 + 1)\
    .withColumn('query_dayofweek_bucket', when(col('query_dayofweek') < 5, 0).otherwise(col('query_dayofweek')))\
    .withColumn('queryQuarter', quarter('queryDate'))

logger.info("====spark calculation finished====")

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

logger.info("====spark Pipeline finished====")

for colName in stringIndexerCols:
    df = df.withColumn('stringIndexer_%s' %colName, col('stringIndexer_%s' %colName).cast('double'))

df_shuffled = df.orderBy(rand())
df_shuffled.cache()


def write_featureDict(df_shuffled, params):
    df = df_shuffled.drop(*params["dropFeatures"]).drop(params['label'])
    featureDict = {}
    tc = 0
    for colName in df.columns:
        if colName in params["numericCols"]:
            continue
        else:  # colName in categoryCols
            uniqueFeature = df.select(colName).distinct().toPandas()[colName].astype('float').values
            featureDict[colName] = dict(zip(uniqueFeature, range(tc, len(uniqueFeature) + tc)))
            tc = tc + len(uniqueFeature)
    if os.path.exists(params["featureDict_fileName"]):
        os.remove(params["featureDict_fileName"])
    with open(params["featureDict_fileName"], 'wb') as f:
        pickle.dump(featureDict, f)


write_featureDict(df_shuffled, params)
logger.info("====featureDict finished====")


df_train_DNN = df_shuffled.filter(df_shuffled.departDate < params["train_val_splitDate"])
df_val_DNN = df_shuffled.filter(df_shuffled.departDate >= params["train_val_splitDate"])
df_val_DNN.cache()
df_train_DNN_sample = df_train_DNN.sample(False, float("%.4f" % (1.5e5/df_train_DNN.count())))
# df_val_DNN_sample = df_val_DNN.sample(False, float("%.4f" % (2e5/df_val_DNN.count())))
todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
featureData_fileName = 'domestic_shuffled_featureData_' + todayStr1 +'.parquet'
DNN_trainData_fileName = 'domestic_DNN_trainData_' + todayStr1 + '.parquet'
DNN_trainSampleData_fileName = 'domestic_trainSampleData_' + todayStr1 + '.parquet'
clientHdfs = client.InsecureClient('http://10.0.4.217:9870', user="search") #跟HDFS建立连接
fileNames = [featureData_fileName, DNN_trainData_fileName, DNN_trainSampleData_fileName]
for fileName in fileNames:
    if fileName in clientHdfs.list('/crawler/'):
        clientHdfs.delete("/crawler/" + fileName, recursive=True)
df_shuffled.write.format('parquet').save("hdfs://10.0.4.217:8020/crawler/" + featureData_fileName)
df_train_DNN.write.format('parquet').save("hdfs://10.0.4.217:8020/crawler/" + DNN_trainData_fileName)
df_train_DNN_sample.repartition(1).write.format('parquet').save("hdfs://10.0.4.217:8020/crawler/" + DNN_trainSampleData_fileName)

for day in ['2019-07-25', '2019-07-26', '2019-07-27', '2019-07-28', '2019-07-29']:
    DNN_valData_queryDate_fileName = 'domestic_DNN_valData_queryDate={}_{}.parquet'.format(day, todayStr1)
    if DNN_valData_queryDate_fileName in clientHdfs.list('/crawler/'):
        clientHdfs.delete("/crawler/" + DNN_valData_queryDate_fileName, recursive=True)
    df_val_DNN.filter(col('queryDate') == day).repartition(1).write.format('parquet').save(
        "hdfs://10.0.4.217:8020/crawler/" + DNN_valData_queryDate_fileName)

    DNN_valData_departDate_fileName = 'domestic_DNN_valData_departDate={}_{}.parquet'.format(day, todayStr1)
    if DNN_valData_departDate_fileName in clientHdfs.list('/crawler/'):
        clientHdfs.delete("/crawler/" + DNN_valData_departDate_fileName, recursive=True)
    df_val_DNN.filter(col('departDate') == day).repartition(1).write.format('parquet').save(
        "hdfs://10.0.4.217:8020/crawler/" + DNN_valData_departDate_fileName)

logger.info("====df_train_DNN, df_val_DNN Finished====")

