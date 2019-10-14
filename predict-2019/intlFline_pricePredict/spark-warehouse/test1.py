# coding:utf-8
# -*- coding:utf-8 -*-
########
#国内低价原始数据增加节假日特征
########
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorIndexer, StringIndexer, VectorAssembler, OneHotEncoder, ChiSqSelector
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
df = df_org.dropna(how='any', subset=['orgPrice', 'distance', 'departTime', 'isShare', 'trend'])\
    .withColumn('discountOff', round((1-col('price')/col('orgPrice')) * 100, 2))\
    .withColumn('distance', col('distance').cast('double'))\
    .na.fill({'seatLeft': '-1'})\
    .withColumn('seatLeft', col('seatLeft').cast('double'))\
    .withColumn('departTime', substring_index('departTime', ':', 1).cast('int'))\
    .withColumn('isShare', col('isShare').cast('int'))\
    .withColumn('trend', col('trend').cast('int'))\
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
stages = []
for colName in stringIndexerCols:
    stringIndexer = StringIndexer(inputCol=colName, outputCol='stringIndexer_%s' %colName)
    stages.append(stringIndexer)
pipeline = Pipeline(stages=stages)
pipelineModel = pipeline.fit(df)
df = pipelineModel.transform(df)
for colName in stringIndexerCols:
    df = df.withColumn('stringIndexer_%s' %colName, col('stringIndexer_%s' %colName).cast('double'))
featureData_fileName = 'domestic_featureData_' + '.parquet'
# clientHdfs = client.InsecureClient("hdfs://10.0.4.217:8020", user="search") #跟HDFS建立连接
# if predict_fileName in clientHdfs.list('/crawler/'):
#     clientHdfs.delete("/crawler/" + predict_fileName, recursive=True)
df.write.format('parquet').save("hdfs://10.0.4.217:8020/crawler/" + featureData_fileName)
assembleFeatures = ['distance', 'seatLeft', 'departTime', 'isShare', 'discountOff','depart_isVacation', 'depart_vacationDays',
     'depart_dayofvacation', 'depart_isFestival', 'departYear','departMonth', 'depart_dayofmonth', 'depart_weekofyear',
     'depart_dayofweek', 'departQuarter', 'intervalDays', 'intervalMonths', 'intervalWeeks', 'intervalQuarters',
     'query_isVacation', 'query_vacationDays', 'query_dayofvacation', 'query_isFestival', 'queryYear', 'queryMonth',
     'query_dayofmonth', 'query_weekofyear', 'query_dayofweek', 'queryQuarter', 'stringIndexer_fn', 'stringIndexer_fc',
     'stringIndexer_org', 'stringIndexer_dst', 'stringIndexer_depart_festival', 'stringIndexer_query_festival']
assembler = VectorAssembler(inputCols=assembleFeatures, outputCol='features')
df_total = assembler.transform(df).select('fn', 'org', 'dst', 'departDate', 'queryDate', 'features', 'trend')
df_train = df_total.filter(df_total.departDate < '2019-07-25')
df_val = df_total.filter(df_total.departDate >= '2019-07-25')
df_train.cache()
df_val.cache()
predict_model = RandomForestClassifier(labelCol='trend', numTrees=50, featureSubsetStrategy='auto', subsamplingRate=0.8)
RF_model = predict_model.fit(df_train)
df_predict = RF_model.transform(df_val)

todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
predict_fileName = 'domestic_RF_model_predictResult_' + todayStr1 + '.parquet'
# clientHdfs = client.InsecureClient("hdfs://10.0.4.217:8020", user="search") #跟HDFS建立连接
# if predict_fileName in clientHdfs.list('/crawler/'):
#     clientHdfs.delete("/crawler/" + predict_fileName, recursive=True)
df_predict.write.format('parquet').save('hdfs://10.0.4.217:8020/crawler/' + predict_fileName)
sc.stop()
