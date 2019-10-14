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



def generate_test_data(params):
    df_train_DNN = spark.read.format('parquet').load(params["sparkHost"] + params["sparkDirName_trainData"])

    df_train_DNN_test = df_train_DNN.filter(col('queryDate') < valDate)
    df_train_DNN_test.repartition(200).write.format('parquet').save(params["sparkHost"] + params["sparkDirName_trainData_test"], mode='overwrite')
    logger.info("====\"{}\" write to HDFS finished ====".format(params["sparkDirName_trainData_test"]))

    df_trainSample_DNN_test = df_train_DNN_test.sample(False, float("%.4f" % (2e5/df_train_DNN_test.count())))
    df_trainSample_DNN_test.write.format('parquet').save(params["sparkHost"] + params["sparkDirName_trainSampleData_test"], mode='overwrite')
    logger.info("====\"{}\" write to HDFS finished ====".format(params["sparkDirName_trainSampleData_test"]))

    df_val_DNN_test = df_train_DNN.filter(col('queryDate') == valDate)
    df_val_DNN_test.write.format('parquet').save(params["sparkHost"] + params["sparkDirName_valData_test"], mode='overwrite')
    logger.info("====\"{}\" write to HDFS finished ====".format(params["sparkDirName_valData_test"]))


if __name__ == "__main__":
    # 将queryDate = valDate的数据作为验证集数据
    valDate = '2019-09-20'

    # spark =SparkSession.builder.master('local[*]').appName("test")\
    #                    .getOrCreate()
    spark = SparkSession.builder \
        .appName("domestic_featuresProcess") \
        .getOrCreate()
    sc = spark.sparkContext
    params = config.params
    generate_test_data(params)


