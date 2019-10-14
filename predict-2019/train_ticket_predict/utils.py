# coding:utf-8
# -*- coding:utf-8 -*-

import time
from datetime import datetime, timedelta
import re
from hdfs import client
from logConfig import logger
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession


# class SparkSessionBase(object):
#     def __init__(self):
#         self.spark_appName = "Spark_job"
#         self.spark_master = "yarn"
#         self.spark_deploy_mode = 'client'
#         self.spark_executor_memory = "10g"
#         self.spark_num_executors = 14
#         self.spark_executor_cores = 4
#         self.spark_driver_memory = '1g'
#         self.spark_driver_python = "/data/search/envspark/bin/python3"
#         self.spark_pyspark_python = "/data/search/envspark/bin/python3"
#         self.spark_jars_1 = "/data/search/spark/jars/bson-3.4.2.jar"
#         self.spark_jars_2 = "/data/search/spark/jars/mongo-spark-connector_2.11-2.1.1.jar"
#
#     def create_sparkSession(self):
#         conf = SparkConf()
#         configs = (
#             ("spark.app.name", self.spark_appName),
#             ("spark.master", self.spark_master),
#             ("spark.submit.deployMode", self.spark_deploy_mode),
#             ("spark.executor.memory", self.spark_executor_memory),
#             ("spark.executor.instances", self.spark_num_executors),
#             ("spark.executor.cores", self.spark_executor_cores),
#             ("spark.driver.memory", self.spark_driver_memory),
#             ("spark.pyspark.driver.python", self.spark_driver_python),
#             ("spark.pyspark.python", self.spark_pyspark_python),
#             ("spark.jars", self.spark_jars_1),
#             ("spark.jars", self.spark_jars_2))
#
#         conf.setAll(configs)
#         spark = SparkSession.builder.config(conf=conf).getOrCreate()
#
#         return spark



class Timer():
    def __init__(self):
        self.startTime = None
        self.endTime = None
        self.costTime = None


    def start(self):
        self.startTime = time.time()

    # def stop(self):
    #     self.endTime = time.time()
    #     logger.info('Time taken: {} ms'.format(round((self.endTime - self.startTime)*1000)))

    def cost(self):
        self.endTime = time.time()
        self.costTime = round((self.endTime - self.startTime)*1000)
        return self.costTime

def delete_before2_localData(fileName, params):
    before2_dateStr1 = datetime.strftime(params["generateDate"] - timedelta(days=2), "%Y%m%d")
    pattern = re.compile(r'\d{8}')
    before2_fileName = re.sub(pattern, before2_dateStr1, fileName)
    if os.path.exists(before2_fileName):
        os.remove(before2_fileName)
        logger.info("====\"{}\" delete finished ====".format(before2_fileName))


def delete_before2_sparkData(fileName, params):
    clientHdfs = client.InsecureClient(params["hdfsHost"], user="search")
    before2_dateStr1 = datetime.strftime(params["generateDate"] - timedelta(days=2), "%Y%m%d")
    pattern = re.compile(r'\d{8}')
    before2_fileName = re.sub(pattern, before2_dateStr1, fileName)
    if before2_fileName in clientHdfs.list(os.path.dirname(fileName)):
        clientHdfs.delete(before2_fileName, recursive=True)
        logger.info("====\"{}\" delete finished ====".format(before2_fileName))

def upload_to_hdfs(localFileName, sparkDirName, params):
    clientHdfs = client.InsecureClient(params["hdfsHost"], user="search")
    if sparkDirName.split('/')[-1] in clientHdfs.list(os.path.dirname(sparkDirName)):
        clientHdfs.delete(sparkDirName, recursive=True)
    clientHdfs.upload(sparkDirName, localFileName)
    logger.info("====\"{}\" upload to HDFS finished====".format(localFileName.split('/')[-1]))
    delete_before2_sparkData(sparkDirName, params)