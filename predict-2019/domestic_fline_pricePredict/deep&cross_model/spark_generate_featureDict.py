# coding:utf-8
# -*- coding:utf-8 -*-
# coding:utf-8
# -*- coding:utf-8 -*-
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pickle
from hdfs import client
import os
import shutil
import config

class Generate_featureDict():
    def __init__(self):
        self.spark = SparkSession.builder\
                                 .config("spark.master", 'yarn')\
                                 .config("spark.app.name", 'Generate_featureDict')\
                                 .config('spark.driver.memory', '1g')\
                                 .config('spark.executor.memory', '10g')\
                                 .config("spark.executor.cores", '4')\
                                 .config("spark.submit.deployMode", 'client')\
                                 .config('spark.pyspark.python', '/data/search/envspark/bin/python3') \
                                 .config('spark.pyspark.driver.python', '/data/search/envspark/bin/python3')\
                                 .getOrCreate()
        # self.spark = SparkSession.builder\
        #                          .config("spark.master", 'local[*]')\
        #                          .config("spark.app.name", 'test')\
        #                          .getOrCreate()

        self.sc = self.spark.sparkContext
        self.sparkHost = config.sparkHost
        self.sparkDirName_totalData = config.sparkDirName_totalData
        self.columnNames = config.columnNames
        self.numericCols = config.numericCols
        self.categoryCols = config.categoryCols
        self.dropFeatures = config.dropFeatures
        self.label = config.label
        self.featureDict_fileName = config.featureDict_fileName
        self.schema = StructType()
        self._init_load()
        self._init_featureDict()
        self._init_write_to_file()
        self.sc.stop()

    def _init_load(self):
        self.df_org = self.spark.read.format('parquet').load(self.sparkHost + self.sparkDirName_totalData)

    def _init_featureDict(self):
        self.df_org = self.df_org.drop(self.label)
        self.featureDict = {}
        tc = 0
        for colName in self.df_org.columns:
            if colName in self.numericCols:
                continue
            else:  # colName in categoryCols
                uniqueFeature = self.df_org.select(colName).distinct().toPandas()[colName].astype('float').values
                self.featureDict[colName] = dict(zip(uniqueFeature, range(tc, len(uniqueFeature) + tc)))
                tc = tc + len(uniqueFeature)
        self.feature_size = tc
        self.field_size = len(list(set(self.columnNames) - set(self.dropFeatures))) - 1

    def _init_write_to_file(self):
        if os.path.exists(self.featureDict_fileName):
            os.remove(self.featureDict_fileName)
        with open(self.featureDict_fileName, 'wb') as f:
            pickle.dump(self.featureDict, f)


if __name__ == '__main__':
    Generate_featureDict()







