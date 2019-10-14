# coding:utf-8
# -*- coding:utf-8 -*-
import config
from logConfig import logger
import load_model_to_predict
import write_result_to_mongo
from hdfs import client
import os
import pickle
from io import StringIO, BytesIO
import pandas as pd
import numpy as np
import deepFM
from pyspark.sql.types import *
import tensorflow as tf
from sklearn.metrics import roc_auc_score
from pyspark.sql import SparkSession,Row
import time
import gc

class DataParser():
    def __init__(self, params, featureDict):
        self.columnNames = params["columnNames"]
        self.numericCols = params["numericCols"]
        self.categoryCols = params["categoryCols"]
        self.dropFeatures = params["dropFeatures"]
        self.baseColumns = params["baseColumns"]
        self.label = params["label"]
        self.featureDict = featureDict

    def data_parser(self, df, has_label=True):
        if has_label:
            #y_label.shape=(m,1)
            y_label = df[self.label].to_frame().astype('float').values
            df = df.drop(self.baseColumns+[self.label], axis=1).astype('float')
            df_index = df.copy()
            df_value = df.copy()
            for colName in df.columns:
                if colName in self.numericCols:
                    df_index[colName] = self.featureDict[colName]
                    df_value[colName] = df_value[colName]
                else:  # colName in categoryCols
                    df_index[colName] = df[colName].map(self.featureDict[colName])
                    df_value[colName] = 1
            df_index = df_index.values
            df_value = df_value.values
            return df_index, df_value, y_label
        if not has_label:
            try:
                df.drop([self.label], axis=1, inplace=True)
            except:
                pass
            df = df.drop(self.baseColumns, axis=1).astype('float')
            df_index = df.copy()
            df_value = df.copy()
            for colName in df.columns:
                if colName in self.numericCols:
                    df_index[colName] = self.featureDict[colName]
                else:  # col in categoryCols
                    df_index[colName] = df[colName].map(self.featureDict[colName])
                    df_value[colName] = 1
            df_index = df_index.values
            df_value = df_value.values
            return df_index, df_value


def get_featureDict_info(featureDict_fileName):
    with open(featureDict_fileName, 'rb') as f:
        featureDict = pickle.load(f)
    field_size = len(featureDict)
    feature_size = 0
    for k, v in featureDict.items():
        if isinstance(v, dict):
            feature_size += len(v)
        else:
            feature_size += 1
    return featureDict, feature_size, field_size


def load_parquet_file(params, clientHdfs, trainSampleData=True):
    if trainSampleData:
        fileNames = clientHdfs.list(params["sparkDirName_trainSampleData"])
        fileNames.remove('_SUCCESS')
        with clientHdfs.read(os.path.join(params["sparkDirName_trainSampleData"], fileNames[0])) as reader:
            context = reader.read()
    elif not trainSampleData:  #valData 数据需要去HDFS读取
        fileNames = clientHdfs.list(params["sparkDirName_valSampleData"])
        fileNames.remove('_SUCCESS')
        with clientHdfs.read(os.path.join(params["sparkDirName_valSampleData"], fileNames[0])) as reader:
            context = reader.read()
    bytesIO = BytesIO()
    bytesIO.write(context)
    bytesIO.seek(0)
    df = pd.read_parquet(bytesIO)
    bytesIO.truncate(0)
    # df = pd.read_parquet(params["sparkDirName_trainSampleData"])
    return df


def train_model(fileName, params, dataParser):
    global dfm
    with clientHdfs.read(os.path.join(params["sparkDirName_trainData"], fileName)) as reader:
        context = reader.read()
    # with open(os.path.join(params["sparkDirName_trainData"], fileName), 'rb') as f:
    #     context = f.read()
    bytesIO = BytesIO()
    bytesIO.write(context)
    bytesIO.seek(0)
    df_train = pd.read_parquet(bytesIO)
    bytesIO.truncate(0)
    df_train_index, df_train_value, y_train_label = dataParser.data_parser(df_train, has_label=True)
    global decayed_learning_rate
    decayed_learning_rate = dfm.init_learning_rate * np.power(dfm.decay_rate, (counter - 1))
    dfm.fit(df_train_index, df_train_value, y_train_label, decayed_learning_rate)


# def log_result(logger, epoch, counter):
#     global dfm
#     global train_scores
#     global val_scores
#     global train_losses
#     global val_losses
#     train_score, train_loss = dfm.evaluate(df_trainSample_index, df_trainSample_value, y_trainSample_label)
#     train_scores.append(round(train_score, 5))
#     train_losses.append(round(train_loss, 5))
#     val_score, val_loss = dfm.evaluate(df_valSample_index, df_valSample_value, y_valSample_label)
#     val_scores.append(round(val_score, 5))
#     val_losses.append(round(val_loss, 5))
#     if counter % 50 == 0:
#         logger.info("====dfm params: {}====".format(dfm_params))
#         logger.info("====epoch: {}-{}, train_scores: {}====".format(epoch + 1, counter, train_scores))
#         logger.info("====epoch: {}-{}, val_scores: {}====".format(epoch + 1, counter, val_scores))
#         logger.info("====epoch: {}-{}, train_losses: {}====".format(epoch + 1, counter, train_losses))
#         logger.info("====epoch: {}-{}, val_losses: {}====".format(epoch + 1, counter, val_losses))




if __name__ == '__main__':
    params = config.params
    clientHdfs = client.InsecureClient(params['hdfsHost'], user="search")
    # clientHdfs = ''
    featureDict, feature_size, field_size = get_featureDict_info(params['featureDict_fileName'])
    dfm_params = {"feature_size": feature_size,
                  "field_size": field_size,
                  "num_category": 1,
                  "embedding_size": config.embedding_size,
                  "dropout_fm": config.dropout_fm,
                  "deep_layers": config.deep_layers,
                  "dropout_deep": config.dropout_deep,
                  "deep_layer_activation": tf.nn.relu,
                  "epoch": config.epoches,
                  "batch_size": config.batch_size,
                  "init_learning_rate": config.init_learning_rate,
                  "decay_rate": config.decay_rate,
                  "optimizer": "adam",
                  "batch_norm": 0,
                  "batch_norm_decay": 0.995,
                  "verbose": False,
                  "random_seed": 2019,
                  "use_fm": True,
                  "use_deep": True,
                  "loss_type": "logloss",
                  "eval_metric": roc_auc_score,
                  "l2_reg": 0.0,
                  "greater_is_better": True
                  }
    dataParser = DataParser(params, featureDict)
    gc.collect()
    fileNames = clientHdfs.list(params['sparkDirName_trainData'])
    # fileNames = os.listdir(params['sparkDirName_trainData'])
    fileNames.remove('_SUCCESS')
    fileNames_num = len(fileNames)

    dfm = deepFM.DeepFM(**dfm_params)
    train_scores = []
    val_scores = []
    train_losses = []
    val_losses = []
    for epoch in range(params['epoches']):
        counter = 0
        for fileName in fileNames:
            counter += 1
            train_model(fileName, params, dataParser)
            # logger.info("====Train {}-{} finished====".format(epoch, counter))
            if counter % 100 == 0:
                dfm.saver.save(dfm.sess, "{}-{}-{}".format(params['localDirName_deepFM_model'], params['generateDate_str1'], epoch + 1), global_step=counter)
    dfm.sess.close()
    logger.info("====deepFM Train Model finished====")

    load_model_to_predict.run_load_model_predict(params, dfm_params)

    write_result_to_mongo.Result_To_Mongo(params)
