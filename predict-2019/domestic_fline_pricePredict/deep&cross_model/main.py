# coding:utf-8
# -*- coding:utf-8 -*-
import config
from logConfig import log
from hdfs import client
import os
import pickle
from io import StringIO, BytesIO
import pandas as pd
import numpy as np
import DCN
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
            #去掉label列和连续属性列，注意y_label.shape=(m,1),
            y_label = df[self.label].to_frame().astype('float').values
            df = df.drop(self.baseColumns+[self.label]+self.numericCols, axis=1)
        else:
            df = df.drop(self.numericCols, axis=1)
        df_index = df.copy()
        df_value = df.copy()
        for colName in df.columns:
            df_index[colName] = df_index[colName].map(self.featureDict[colName])
            df_value[colName] = 1
        cate_Xi = df_index.values
        # cate_Xv = df_value.values
        numeric_Xv = df[self.numericCols].values
        if has_label:
            return cate_Xi, numeric_Xv, y_label
        if not has_label:
            return cate_Xi, numeric_Xv


def get_featureDict_info(base_params):
    with open(base_params["featureDict_fileName"], 'rb') as f:
        featureDict = pickle.load(f)
    cate_field_size = len(featureDict)
    cate_feature_size = 0
    for k, v in featureDict.items():
        cate_feature_size += len(v)
    return featureDict, cate_feature_size, cate_field_size


def load_parquet_file(base_params, clientHdfs, trainSampleData=True):
    if trainSampleData:
        fileNames = clientHdfs.list(base_params["sparkDirName_trainSampleData"])
        fileNames.remove('_SUCCESS')
        with clientHdfs.read(os.path.join(base_params["sparkDirName_trainSampleData"], fileNames[0])) as reader:
            context = reader.read()
    elif not trainSampleData:  #valData 数据需要去HDFS读取
        fileNames = clientHdfs.list(base_params["sparkDirName_valSampleData"])
        fileNames.remove('_SUCCESS')
        with clientHdfs.read(os.path.join(base_params["sparkDirName_valSampleData"], fileNames[0])) as reader:
            context = reader.read()
    bytesIO = BytesIO()
    bytesIO.write(context)
    bytesIO.seek(0)
    df = pd.read_parquet(bytesIO)
    bytesIO.truncate(0)
    # df = pd.read_parquet(base_params["sparkDirName_trainSampleData"])
    return df

def train_model(fileName, base_params, dataParser, counter):
    global dcn
    with clientHdfs.read(os.path.join(base_params["sparkDirName_trainData"], fileName)) as reader:
        context = reader.read()
    # with open(os.path.join(base_params["sparkDirName_trainData"], fileName), 'rb') as f:
    #     context = f.read()
    bytesIO = BytesIO()
    bytesIO.write(context)
    bytesIO.seek(0)
    df_train = pd.read_parquet(bytesIO)
    bytesIO.truncate(0)
    train_Xi, train_numeric_Xv, train_y_label = dataParser.data_parser(df_train, has_label=True)
    global decayed_learning_rate
    decayed_learning_rate = dcn.init_learning_rate * np.power(dcn.decay_rate, (counter-1))
    dcn.fit(train_Xi, train_numeric_Xv, train_y_label, decayed_learning_rate)


def log_result(epoch, counter):
    global dcn
    global train_scores
    global val_scores
    global train_losses
    global val_losses
    train_score, train_loss = dcn.evaluate(trainSample_Xi, trainSample_numeric_Xv, trainSample_y_label)
    train_scores.append(round(train_score, 5))
    train_losses.append(round(train_loss, 5))
    val_score, val_loss = dcn.evaluate(valSample_Xi, valSample_numeric_Xv, valSample_y_label)
    val_scores.append(round(val_score, 5))
    val_losses.append(round(val_loss, 5))
    if counter % 50 == 0:
        logger.info("====dcn params: {}====".format(dcn_params))
        logger.info("====epoch: {}-{}, train_scores: {}====".format(epoch + 1, counter, train_scores))
        logger.info("====epoch: {}-{}, val_scores: {}====".format(epoch + 1, counter, val_scores))
        logger.info("====epoch: {}-{}, train_losses: {}====".format(epoch + 1, counter, train_losses))
        logger.info("====epoch: {}-{}, val_losses: {}====".format(epoch + 1, counter, val_losses))


if __name__ == '__main__':
    logger = log()
    base_params = {"epoches": config.epoches,
                    "sparkDirName_trainData": config.sparkDirName_trainData,
                    "sparkDirName_trainSampleData": config.sparkDirName_trainData,
                    "sparkDirName_valSampleData": config.sparkDirName_valSampleData,
                    "featureDict_fileName": config.featureDict_fileName,
                    "DNN_model_fileName": config.DNN_model_fileName,
                    "columnNames": config.columnNames,
                    "numericCols": config.numericCols,
                    "categoryCols": config.categoryCols,
                    "dropFeatures": config.dropFeatures,
                    "baseColumns": config.baseColumns,
                    "label": config.label,
                    "hdfsHost": config.hdfsHost
                    }
    clientHdfs = client.InsecureClient(base_params['hdfsHost'], user="search")
    # clientHdfs = ''

    featureDict, cate_feature_size, cate_field_size = get_featureDict_info(base_params)
    dcn_params = {"cate_feature_size": cate_feature_size,
                 "cate_field_size": cate_field_size,
                 "numeric_feature_size": len(config.numericCols),
                 "embedding_size": config.embedding_size,
                 "deep_layers": config.deep_layers,
                 "dropout_deep": config.dropout_deep,
                 "deep_layers_activation": tf.nn.relu,
                 "epoch": config.epoches,
                 "batch_size": config.batch_size,
                 "init_learning_rate": config.init_learning_rate,
                 "decay_rate": config.decay_rate,
                 "optimizer_type": "adam",
                 "batch_norm": 0,
                 "batch_norm_decay": 0.995,
                 "verbose": False,
                 "random_seed": 2016,
                 "loss_type": "logloss",
                 "eval_metric": roc_auc_score,
                 "l2_reg": 0.0,
                 "greater_is_better": True,
                 "cross_layers_num": config.cross_layers_num
                }
    df_trainSample = load_parquet_file(base_params, clientHdfs, trainSampleData=True).sample(10000)
    df_valSample = load_parquet_file(base_params, clientHdfs, trainSampleData=False).sample(10000)
    dataParser = DataParser(base_params, featureDict)
    trainSample_Xi, trainSample_numeric_Xv, trainSample_y_label = dataParser.data_parser(df_trainSample, has_label=True)
    valSample_Xi, valSample_numeric_Xv, valSample_y_label = dataParser.data_parser(df_valSample, has_label=True)
    del df_trainSample
    del df_valSample
    gc.collect()
    fileNames = clientHdfs.list(base_params['sparkDirName_trainData'])
    # fileNames = os.listdir(base_params['sparkDirName_trainData'])
    fileNames.remove('_SUCCESS')
    fileNames_num = len(fileNames)
    #参数搜索
    search_params = {"init_learning_rate": [0.1, 0.01, 0.005, 0.001]}
    for k, v in search_params.items():
        for param in v:
            dcn_params[k] = param

            dcn = DCN.DCN(**dcn_params)
            train_scores = []
            val_scores = []
            train_losses = []
            val_losses = []
            counter = 0
            for epoch in range(base_params['epoches']):
                for fileName in fileNames:
                    counter += 1
                    train_model(fileName, base_params, dataParser, counter)
                    log_result(epoch, counter)
                    logger.info("====Train {}-{} finished, decayed_learning_rate={} ====".format(epoch, counter, decayed_learning_rate))
                    if counter % 10 == 0:
                        dcn.saver.save(dcn.sess, "{}-{}".format(base_params['DNN_model_fileName'], epoch + 1),
                                       global_step=counter)
            dcn.sess.close()
    logger.info("====All Train Model finished====")

