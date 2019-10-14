# coding:utf-8
# -*- coding:utf-8 -*-

import tensorflow as tf
import numpy as np
import pandas as pd
import config
import os
import pickle
from hdfs import client
from io import StringIO, BytesIO
from logConfig import logger
from sklearn.metrics import roc_auc_score, precision_score, recall_score
from datetime import datetime
import math

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


class DeepFM_model_predict():
    def __init__(self, params, dfm_params):
        self.sess = tf.Session()
        self.import_saver = tf.train.import_meta_graph("{}-{}-{}.meta".format(params["localDirName_deepFM_model"], params["generateDate_str1"], params["DNN_model_choice_num"]))
        self.import_saver.restore(self.sess, tf.train.latest_checkpoint(os.path.dirname(params["localDirName_deepFM_model"])))
        self.save_saver = tf.train.Saver(max_to_keep=6)
        self.graph = tf.get_default_graph()
        self.tensor_output = self.graph.get_tensor_by_name("output:0")
        self.tensor_feature_index = self.graph.get_tensor_by_name("feature_index:0")
        self.tensor_feature_value = self.graph.get_tensor_by_name("feature_value:0")
        self.tensor_label = self.graph.get_tensor_by_name("label:0")
        self.tensor_decayed_learning_rate = self.graph.get_tensor_by_name("decayed_learning_rate:0")
        self.tensor_fm_dropout_keep = self.graph.get_tensor_by_name("fm_dropout_keep:0")
        self.tensor_deep_dropout_keep = self.graph.get_tensor_by_name("deep_dropout_keep:0")
        self.tensor_optimizer = self.graph.get_operation_by_name("optimizer")
        self.batch_size = dfm_params["batch_size"]
        self.dropout_fm = dfm_params["dropout_fm"]
        self.dropout_deep = dfm_params["dropout_deep"]

    def get_batch(self, df_index, df_value, index_batch):
        start = index_batch * self.batch_size
        end = start + self.batch_size
        if end < len(df_index):
            end = end
        else:
            end = len(df_index)
        df_index_batch = df_index[start:end]
        df_value_batch = df_value[start:end]
        return df_index_batch, df_value_batch

    def model_output(self, params, df_online_index, df_online_value):
        y_rawPred = []
        total_batch = math.ceil(len(df_online_index) / self.batch_size)
        for index_batch in range(total_batch):
            df_index_batch, df_value_batch = self.get_batch(df_online_index, df_online_value, index_batch)
            feed_dict = {self.tensor_feature_index: df_index_batch,
                         self.tensor_feature_value: df_value_batch,
                         self.tensor_fm_dropout_keep: len(self.dropout_fm) * [1],
                         self.tensor_deep_dropout_keep: len(self.dropout_deep) * [1]
                         }
            y_rawPred_batch = self.sess.run(self.tensor_output, feed_dict=feed_dict)
            # y_rawPred的shape是：（-1，）
            y_rawPred = np.append(y_rawPred, y_rawPred_batch)
        return y_rawPred

    def write_result(self, params, df_online, df_online_index, df_online_value):
            y_rawPred = self.model_output(params, df_online_index, df_online_value)
            df = df_online[params["baseColumns"]]
            df['id_cityCode'] = df[params["baseColumns"][0]].str.cat(df[params["baseColumns"][1]], sep='_')\
                                                            .str.cat(df[params["baseColumns"][2]], sep='_')
            df = df.drop(params["baseColumns"][:3], axis=1)
            #模型预测时，是利用昨天的信息预测今天之后的价格趋势，所以queryDate是yesterday，但是为了便于使用，将queryDate改为today，即站在今天的角度看以后的趋势
            df["queryDate"] = params["generateDate_str2"]
            df["rawPred"] = pd.Series(y_rawPred)
            df['trend'] = df.rawPred.apply(lambda x: 1 if x > params["threshold"] else 0)
            #df的columns=['id_cityCode', 'queryDate', 'rawPred', 'trend']
            df.to_csv(params["localFileName_deepFM_result"], mode='w', header=True, index=False)

    # def re_train(self, df_reTrain_index, df_reTrain_value, y_reTrain_label):
    #     total_batch = math.ceil(len(y_reTrain_label) / self.batch_size)
    #     for index_batch in range(total_batch):
    #         df_index_batch, df_value_batch, y_label_batch = self.get_batch(df_reTrain_index, df_reTrain_value, y_reTrain_label, index_batch)
    #         feed_dict = {self.tensor_feature_index: df_index_batch,
    #                      self.tensor_feature_value: df_value_batch,
    #                      self.tensor_label: y_label_batch,
    #                      self.tensor_fm_dropout_keep: self.dropout_fm,
    #                      self.tensor_deep_dropout_keep: self.dropout_deep,
    #                      self.tensor_extend_dropout_keep: self.dropout_extend
    #                      }
    #         self.sess.run(self.tensor_optimizer, feed_dict=feed_dict)
    #
    # def save_model(self, params):
    #     self.save_saver.save(self.sess, "{}-{}".format(params['DNN_model_fileName'], params["queryDate"]))


def load_data(clientHdfs, params):
    sparkDirName = params["sparkDirName_onlineData"]
    fileNames = clientHdfs.list(sparkDirName)
    fileNames.remove('_SUCCESS')
    df = pd.DataFrame()
    for fileName in fileNames:
        with clientHdfs.read(os.path.join(sparkDirName, fileName)) as reader:
            context = reader.read()
        bytesIO = BytesIO()
        bytesIO.write(context)
        bytesIO.seek(0)
        df_temp = pd.read_parquet(bytesIO)
        df = pd.concat([df, df_temp], axis=0)
    return df


def get_featureDict_info(params):
    with open(params["featureDict_fileName"], 'rb') as f:
        featureDict = pickle.load(f)
    field_size = len(featureDict)
    feature_size = 0
    for k, v in featureDict.items():
        if isinstance(v, dict):
            feature_size += len(v)
        else:
            feature_size += 1
    return featureDict, feature_size, field_size


# def get_metric_scores(params, existLabel=True):
#     if existLabel:
#         pred_fileName = os.path.join(params["DNN_pred_dirName"], "DNN_pred{}_result.csv".format(params["queryDate"]))
#         df = pd.read_csv(pred_fileName, names=params["baseColumns"] + [params['label'], 'rawPred'])
#         df['pred'] = df.rawPred.apply(lambda x: 1 if x > params["threshold"] else 0)
#         aucScore = roc_auc_score(df[params['label']], df['rawPred'])
#         precisionScore = precision_score(df[params['label']], df['pred'])
#         recallScore = recall_score(df[params['label']], df['pred'])
#         return aucScore, precisionScore, recallScore
#     if not existLabel:
#         pass


def run_load_model_predict(params, dfm_params):
    clientHdfs = client.InsecureClient(params['hdfsHost'], user="search")
    fileNames = clientHdfs.list(params['sparkDirName_onlineData'])
    fileNames.remove('_SUCCESS')
    featureDict, _, _ = get_featureDict_info(params)
    dataParser = DataParser(params, featureDict)
    df_online = load_data(clientHdfs, params)
    df_online_index, df_online_value = dataParser.data_parser(df_online, has_label=False)
    deep_model_predict = DeepFM_model_predict(params, dfm_params)
    deep_model_predict.write_result(params, df_online, df_online_index, df_online_value)
    deep_model_predict.sess.close()
    logger.info("====\"{}\" write finished====".format(params["localFileName_deepFM_result"].split("/")[-1]))

