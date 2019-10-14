# coding:utf-8
# -*- coding:utf-8 -*-
# coding:utf-8
# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
import tensorflow as tf
from sklearn.preprocessing import OneHotEncoder
import pickle
import logging
import math
from logging import handlers
from sklearn.metrics import roc_auc_score
from logConfig import logger


class DeepFM(object):
    #如果是二分类，num_category=1
    def __init__(self, feature_size,
                 field_size,
                 num_category=1,
                 embedding_size=8,
                 dropout_fm=[1.0, 1.0],
                 deep_layers=[32, 32],
                 dropout_deep=[0.5, 0.5, 0.5],
                 deep_layer_activation=tf.nn.relu,
                 epoch=10,
                 batch_size=256,
                 init_learning_rate=0.001,
                 decay_rate=0.98,
                 optimizer="adam",
                 batch_norm=0,
                 batch_norm_decay=0.995,
                 verbose=False,
                 random_seed=2019,
                 use_fm=True,
                 use_deep=True,
                 loss_type="logloss",
                 eval_metric=roc_auc_score,
                 l2_reg=0.0,
                 greater_is_better=True):

        self.feature_size = feature_size
        self.field_size = field_size
        self.embedding_size = embedding_size
        self.num_category = num_category
        self.dropout_fm = dropout_fm
        self.deep_layers = deep_layers
        self.dropout_deep = dropout_deep
        self.deep_layers_activation = deep_layer_activation
        self.epoch = epoch
        self.batch_size = batch_size
        self.init_learning_rate = init_learning_rate
        self.decay_rate = decay_rate
        self.optimizer_type = optimizer
        self.batch_norm = batch_norm
        self.batch_norm_decay = batch_norm_decay
        self.verbose = verbose
        self.random_seed = random_seed
        self.use_fm = use_fm
        self.use_deep = use_deep
        self.loss_type = loss_type
        self.eval_metric = eval_metric
        self.l2_reg = l2_reg
        self.greater_is_better = greater_is_better
        self._init_graph()
        #early_stop相关参数
        self.best_valScore = 0
        self.lessScores_container = []
        self.best_valScore_epoch = 0
        self.best_valScore_counter = 0


    def initialize_weights(self):
        weights = dict()
        #embedding
        weights['feature_embeddings'] = tf.Variable(tf.random_normal(shape=[self.feature_size, self.embedding_size], mean=0.0, stddev=0.01), name='feature_embeddings')
        weights['feature_line'] = tf.Variable(tf.random_normal(shape=[self.feature_size, 1], mean=0.0, stddev=1), name='feature_line')
        #deep layers
        num_layers = len(self.deep_layers)
        input_size = self.field_size * self.embedding_size
        glorot = np.sqrt(2/(input_size+self.deep_layers[0]))
        weights['layers_0'] = tf.Variable(np.random.normal(loc=0, scale=glorot, size=[input_size, self.deep_layers[0]]), dtype=np.float32)
        weights['bias_0'] = tf.Variable(np.random.normal(loc=0, scale=glorot, size=[1, self.deep_layers[0]]), dtype=np.float32)
        for i in range(1, num_layers):
            glorot = np.sqrt(2/(self.deep_layers[i-1]+self.deep_layers[i]))
            weights["layers_%d" %i] = tf.Variable(np.random.normal(loc=0, scale=glorot, size=[self.deep_layers[i-1], self.deep_layers[i]]), dtype=np.float32)
            weights["bias_%d" %i] = tf.Variable(np.random.normal(loc=0, scale=glorot, size=[1, self.deep_layers[i]]), dtype=np.float32)
        if self.use_fm and self.use_deep:
            concat_input_size = self.field_size + self.embedding_size + self.deep_layers[-1]
        elif self.use_fm:
            concat_input_size = self.field_size + self.embedding_size
        elif self.use_deep:
            concat_input_size = self.deep_layers[-1]
        glorot = np.sqrt(2/(concat_input_size+self.num_category))
        weights['feature_concat'] = tf.Variable(np.random.normal(loc=0, scale=glorot, size=[concat_input_size, self.num_category]), dtype=np.float32)
        weights['bias_concat'] = tf.Variable(np.random.normal(loc=0, scale=glorot, size=[1, self.num_category]),
                                             dtype=np.float32)
        return weights

    def _init_graph(self):
        self.graph = tf.Graph()
        with self.graph.as_default():
            tf.set_random_seed(self.random_seed)
            self.feature_index = tf.placeholder(tf.int32, shape=[None, None], name='feature_index')
            self.feature_value = tf.placeholder(tf.float32, shape=[None, None], name='feature_value')
            self.label = tf.placeholder(tf.float32, shape=[None, self.num_category], name='label')
            self.decayed_learning_rate = tf.placeholder(shape=[], dtype=np.float32, name='decayed_learning_rate')
            self.fm_dropout_keep = tf.placeholder(tf.float32, shape=[None], name='fm_dropout_keep')
            self.deep_dropout_keep = tf.placeholder(tf.float32, shape=[None], name='deep_dropout_keep')
            # self.train_phase = tf.placeholder(tf.bool, name='train_phase')
            self.weights = self.initialize_weights()
            #m=num_samples, F=field_size, N=feature_size, K=embedding_size
            self.embeddings = tf.nn.embedding_lookup(self.weights['feature_embeddings'], self.feature_index) #m*F*K
            feature_value = tf.reshape(self.feature_value, shape=[-1, self.field_size, 1])
            self.embeddings = tf.multiply(self.embeddings, feature_value)
            #first order term
            self.y_first_order = tf.nn.embedding_lookup(self.weights['feature_line'], self.feature_index)
            self.y_first_order = tf.reduce_sum(tf.multiply(self.y_first_order, feature_value), 2)
            self.y_first_order = tf.nn.dropout(self.y_first_order, self.fm_dropout_keep[0])
            #second order term
            self.sum_square_embedding = tf.square(tf.reduce_sum(self.embeddings, 1))  #m*K
            self.square_sum_embedding = tf.reduce_sum(tf.square(self.embeddings), 1)  #m*K
            self.y_second_order = 0.5 * tf.subtract(self.sum_square_embedding, self.square_sum_embedding)
            self.y_second_order = tf.nn.dropout(self.y_second_order, self.fm_dropout_keep[1])
            #deep component
            self.deep_input = tf.reshape(self.embeddings, shape=[-1, self.field_size*self.embedding_size])
            self.y_deep = tf.nn.dropout(self.deep_input, self.deep_dropout_keep[0])
            for i in range(0, len(self.deep_layers)):
                self.y_deep = tf.add(tf.matmul(self.y_deep, self.weights["layers_%d" %i]), self.weights['bias_%d' %i])
                self.y_deep = self.deep_layers_activation(self.y_deep)
                self.y_deep = tf.nn.dropout(self.y_deep, self.deep_dropout_keep[i+1])

            if self.use_fm and self.use_deep:
                feature_concat = tf.concat([self.y_first_order, self.y_second_order, self.y_deep], axis=1)
            elif self.use_fm:
                feature_concat = tf.concat([self.y_first_order, self.y_second_order], axis=1)
            elif self.use_deep:
                feature_concat = self.y_deep
            self.output = tf.add(tf.matmul(feature_concat, self.weights['feature_concat']), self.weights['bias_concat'])

            #loss
            if self.loss_type == 'logloss':
                self.output = tf.nn.sigmoid(self.output, name='output')
                self.loss = tf.losses.log_loss(self.label, self.output)
            if self.loss_type == 'mse':
                self.loss = tf.reduce_mean(tf.square(self.label - self.output), name='output')
            if self.loss_type == 'softmax':
                self.output = tf.nn.softmax(self.output,  name='output')
                self.loss = -tf.reduce_sum(self.label * tf.log(self.output))/len(self.label)
            #regulation on weight
            if self.l2_reg > 0:
                self.loss += tf.contrib.layers.l2_regularizer(self.l2_reg)(self.weights['feature_concat'])
                if self.use_deep:
                    for i in range(len(self.deep_layers)):
                        self.loss += tf.contrib.layers.l2_regularizer(self.l2_reg)(self.weights['layers_%d' %i])
            #optimizer
            if self.optimizer_type == 'adam':
                self.optimizer = tf.train.AdamOptimizer(learning_rate=self.decayed_learning_rate, beta1=0.9, beta2=0.999, epsilon=1e-8, name='optimizer').minimize(self.loss)
            elif self.optimizer_type == 'adagrad':
                self.optimizer = tf.train.AdagradOptimizer(leraning_rate=self.decayed_learning_rate, initial_accumulator_value=1e-8, name='optimizer').minimize(self.loss)
            elif self.opitimizer == 'gd':
                self.optimizer = tf.train.GradientDescentOptimizer(learning_rate=self.decayed_learning_rate, name='optimizer').minimize(self.loss)
            elif self.optimizer_type == 'momentum':
                self.optimizer = tf.train.MomentumOptimizer(learning_rate=self.decayed_learning_rate, momentum=0.95, name='optimizer').minimize(self.loss)
            # init
            self.saver = tf.train.Saver(max_to_keep=6)
            init = tf.global_variables_initializer()
            self.sess = tf.Session()
            self.sess.run(init)

            # #number of parameters
            # total_parameters = 0
            # for variable in self.weights.values():
            #     shape = tf.get_shape()
            #     variable_parameters = 1
            #     for dim in shape:
            #         variable_parameters *= dim
            #     total_parameters += variable_parameters
            # if self.verbose > 0:
            #     print("#params: %d" % total_parameters)

    def get_batch(self, df_index, df_value, y_label, index_batch):
        start = index_batch * self.batch_size
        end = start + self.batch_size
        if end < len(y_label):
            end = end
        else:
            end = len(y_label)
        df_index_batch = df_index[start:end]
        df_value_batch = df_value[start:end]
        y_label_batch = y_label[start:end]
        return df_index_batch, df_value_batch, y_label_batch

    def fit_on_batch(self, df_index_batch, df_value_batch, y_label_batch, decayed_learning_rate):
        feed_dict = {self.feature_index: df_index_batch,
                     self.feature_value: df_value_batch,
                     self.label: y_label_batch,
                     self.fm_dropout_keep: self.dropout_fm,
                     self.deep_dropout_keep: self.dropout_deep,
                     self.decayed_learning_rate: decayed_learning_rate}
        self.sess.run(self.optimizer, feed_dict=feed_dict)


    def fit(self, df_index, df_value, y_label, decayed_learning_rate):
        total_batch = math.ceil(len(y_label)/self.batch_size)
        for index_batch in range(total_batch):
            df_index_batch, df_value_batch, y_label_batch = self.get_batch(df_index, df_value, y_label, index_batch)
            self.fit_on_batch(df_index_batch, df_value_batch, y_label_batch, decayed_learning_rate)
            # logger.info("==== fit on batch index:{}====".format(index_batch))

    def predict(self, df_index, df_value):
        y_temp = [[1]] * len(df_index)
        y_rawPred = []
        total_batch = math.ceil(len(df_index) / self.batch_size)
        for index_batch in range(total_batch):
            df_index_batch, df_value_batch, y_label_batch = self.get_batch(df_index, df_value, y_temp, index_batch)
            feed_dict = {self.feature_index: df_index_batch,
                         self.feature_value: df_value_batch,
                         self.fm_dropout_keep: [1.0] * len(self.dropout_fm),
                         self.deep_dropout_keep: [1.0] * len(self.dropout_deep)}
            y_rawPred_batch = self.sess.run(self.output, feed_dict=feed_dict)
            # y_rawPred的shape是：（-1，）
            y_rawPred = np.append(y_rawPred, y_rawPred_batch)
        return y_rawPred

    def evaluate(self, df_index, df_value, y_label):
        loss_sum = 0
        y_rawPred = []
        total_batch = math.ceil(len(y_label) / self.batch_size)
        for index_batch in range(total_batch):
            df_index_batch, df_value_batch, y_label_batch = self.get_batch(df_index, df_value, y_label, index_batch)
            feed_dict = {self.feature_index: df_index_batch,
                         self.feature_value: df_value_batch,
                         self.label: y_label_batch,
                         self.fm_dropout_keep: [1.0] * len(self.dropout_fm),
                         self.deep_dropout_keep: [1.0] * len(self.dropout_deep)}
            y_rawPred_batch, loss_batch = self.sess.run([self.output, self.loss], feed_dict=feed_dict)
            # logger.info("==== y_rawPred_batch shape is:{}====".format(np.shape(y_rawPred_batch)))
            # y_rawPred的shape是：（-1，）
            y_rawPred = np.append(y_rawPred, y_rawPred_batch)
            loss_sum = loss_sum + (loss_batch * self.batch_size)
        loss = loss_sum / len(y_label)
        y_label = np.reshape(y_label, (-1,))
        eval_score = self.eval_metric(y_label, y_rawPred)
        return eval_score, loss

    #如果使用early_stop，需要将最近产生的训练集作为测试集，这样就损失了最近发生的数据，但是最近发生的数据往往比较重要，所以不使用eraly_stop
    def early_stop(self, df_index, df_value, y_label, epoch, counter, params):
        valScore, _ = self.evaluate(df_index, df_value, y_label)
        if self.best_valScore < valScore:
            self.best_valScore = valScore
            self.lessScores_container = []
            self.best_valScore_epoch = epoch + 1
            self.best_valScore_counter = counter
            self.saver.save(self.sess, "{}-{}".format(params['localDirName_deepFM_model'], params["generateDate_str1"]),
                           global_step=0)
        else:
            self.lessScores_container.append(valScore)
            if len(self.lessScores_container) > 10:
                logger.info("====deepFM train Model best_counter is {}-{}".format(self.best_valScore_epoch, self.best_valScore_counter))
                self.earlyStop_info = True
        return self.earlyStop_info

