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
from logConfig import log

logger = log()

class DCN():
    def __init__(self, cate_feature_size,
                 cate_field_size,
                 numeric_feature_size,
                 embedding_size=8,
                 deep_layers=[32, 32],
                 dropout_deep=[0.5, 0.5, 0.5],
                 deep_layers_activation=tf.nn.relu,
                 epoch=10,
                 batch_size=256,
                 init_learning_rate=0.001,
                 decay_rate=0.98,
                 optimizer_type="adam",
                 batch_norm=0,
                 batch_norm_decay=0.995,
                 verbose=False,
                 random_seed=2016,
                 loss_type="logloss",
                 eval_metric=roc_auc_score,
                 l2_reg=0.0,
                 greater_is_better=True,
                 cross_layers_num=3):
        self.cate_feature_size = cate_feature_size
        self.cate_field_size = cate_field_size
        self.numeric_feature_size = numeric_feature_size
        self.embedding_size = embedding_size
        self.total_size = self.cate_field_size * self.embedding_size + self.numeric_feature_size
        self.deep_layers = deep_layers
        self.dropout_deep = dropout_deep
        self.deep_layers_activation = deep_layers_activation
        self.epoch = epoch
        self.batch_size = batch_size
        self.init_learning_rate = init_learning_rate
        self.decay_rate = decay_rate
        self.optimizer_type = optimizer_type
        self.batch_norm = batch_norm
        self.batch_norm_decay = batch_norm_decay
        self.verbose = verbose
        self.random_seed = random_seed
        self.loss_type = loss_type
        self.eval_metric = eval_metric
        self.l2_reg = l2_reg
        self.greater_is_better = greater_is_better
        self.cross_layers_num = cross_layers_num

        self._init_graph()

    def _initialize_weights(self):
        weights = dict()
        #embeddings
        weights['feature_embeddings'] = tf.Variable(np.random.normal(size=(self.cate_feature_size, self.embedding_size), loc=0, scale=0.01), dtype=np.float32, name='feature_embeddings')
        #deep_layers
        glorot = np.sqrt(2/(self.total_size+self.deep_layers[0]))
        weights['deep_layer_0'] = tf.Variable(np.random.normal(size=(self.total_size, self.deep_layers[0]), loc=0, scale=glorot), dtype=np.float32)
        weights['deep_bias_0'] = tf.Variable(np.random.normal(size=(1, self.deep_layers[0]), loc=0, scale=glorot), dtype=np.float32)
        for i in range(1, len(self.deep_layers)):
            glorot = np.sqrt(2/(self.deep_layers[i-1]+self.deep_layers[i]))
            weights['deep_layer_%s' %i] = tf.Variable(np.random.normal(size=(self.deep_layers[i-1], self.deep_layers[i]), loc=0, scale=glorot), dtype=np.float32)
            weights['deep_bias_%s' %i] = tf.Variable(np.random.normal(size=(1, self.deep_layers[i]), loc=0, scale=glorot), dtype=np.float32)
        #cross layers
        for i in range(self.cross_layers_num):
            glorot = np.sqrt(2/(self.total_size+1))
            weights['cross_layer_%s' %i] = tf.Variable(np.random.normal(size=(self.total_size, 1), loc=0, scale=glorot), dtype=np.float32)
            weights['cross_bias_%s' %i] = tf.Variable(np.random.normal(size=(self.total_size, 1), loc=0, scale=glorot), dtype=np.float32)

        concat_size = self.total_size + self.deep_layers[-1]
        glorot = np.sqrt(2/(concat_size+1))
        weights['feature_concat'] = tf.Variable(np.random.normal(size=(concat_size, 1), loc=0, scale=glorot), dtype=np.float32)
        weights['concat_bias'] = tf.Variable(tf.constant(0.01), dtype=np.float32)
        return weights

    def _init_graph(self):
        self.graph = tf.Graph()
        with self.graph.as_default():
            tf.set_random_seed(self.random_seed)
            self.feat_index = tf.placeholder(shape=(None, None), dtype=np.int32, name='feat_index')
            # self.feat_value = tf.placeholder(shape=(None, None), dtype=np.float32, name='feat_value')
            self.numeric_value = tf.placeholder(shape=(None, None), dtype=np.float32, name='numeric_value')
            self.label = tf.placeholder(shape=(None, 1), dtype=np.float32, name='label')
            self.decayed_learning_rate = tf.placeholder(shape=[], dtype=np.float32, name='decayed_learning_rate')
            self.dropout_keep_deep = tf.placeholder(shape=(None,), dtype=np.float32, name='dropout_keep_deep')

            self.weights = self._initialize_weights()

            self.embeddings = tf.nn.embedding_lookup(self.weights['feature_embeddings'], self.feat_index)
            # feat_value = tf.reshape(self.feat_value, shape=(-1, self.cate_field_size, 1))
            # self.embeddings = tf.multiply(self.embeddings, feat_value)
            self.X0 = tf.concat([tf.reshape(self.embeddings, shape=(-1, self.cate_field_size*self.embedding_size)), self.numeric_value], axis=1)
            #deep part
            self.y_deep = tf.nn.dropout(self.X0, keep_prob=self.dropout_keep_deep[0])
            for i in range(len(self.deep_layers)):
                self.y_deep = tf.add(tf.matmul(self.y_deep, self.weights['deep_layer_%s' %i]), self.weights['deep_bias_%s' %i])
                self.y_deep = self.deep_layers_activation(self.y_deep)
                self.y_deep = tf.nn.dropout(self.y_deep, self.dropout_keep_deep[i+1])
            #cross part
            self.X0 = tf.reshape(self.X0, shape=(-1, self.total_size, 1))
            X_l = self.X0
            for i in range(self.cross_layers_num):
                X_l = tf.tensordot(tf.matmul(self.X0, X_l, transpose_b=True), self.weights['cross_layer_%s' %i], axes=1) + self.weights['cross_bias_%s' %i] + X_l
            self.cross_output = tf.reshape(X_l, shape=(-1, self.total_size))
            #concat part
            concat_input = tf.concat([self.cross_output, self.y_deep], axis=1)
            self.output = tf.add(tf.matmul(concat_input, self.weights['feature_concat']), self.weights['concat_bias'])
            #loss
            if self.loss_type == "logloss":
                self.output = tf.nn.sigmoid(self.output, name="output")
                self.loss = tf.losses.log_loss(self.label, self.output)
            elif self.loss_type == "mse":
                self.loss = tf.reduce_mean(np.square(self.label-self.output))
            #l2 regularization
            if self.l2_reg > 0:
                self.loss += tf.contrib.layers.l2_regularizer(self.l2_reg)(self.weights['feature_concat'])
                for i in range(len(self.deep_layers)):
                    self.loss += tf.contrib.layers.l2_regularizer(self.l2_reg)(self.weights['deep_layer_%s' %i])
                for i in range(self.cross_layers_num):
                    self.loss += tf.contrib.layers.l2_regularizer(self.l2_reg)(self.weights['cross_layer_%s' %i])
            #optimizer
            if self.optimizer_type == 'adam':
                self.optimizer = tf.train.AdamOptimizer(learning_rate=self.decayed_learning_rate, beta1=0.9, beta2=0.999, epsilon=1e-8, name='optimizer').minimize(self.loss)
            elif self.optimizer_type == 'adagrad':
                self.optimizer = tf.train.AdagradOptimizer(learning_rate=self.decayed_learning_rate, initial_accumulator_value=1e-8, name='optimizer').minimize(self.loss)
            elif self.optimizer_type == 'gd':
                self.optimizer = tf.train.GradientDescentOptimizer(learning_rate=self.decayed_learning_rate, name='optimizer').minimize(self.loss)
            elif self.optimizer_type == 'momentum':
                self.optimizer = tf.train.MomentumOptimizer(learning_rate=self.decayed_learning_rate, momentum=0.95, name='optimizer').minimize(self.loss)

            self.saver = tf.train.Saver(max_to_keep=50)
            init = tf.global_variables_initializer()
            self.sess = tf.Session()
            self.sess.run(init)

    def get_batch(self, cate_Xi, numeric_Xv, y_label, index_batch):
        start = index_batch * self.batch_size
        end = (index_batch + 1) * self.batch_size
        end = end if end < len(y_label) else len(y_label)
        cate_Xi_batch = cate_Xi[start:end]
        # cate_Xv_batch = cate_Xv[start:end]
        numeric_Xv_batch = numeric_Xv[start:end]
        y_label_batch = y_label[start:end]
        return cate_Xi_batch, numeric_Xv_batch, y_label_batch

    def fit_on_batch(self, cate_Xi_batch, numeric_Xv_batch, y_label_batch, decayed_learning_rate):
        feed_dict = {self.feat_index: cate_Xi_batch,
                     # self.feat_value: cate_Xv_batch,
                     self.numeric_value: numeric_Xv_batch,
                     self.label: y_label_batch,
                     self.dropout_keep_deep: self.dropout_deep,
                     self.decayed_learning_rate: decayed_learning_rate
                     }
        self.sess.run(self.optimizer, feed_dict=feed_dict)

    def fit(self, cate_Xi, numeric_Xv, y_label, decayed_learning_rate):
        total_batch = math.ceil(len(y_label)/self.batch_size)
        for index_batch in range(total_batch):
            cate_Xi_batch, numeric_Xv_batch, y_label_batch = self.get_batch(cate_Xi, numeric_Xv, y_label, index_batch)
            # logger.info("==== get batch index:{}====".format(index_batch))
            self.fit_on_batch(cate_Xi_batch, numeric_Xv_batch, y_label_batch, decayed_learning_rate)
            logger.info("==== fit on batch index:{}====".format(index_batch))

    def predict(self, cate_Xi, numeric_Xv):
        y_temp = [[1]] * len(cate_Xi)
        y_rawPred = []
        total_batch = math.ceil(len(cate_Xi) / self.batch_size)
        for index_batch in range(total_batch):
            cate_Xi_batch, numeric_Xv_batch, y_temp_batch = self.get_batch(cate_Xi, numeric_Xv, y_temp, index_batch)
            feed_dict = {self.feat_index: cate_Xi_batch,
                         # self.feat_value: cate_Xv_batch,
                         self.numeric_value: numeric_Xv_batch,
                         self.dropout_keep_deep: [1.0] * len(self.dropout_deep)
                        }
            y_rawPred_batch = self.sess.run(self.output, feed_dict=feed_dict)
            #y_rawPred的shape是：（-1，）
            y_rawPred = np.append(y_rawPred, y_rawPred_batch)
        return y_rawPred

    def evaluate(self, cate_Xi, numeric_Xv, y_label):
        loss_sum = 0
        y_rawPred = []
        total_batch = math.ceil(len(cate_Xi) / self.batch_size)
        for index_batch in range(total_batch):
            cate_Xi_batch, numeric_Xv_batch, y_label_batch = self.get_batch(cate_Xi, numeric_Xv, y_label, index_batch)
            feed_dict = {self.feat_index: cate_Xi_batch,
                         # self.feat_value: cate_Xv_batch,
                         self.numeric_value: numeric_Xv_batch,
                         self.label: y_label_batch,
                         self.dropout_keep_deep: [1.0] * len(self.dropout_deep)
                         }
            y_rawPred_batch, loss_temp = self.sess.run([self.output, self.loss], feed_dict=feed_dict)
            # y_rawPred的shape是：（-1，）
            y_rawPred = np.append(y_rawPred, y_rawPred_batch)
            loss_sum = loss_sum + (loss_temp * self.batch_size)
            logger.info("==== y_rawPred on batch index:{}====".format(index_batch))
        loss = loss_sum/len(cate_Xi)
        y_label = np.reshape(y_label, (-1,))
        eval_score = self.eval_metric(y_label, y_rawPred)
        return eval_score, loss



















