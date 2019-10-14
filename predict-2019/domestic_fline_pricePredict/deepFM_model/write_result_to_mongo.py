# coding:utf-8
# -*- coding:utf-8 -*-

from pymongo import MongoClient, UpdateOne
import pandas as pd
import json
from datetime import datetime, timedelta
from logConfig import logger


class Result_To_Mongo():
    def __init__(self, params):
        self.params = params
        clientMongo = MongoClient("172.29.1.172:27017", authSource="flight",
                                  username='search', password='search@huoli123', authMechanism='MONGODB-CR')
        self.cursor = clientMongo.flight[self.params["mongoTableName_deepFM"]]
        self._init_df_transform()
        self._init_write_to_mongo()

    def _init_df_transform(self):
        # df的columns=['id_cityCode', 'queryDate', 'rawPred', 'trend']
        df = pd.read_csv(self.params["localFileName_deepFM_result"], header=0)
        df[['fn', 'org', 'dst', 'departDate']] = df['id_cityCode'].str.split('_', expand=True)
        # df['queryDate'] = self.params["generateDate_str2"]
        self.df = df[['id_cityCode', 'fn', 'org', 'dst', 'queryDate', 'trend']].rename(columns={'id_cityCode': "_id"})

    def _init_write_to_mongo(self):
        data = json.loads(self.df.to_json(orient='records'))
        requests = []
        for sample in data:
            requests.append(UpdateOne({'_id': sample.get('_id')}, {'$set': sample}, upsert=True))
        self.cursor.bulk_write(requests, ordered=False)
        logger.info("====update mongoDB finished====")
        self.cursor.delete_many({'queryDate': {'$lt': self.params["yesterday_str2"]}})




# def result_to_mongo(params):
#
#
#     clientMongo = MongoClient("172.29.1.172:27017", authSource="flight",
#                               username='search', password='search@huoli123', authMechanism='MONGODB-CR')
#     cursor = clientMongo.flight[params["mongoTableName_deepFM"]]
#
#     #df的columns=['id', 'queryDate', 'rawPred', 'tred']
#
#
#
#     def confirm_departDate(df):  # 判断起飞日期逻辑
#         if df.queryDate[5:] > df.departDate:
#             df['departDate'] = str(int(df.queryDate[:4]) + 1) + '-' + df.departDate
#         else:
#             df['departDate'] = df.queryDate[:4] + '-' + df.departDate
#         return df['departDate']
#
#     df = pd.read_csv(params["localFileName_deepFM_result"], header=0)
#
#
#
#
#     df[['fn', 'org', 'dst', 'departDate']] = df['id'].str.split('_', expand=True)
#
#     df["departDate"] = df.apply(confirm_departDate, axis=1)
#
#     def modify_id(df):
#         id_list = df.id.split('_')
#         id_list[-1] = df.departDate
#         df.id = "_".join(id_list)
#         return df['id']
#     df['id'] = df.apply(modify_id, axis=1)
#
#     # def modify_queryDate(date_str):
#     #     date_add1 = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
#     #     date_add1 = datetime.strftime(date_add1, "%Y-%m-%d")
#     #     return date_add1
#
#
#
#     df['queryDate'] = params["generateDate_str2"]
#
#     df_select = df[['id', 'fn', 'org', 'dst', 'queryDate', 'trend']].rename(columns = {'id': "_id"})
#
#     data = json.loads(df_select.to_json(orient='records'))
#     if params["mongoTableName_deepFM"] in clientMongo.flight.collection_names():
#         cursor.delete_many({})
#     cursor.insert_many(data)
#     print("===mongo OK====")

