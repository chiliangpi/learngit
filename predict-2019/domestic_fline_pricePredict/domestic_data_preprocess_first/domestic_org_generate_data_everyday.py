# coding:utf-8
# -*- coding:utf-8 -*-

import os, time, re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient
from math import radians, sin, cos, asin, sqrt, ceil
from logging import handlers
import logging
from hdfs import client
from logConfig import logger
import config
import utils

clientMongo = MongoClient("172.29.1.172", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR', connect=False)
cursor = clientMongo.flight.historyLowPrice_fn_domestic
cursor1 = clientMongo.flight.fd_domestic_a
cursor2 = clientMongo.flight.globalAirport
cursor3 = clientMongo.flight.dynamic_seatleft
cursor4 = clientMongo.flight.flightInfoBase


def lowPrice_train_data_add(params):
    columns = ["queryDate", 'price', 'id', 'org', 'dst']
    yesterday_monthDay_str = (params["generateDate"] - timedelta(days=1)).strftime('%m-%d')
    with open(params["localFileName_org_lowPrice_data_add"], 'w') as f:
        f.write(','.join(columns))
        f.write('\n')
        f.seek(2)
        for sample in cursor.find({'_id': {"$regex": r'.*{}$'.format(yesterday_monthDay_str)}}):
            lowPrice_id = sample.get('_id')
            del sample['_id']
            org = sample.get('dairport')
            del sample['dairport']
            dst = sample.get('aairport')
            del sample['aairport']
            df = pd.DataFrame.from_dict(sample, orient='index').reset_index().rename(
                columns={'index': 'queryDate', 0: 'price'})
            df['id'] = lowPrice_id
            df['org'] = org
            df['dst'] = dst
            df.to_csv(params["localFileName_org_lowPrice_data_add"], header=False, index=False, mode='a')
        logger.info("=====\"{}\" finished======".format(params["localFileName_org_lowPrice_data_add"].split('/')[-1]))
    utils.delete_before4_localData(params["localFileName_org_lowPrice_data_add"], params)
    utils.upload_to_hdfs(params["localFileName_org_lowPrice_data_add"], params["sparkDirName_org_lowPrice_data_add"], params)


def orgPrice_data(params):
    columns = ["orgPrice_id", "fc", "orgPrice"]
    with open(params["localFileName_org_orgPrice_data"], 'w') as f:
        f.write(','.join(columns))
        f.write('\n')
        for sample in cursor1.find({'date': {'$gte': datetime.today().strftime("%Y-%m-%d")}}):
            orgPrice_id = sample.get('_id')
            del sample['_id']
            del sample["date"]
            del sample["ut"]
            del sample["src"]
            for key, value in sample.items():
                try:
                    orgPrice = value.get('Y').get('price')
                    content = ','.join([orgPrice_id, key, orgPrice])
                    f.write(content + '\n')
                except:
                    continue
        logger.info("====\"{}\" finished====".format(params["localFileName_org_orgPrice_data"].split('/')[-1]))
    utils.delete_before4_localData(params["localFileName_org_orgPrice_data"], params)
    utils.upload_to_hdfs(params["localFileName_org_orgPrice_data"], params["sparkDirName_org_orgPrice_data"], params)


def globalAirport_data(params):
    columns = ["Airport_code", "latitude", "longitude"]
    with open(params["localFileName_org_Airport_data"], 'w') as f:
        f.write(','.join(columns))
        f.write('\n')
        for sample in cursor2.find({}):
            Airport_code = sample.get("_id")
            latitude = sample.get("latitude")
            longitude = sample.get("longitude")
            try:
                content = ','.join([Airport_code, latitude, longitude])
                f.write(content + '\n')
            except:
                continue
        logger.info("====\"{}\" finished====".format(params["localFileName_org_Airport_data"].split('/')[-1]))
    utils.delete_before4_localData(params["localFileName_org_Airport_data"], params)
    utils.upload_to_hdfs(params["localFileName_org_Airport_data"], params["sparkDirName_org_Airport_data"], params)


def seatleft_data_add(params):
    tomorrow_str2 = datetime.strftime(params["generateDate"] + timedelta(days=1), '%Y-%m-%d')
    today_monthDay = datetime.strftime(params["generateDate"], '%m-%d')
    yesterday_str2 = (params["generateDate"] - timedelta(days=1)).strftime('%Y-%m-%d')
    after30_str2 = datetime.strftime(params["generateDate"] + timedelta(days=30), '%Y-%m-%d')
    monthDay_list = pd.date_range(start=yesterday_str2, end=after30_str2, freq='d').map(lambda x: datetime.strftime(x, '%m-%d')).to_list()
    monthDay_list.remove(today_monthDay)
    columns = ['queryDatetime', 'seatLeft', 'seatLeft_id']
    with open(params["localFileName_org_seatLeft_data_add"], 'w') as f:
        f.write(','.join(columns))
        f.write('\n')
        f.seek(2)
        for monthDay in monthDay_list:
            for sample in cursor3.find({'_id': {"$regex": r'.*{}$'.format(monthDay)}}):
                seatLeft_id = sample.get('_id')
                df = pd.DataFrame.from_dict(sample.get('fc'), orient='index').reset_index().rename(columns={'index': 'queryDatetime', 0: 'seatLeft'})
                df['seatLeft_id'] = seatLeft_id
                df.to_csv(params["localFileName_org_seatLeft_data_add"], header=False, index=False, mode='a')
        logger.info("====\"{}\" finished====".format(params["localFileName_org_seatLeft_data_add"].split('/')[-1]))
    utils.delete_before4_localData(params["localFileName_org_seatLeft_data_add"], params)
    utils.upload_to_hdfs(params["localFileName_org_seatLeft_data_add"], params["sparkDirName_org_seatLeft_data_add"], params)


def infoBase_data(params):
    columns = ["infoBase_id", "departTime", "arriveTime", "isShare"]
    with open(params["localFileName_org_infoBase_data"], 'w') as f:
        f.write(','.join(columns))
        f.write('\n')
        for sample in cursor4.find({'date': {'$gte': datetime.today().strftime("%Y-%m-%d")}}):
            infoBase_id = sample.get("_id")
            departtime = sample.get('origindeparttime')
            arrivetime = sample.get('originarrivetime')
            isShare = sample.get('isshare')
            try:
                content = ','.join([infoBase_id, departtime, arrivetime, str(isShare)])
                f.write(content + '\n')
            except:
                continue
        logger.info("====\"{}\" finished====".format(params["localFileName_org_infoBase_data"].split('/')[-1]))
    utils.delete_before4_localData(params["localFileName_org_infoBase_data"], params)
    utils.upload_to_hdfs(params["localFileName_org_infoBase_data"], params["sparkDirName_org_infoBase_data"], params)


def lowPrice_online_data(params):
    tomorrow_str2 = datetime.strftime(params["generateDate"] + timedelta(days=1), '%Y-%m-%d')
    after30_str2 = datetime.strftime(params["generateDate"] + timedelta(days=30), '%Y-%m-%d')
    monthDay_list = pd.date_range(start=tomorrow_str2, end=after30_str2, freq='d').map(
        lambda x: datetime.strftime(x, '%m-%d'))
    columns = ["queryDate", 'price', 'id', 'org', 'dst']
    with open(params["localFileName_org_lowPrice_onlineData"], 'w') as f:
        f.write(','.join(columns))
        f.write('\n')
        for monthDay in monthDay_list:
            #对于online数据，根据id的条件筛选，然后只要拿到最大查询日期对应的价格即可（一条记录）。进一步的判断信息在spark上完成
            #如果最大查询日期距离今天相隔7天以内，即该id在最近7天有价格记录，则将最大查询日期改为yesterday（模型只能通过昨天的信息，预测今天之后的价格趋势）
            for sample in cursor.find({'_id': {"$regex": r'.*{}$'.format(monthDay)}}):
                lowPrice_id = sample.get('_id')
                del sample['_id']
                org = sample.get('dairport')
                del sample['dairport']
                dst = sample.get('aairport')
                del sample['aairport']
                # df = pd.DataFrame.from_dict(sample, orient='index').reset_index().rename(
                #     columns={'index': 'queryDate', 0: 'price'})
                queryDate = max(sample.keys())
                price = sample.get(queryDate)
                # historyLowPrice_fn_domestic中有异常，例如_id="3U3100_null_null_09-24"
                try:
                    content = ','.join([queryDate, price, lowPrice_id, org, dst])
                    f.write(content + '\n')
                except:
                    continue
        logger.info(
            "=====\"{}\" finished======".format(params["localFileName_org_lowPrice_onlineData"].split('/')[-1]))
    utils.delete_before4_localData(params["localFileName_org_lowPrice_onlineData"], params)
    utils.upload_to_hdfs(params["localFileName_org_lowPrice_onlineData"],
                   params["sparkDirName_org_lowPrice_onlineData"], params)
