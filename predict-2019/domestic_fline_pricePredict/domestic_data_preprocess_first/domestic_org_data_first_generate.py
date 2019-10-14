# coding:utf-8
# -*- coding:utf-8 -*-

import os, time, re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient
from math import radians, sin, cos, asin, sqrt, ceil
from io import StringIO
from logging import handlers
import logging
from hdfs import client
from logConfig import logger
import config
from multiprocessing import Pool, Process
import utils

clientMongo = MongoClient("172.29.1.172", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR', connect=False)
cursor = clientMongo.flight.historyLowPrice_fn_domestic
cursor1 = clientMongo.flight.fd_domestic_a
cursor2 = clientMongo.flight.globalAirport
cursor3 = clientMongo.flight.dynamic_seatleft
cursor4 = clientMongo.flight.flightInfoBase


def lowPrice_data_main(params):
    write_idList(params, cursor)
    utils.delete_before4_localData(params["localFileName_lowPrice_idList"], params)

    if os.path.exists(params["localFileName_org_lowPrice_data"]):
        os.remove(params["localFileName_org_lowPrice_data"])
    columns = ["queryDate", 'price', 'id', 'org', 'dst']
    with open(params["localFileName_org_lowPrice_data"], 'a') as f_write:
        f_write.write(",".join(columns))
        f_write.write("\n")
        f_write.seek(2)
        p = Pool(10)
        counter = 0
        with open(params["localFileName_lowPrice_idList"], 'r') as f_read:
            for line in f_read:
                counter += 1
                L = line.strip().split(',')
                p.apply_async(lowPrice_data, args=(params, L, ))
        p.close()
        p.join()
        logger.info("=====\"{}\" finished======".format(params["localFileName_org_lowPrice_data"].split('/')[-1]))
    utils.delete_before4_localData(params["localFileName_org_lowPrice_data"], params)
    utils.upload_to_hdfs(params["localFileName_org_lowPrice_data"], params["sparkDirName_org_lowPrice_data"], params)



def lowPrice_data(params, L):
    for sample in cursor.find({"_id": {"$in": L}}):  #备注正则匹配cursor.find({'_id':{"$regex": r'^abc'}})
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
        df.to_csv(params["localFileName_org_lowPrice_data"], header=False, index=False, mode='a')



def write_idList(params, cursor):
    stringIO_temp = StringIO()
    count = 0
    with open(params["localFileName_lowPrice_idList"], 'w') as f:  # 遍历mongoDB中的ID，写入文件
        for sample in cursor.find():
            count += 1
            stringIO_temp.write(sample.get('_id') + ',')
            if count % 10000 == 0:
                content = stringIO_temp.getvalue().strip(',') + '\n'
                f.write(content)
                stringIO_temp = StringIO()
        content = stringIO_temp.getvalue().strip(',') + '\n'
        f.write(content)
        logger.info("====\"{}\" finished====".format(params["localFileName_lowPrice_idList"].split('/')[-1]))


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


def seatleft_data(params):
    columns = ['queryDatetime', 'seatLeft', 'seatLeft_id']
    with open(params["localFileName_org_seatLeft_data"], 'w') as f:
        f.write(','.join(columns))
        f.write('\n')
        f.seek(2)
        for sample in cursor3.find({}):
            seatLeft_id = sample.get('_id')
            df = pd.DataFrame.from_dict(sample.get('fc'), orient='index').reset_index().rename(columns={'index': 'queryDatetime', 0: 'seatLeft'})
            df['seatLeft_id'] = seatLeft_id
            df.to_csv(params["localFileName_org_seatLeft_data"], header=False, index=False, mode='a')
        logger.info("====\"{}\" finished====".format(params["localFileName_org_seatLeft_data"].split('/')[-1]))
    utils.delete_before4_localData(params["localFileName_org_seatLeft_data"], params)
    utils.upload_to_hdfs(params["localFileName_org_seatLeft_data"], params["sparkDirName_org_seatLeft_data"], params)


def infoBase_data(params):
    columns = ["infoBase_id", "departTime", "arriveTime", "isShare"]
    with open(params["localFileName_org_infoBase_data"], 'w') as f:
        f.write(','.join(columns))
        f.write('\n')
        #使用$gte：today的过滤条件，数量减少的很少，所以不加过滤条件
        for sample in cursor4.find({'date': {'$gte': datetime.today().strftime("%Y-%m-%d")}}):
        # for sample in cursor4.find({}):
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













