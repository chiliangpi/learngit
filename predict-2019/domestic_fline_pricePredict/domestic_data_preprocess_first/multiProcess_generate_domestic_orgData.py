# coding:utf-8
# -*- coding:utf-8 -*-

###########
#多进程整理国内低价的原始数据
###########
import os, time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymongo import MongoClient
from functools import lru_cache
from math import radians, sin, cos, asin, sqrt, ceil
from multiprocessing import Pool, Process
from io import StringIO
import logging
from logging import handlers

class Timer():
    def __init__(self):
        self.startTime = None

    def start(self):
        self.startTime = time.time()

    # def stop(self):
    #     self.endTime = time.time()
    #     logger.info('Time taken: {} ms'.format(round((self.endTime - self.startTime)*1000)))

    def cost(self):
        self.endTime = time.time()
        self.costTime = round((self.endTime - self.startTime)*1000)
        # logger.info('costTime is: '.format(self.costTime))
        return self.costTime

def confirm_departDate(df):  #判断起飞日期逻辑
    if df.checkDate[5:] > df.departMonthDay:
        df['departDate'] = str(int(df.checkDate[:4]) + 1) + '-' + df.departMonthDay
    else:
        df['departDate'] = df.checkDate[:4] + '-' + df.departMonthDay
    return df['departDate']


def roll_minPrice(x, priceList):  # 查询日期至起飞日期期间内的最低价格的计算逻辑, 起飞当天的最低价格为None
    try:
        priceList.pop(0)
        return min(priceList)
    except:
        return None

def trend(df):  #根据查询日期至起飞日期期间内的最低价格，得出当前日期之后的价格趋势，如果当前价格高于futureMinPrice 4%以上，视为降价，trend=1， 其余情况trend=0
    try:
        if (df['price'] - df['futureMinPrice']) / df['price'] > 0.04:  # 下降
            return 1
        elif (df['price'] - df['futureMinPrice']) / df['price'] <= 0.04:  # 下降
            return 0
        else:  # futureMinPrice存在缺失的情况
            return None
    except:  # price存在缺失值的情况
        return None


def org_price(id, cursor1):
    Airlines = id[:2]
    flag = 0
    for i in range(32):  #在fd_domestic_a(原价)表中，从当前日期向后找60天的数据，如果能找到，flag=1(即mongoDB中距离今天最近的数据作为原价)
        idDate = datetime.strftime(datetime.today() + timedelta(i), '%Y-%m-%d')
        List = id.split('_')[1:3] + [idDate, 'fd']
        orgPrice_id = '_'.join(List)
        if cursor1.find_one({'_id': orgPrice_id}):
            flag = 1
            break
    if flag == 1:
        try:
            orgPrice = int(cursor1.find_one({'_id': orgPrice_id}).get(Airlines).get('Y').get('price'))
        except:
            orgPrice = None
    else:
        orgPrice = None
    return orgPrice

def discount_rate(df):  #根据机票原价和当前价格，计算折扣率
    try:
        discount = round(df.price / df.orgPrice * 100, 2)
    except:
        discount = None
    return discount

'''
给定两个地址的坐标，计算返回两地相距多少公里 
demo:
address1=成都 lon1 = 104.071000 lat1 = 30.670000
address2=宜宾 104.622000 lat2 = 28.765000
'''
# @lru_cache(maxsize=1024 * 1024 * 512)
def get_address_distance(id, cursor2):
    distance_id1 = id.split('_')[1]
    distance_id2 = id.split('_')[2]
    try:
        lon1 = float(cursor2.find_one({'_id': distance_id1}).get('longitude'))
        lat1 = float(cursor2.find_one({'_id': distance_id1}).get('latitude'))
        lon2 = float(cursor2.find_one({'_id': distance_id2}).get('longitude'))
        lat2 = float(cursor2.find_one({'_id': distance_id2}).get('latitude'))
        # 将十进制度数转化为弧度
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine公式
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # 地球平均半径，单位为公里
        # 返回结果除以1000以公里为单位保留两位小数
        distance = round((c * r * 1000) / 1000, 2)
    except:
        distance = None
    return distance

def isShare_convert(isShareStr):  #原始数据是'true'、'false'布尔型，转换成1或0
    if isShareStr is True:
        return 1
    elif isShareStr is False:
        return 0
    else:
        return None


def departTime_isShare(id, cursor4):  #查询flightInfoBase距离今天最近的数据，作为起飞时间（24时制）和isShare
    List_temp = id.split('_')
    flag = 0
    for i in range(32):
        idDate = datetime.strftime(datetime.today() + timedelta(i), '%Y-%m-%d')
        List = List_temp[1:3] + [List_temp[0], idDate]
        infoBase_id = '_'.join(List)
        if cursor4.find_one({'_id': infoBase_id}):
            flag = 1
            break
    if flag == 1:
        departTime = cursor4.find_one({'_id': infoBase_id}).get('origindeparttime')
        isShareStr = cursor4.find_one({'_id': infoBase_id}).get('isshare')
        isShare = isShare_convert(isShareStr)
    else:
        departTime = None
        isShare = None
    return departTime, isShare

def processRun(idList):
    timerTotal = Timer()
    timer = Timer()
    clientMongo = MongoClient("172.29.1.172", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR', connect=False)
    cursor = clientMongo.flight.historyLowPrice_fn_domestic
    cursor1 = clientMongo.flight.fd_domestic_a
    cursor2 = clientMongo.flight.globalAirport
    cursor3 = clientMongo.flight.dynamic_seatleft
    cursor4 = clientMongo.flight.flightInfoBase
    fileName = 'featureAnalysis_orgData_domestic_' + str(os.getpid()) + '_' + todayStr1 + '.csv'
    dirName = '/data/search/predict-2019/featureAnalysis_orgData_domestic_' + todayStr1 + '.file'
    # 根据需求写出列名
    # columnNames = ','.join(
    #     ['checkDate', 'price', 'fn', 'org', 'dst', 'lowPrice_id', 'fc', 'departDate', 'dayofweek', 'futureMinPrice',
    #      'trend', 'orgPrice', 'discount', 'distance', 'seatLeft', 'departTime', 'isShare'])
    # if fileName in os.listdir('/data/search/predict-2019/'):
    #     os.remove('/data/search/predict-2019/' + fileName)
    if not os.path.exists(dirName):
        os.makedirs(dirName)
    f = open(dirName + '/' + fileName, 'a')
    stringIO = StringIO()
    count = 0
    # f.write(columnNames + '\n')
    for lowPrice_id in idList:
        timerTotal.start()
        timer.start()
        sample = cursor.find_one({'_id': lowPrice_id})
        del sample['_id']
        aairport = sample.get('aairport')
        del sample['aairport']
        dairport = sample.get('dairport')
        del sample['dairport']
        idList = lowPrice_id.split('_')
        idList[1] = dairport
        idList[2] = aairport
        id = '_'.join(idList)
        # 构建departDate,dayofweek,futureMinPrice,trend特征
        df = pd.DataFrame.from_dict(sample, orient='index').reset_index().rename(
            columns={'index': 'checkDate', 0: 'price'})
        for name, value in zip(['fn', 'org', 'dst', 'departMonthDay', 'lowPrice_id'], id.split('_') + [lowPrice_id]):
            df[name] = value
        df['fc'] = df.fn.str[:2]
        df['departDate'] = df.apply(confirm_departDate, axis=1)
        #Monday:1, Sunday:7
        df['dayofweek'] = pd.to_datetime(df.departDate).dt.dayofweek + 1
        df['price'] = df['price'].astype('int')

        df['startDate'] = df.departDate.apply(
            lambda x: datetime.strftime(datetime.strptime(x, '%Y-%m-%d') + timedelta(-30), '%Y-%m-%d'))
        # 国内航线的有效数据范围是距起飞日期30天以内,且起飞日期小于当前日期（起飞日期大于等于当前日期，无法判定后续价格涨跌）
        df = df[(df.departDate >= '2018-01-01') & (df.departDate < todayStr2) & (df.checkDate >= df.startDate)]
        columnNamesList = df.columns.to_list()
        columnNamesList.append('futureMinPrice')
        groupDF = pd.DataFrame(columns=columnNamesList)  # 创建空的dataframe，供拼接使用
        for name, group in df.groupby('departDate'):  # 计算查询日期至起飞日期期间内的最低价格，按照起飞日期分组后，如果数据量少于15条，则不考虑这部分数据
            if len(group) >= 15:
                group.dropna(axis=0, inplace=True)
                group = group.sort_values('checkDate')
                priceList = group.price.to_list()
                priceList.pop(0)
                group['futureMinPrice'] = group.price.apply(roll_minPrice, args=(priceList,))
                groupDF = pd.concat([groupDF, group], axis=0)
            else:
                continue
        df = groupDF.drop(columns=['departMonthDay', 'startDate'], axis=1)
        if df.empty:  #如果数据量不足，导致dataframe为空，则直接跳至下一次循环
            continue
        # logger.info('process {} groupDF costTime {} ms'.format(os.getpid(), timer.cost()))
        timer.start()
        df['trend'] = df.apply(trend, axis=1)
        # logger.info('process {} trend costTime {} ms'.format(os.getpid(), timer.cost()))

        # 匹配机票的原价，计算折扣率
        timer.start()
        orgPrice = org_price(id, cursor1)
        df['orgPrice'] = orgPrice
        df['discount'] = df.apply(discount_rate, axis=1)
        # logger.info('process {} discount costTime {} ms'.format(os.getpid(), timer.cost()))

        # 匹配航线距离
        timer.start()
        distance = get_address_distance(id, cursor2)
        df['distance'] = distance
        # logger.info('process {} distance costTime {} ms'.format(os.getpid(), timer.cost()))

        ####匹配客座率
        timer.start()
        try:
            seatLeft_df = pd.Series(cursor3.find_one({'_id': id}).get('fc')).reset_index().rename(
                columns={'index': 'dateTime', 0: 'seatLeft'})
            seatLeft_df['seatLeft'] = seatLeft_df.seatLeft.str[:-1].astype('float')
            seatLeft_df[['date', 'time']] = seatLeft_df.dateTime.str.split(' ', expand=True)
            seatLeft_df = seatLeft_df.groupby('date', as_index=False).apply(lambda x: x[x.time == x.time.max()])
            df = df.merge(seatLeft_df, 'left', left_on='checkDate', right_on='date').drop(
                ['dateTime', 'date', 'time'], axis=1)
        except:
            df['seatLeft'] = None
        # logger.info('process {} seatLeft costTime {} ms'.format(os.getpid(), timer.cost()))

        # 匹配起飞时间和是否共享（isshare）
        timer.start()
        departTime, isShare = departTime_isShare(id, cursor4)
        df['departTime'] = departTime
        df['isShare'] = isShare
        # logger.info('process {} departTime_isShare costTime {} ms'.format(os.getpid(), timer.cost()))

        #每循环100次，将数据写入文件

        df.to_csv(stringIO, header=False, index=False, mode='a')
        count += 1
        if count % 100 == 0:
            timer.start()
            f.write(stringIO.getvalue())
            stringIO = StringIO()
        #     logger.info('====process {} writeStringIO costTime {} ms===='.format(os.getpid(), timer.cost()))
        # logger.info('====process {} one sample timeTotal costTime {} ms===='.format(os.getpid(), timerTotal.cost()))
    f.write(stringIO.getvalue())
    f.close()

def generate_idList(idList_fileName):
    clientMongo = MongoClient("172.29.1.172", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR', connect=False)
    cursor = clientMongo.flight.historyLowPrice_fn_domestic
    stringIO_temp = StringIO()
    count_temp = 0
    with open(idList_fileName, 'a') as f_temp:  # 遍历mongoDB中的ID，写入文件
        for sample in cursor.find():
            count_temp += 1
            stringIO_temp.write(sample.get('_id') + ',')
            if count_temp % 1000 == 0:
                f_temp.write(stringIO_temp.getvalue())
                stringIO_temp = StringIO()
        f_temp.write(stringIO_temp.getvalue())


if __name__ == '__main__':
    # generateDate = datetime.today()
    # generateDate = datetime.strptime('2019-07-26', '%Y-%m-%d')
    # generateDate_str = datetime.strftime(generateDate, '%Y-%m-%d')
    todayStr1 = datetime.strftime(datetime.now(), '%Y%m%d')
    todayStr2 = datetime.strftime(datetime.now(), '%Y-%m-%d')
    logFileName = '/data/search/predict-2019/script/intl_pricePrediction.log'
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='[%(asctime)s] - %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    rotating_file = handlers.RotatingFileHandler(filename=logFileName,  mode='a', maxBytes=1024*1024*1024, backupCount=3)
    rotating_file.setLevel(logging.DEBUG)
    rotating_file.setFormatter(formatter)
    logger.addHandler(rotating_file)
    idList_fileName = '/data/search/predict-2019/domestic_idList.txt'
    generate_idList(idList_fileName)
    logger.info("====generate idList_file finished====")
    with open(idList_fileName, 'r') as f_temp:
        str_L = f_temp.readline()
    L = str.split(str_L, ',')
    L.pop()
    n = ceil(len(L)/100)
    List = [L[i:i+n] for i in range(0, len(L), n)]
    p = Pool(14)
    for i in range(len(List)):
        p.apply_async(processRun, args=(List[i],))
    p.close()
    p.join()