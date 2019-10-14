# coding:utf-8
# -*- coding:utf-8 -*-
import logging
from logging import handlers
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from flask import Flask, request, abort, make_response, jsonify
from pymongo import MongoClient
import requests
from functools import lru_cache


logFileName = '/data/search/predict-2019/script/intl_pricePrediction.log'
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt='[%(asctime)s] - %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
rotating_file = handlers.RotatingFileHandler(filename=logFileName,  mode='a', maxBytes=1024*1024*1024, backupCount=3)
rotating_file.setLevel(logging.DEBUG)
rotating_file.setFormatter(formatter)
logger.addHandler(rotating_file)

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
        logger.info('costTime is: '.format(self.costTime))
        return self.costTime

class App():
    timer = Timer()
    @lru_cache(maxsize=1024*1024*1024, typed=False)
    def historyPrice(self, org, dst, isReturn_type, isDirect_type, departDate_date, currentDate_date):
        """
        查找当前日期之前半年（180天）的数据，取出最近的14条数据作为历史价格（尽可能满足14条，但是可以不足14条数据）
        :return:
        """
        monthDay = datetime.strftime(departDate_date, '%m-%d')
        _id_monthDay = org + '_' + dst + '_' + isReturn_type + '_' + 'Y' + '_' + isDirect_type + '_' + monthDay
        mongoDict = cursor2.find_one({'_id': _id_monthDay}, {'_id': 0})
        if mongoDict:
            history180Days_date = pd.date_range(end=currentDate_date + timedelta(days=-1), periods=180, freq='D')\
                                        .map(lambda x: datetime.strftime(x, '%Y-%m-%d'))\
                                        .sort_values(ascending=False)
            historyPriceData = {}
            count = 0
            for day in history180Days_date:
                if mongoDict.__contains__(day):
                    historyPriceData[day] = int(mongoDict.get(day))
                    count += 1
                    if count == 14:
                        break
        else:  #找不到对应_id的历史数据
            historyPriceData = {}
        return historyPriceData

    # def historyPrice(self, org, dst, isReturn_type, isDirect_type, departDate_date, currentDate_date):
    #     """
    #     单程返回历史价格，往返不返回历史价格；
    #     依据_id查找到一条mongodict数据，首先锁定当前日期前一个月的数据，然后取出最近的14条有效数据（最多14条），
    #     如果有效数据>3条，则返回与当前日期最邻近的价格，其余价格的最大值和最小值；如果有效数据<=3条，则不必比较大小，直接顺次返回数据
    #     """
    #     monthDay = datetime.strftime(departDate_date, '%m-%d')
    #     _id_monthDay = org + '_' + dst + '_' + isReturn_type + '_' + 'Y' + '_' + isDirect_type + '_' + monthDay
    #     mongoDict = cursor2.find_one({'_id': _id_monthDay}, {'_id': 0})
    #     if mongoDict:
    #         history30Days_date = pd.date_range(end=currentDate_date+timedelta(days=-1), periods=30, freq='D').map(lambda x: datetime.strftime(x, '%Y-%m-%d'))
    #         history14Days_date = []
    #         count = 0
    #         for day in history30Days_date:
    #             if mongoDict.__contains__(day):
    #                 history14Days_date.append(day)
    #                 count += 1
    #                 if count == 14:
    #                     break
    #         history14Days_date.sort(reverse=True)
    #         if count == 0:
    #             beforeCurrentPrice = ''
    #             beforeCurrentDate = ''
    #             H_highPriceDate = ''
    #             H_highPrice = ''
    #             H_lowPriceDate = ''
    #             H_lowPrice = ''
    #         elif count == 1:
    #             beforeCurrentDate = history14Days_date[0]
    #             beforeCurrentPrice = int(mongoDict[beforeCurrentDate])
    #             H_highPriceDate = ''
    #             H_highPrice = ''
    #             H_lowPriceDate = ''
    #             H_lowPrice = ''
    #         elif count == 2:
    #             beforeCurrentDate = history14Days_date[0]
    #             beforeCurrentPrice = int(mongoDict[beforeCurrentDate])
    #             H_highPriceDate = history14Days_date[1]
    #             H_highPrice = int(mongoDict[H_highPriceDate])
    #             H_lowPriceDate = ''
    #             H_lowPrice = ''
    #         elif count == 3:
    #             beforeCurrentDate = history14Days_date[0]
    #             beforeCurrentPrice = int(mongoDict[beforeCurrentDate])
    #             H_highPriceDate = history14Days_date[1]
    #             H_highPrice = int(mongoDict[H_highPriceDate])
    #             H_lowPriceDate = history14Days_date[2]
    #             H_lowPrice = int(mongoDict[H_lowPriceDate])
    #         else:  #count>=4
    #             beforeCurrentDate = history14Days_date[0]
    #             beforeCurrentPrice = int(mongoDict[beforeCurrentDate])
    #             H_lowPrice = 99999
    #             H_highPrice = 0
    #             for day in history14Days_date[1:]:
    #                 if H_lowPrice > int(mongoDict.get(day, 99999)):
    #                     H_lowPrice = int(mongoDict[day])
    #                     H_lowPriceDate = day
    #                 if H_highPrice < int(mongoDict.get(day, 0)):
    #                     H_highPrice = int(mongoDict[day])
    #                     H_highPriceDate = day
    #     else:  #找不到数据
    #         beforeCurrentDate = ''
    #         beforeCurrentPrice = ''
    #         H_highPriceDate = ''
    #         H_highPrice = ''
    #         H_lowPriceDate = ''
    #         H_lowPrice  = ''
    #     return beforeCurrentDate, beforeCurrentPrice, H_highPriceDate, H_highPrice, H_lowPriceDate, H_lowPrice

    @lru_cache(maxsize=1024*1024*1024, typed=False)
    def priceRange(self, org, dst, isReturn_type, isDirect_type, departDate_date, currentDate_date):
        monthStr = departDate_date.strftime('%m')
        _id_month = org + '_' + dst + '_' + isReturn_type + '_' + isDirect_type + '_' + monthStr
        mongoDict = cursor3.find_one({'_id': _id_month}, {'_id': 0, 'priceRange_basePercentile': 1})
        if mongoDict:
            priceRange_low = int(float(mongoDict['priceRange_basePercentile'][2][1]))
            priceRange_high = int(float(mongoDict['priceRange_basePercentile'][7][1]))
        else:
            priceRange_low = ''
            priceRange_high = ''
        return priceRange_low, priceRange_high

    def responseResult(self, _id, cabin, org, dst, isReturn_type, isDirect_type, departDate_str, departDate_date, currentDate_str, currentDate_date, currentPrice, mongoDict):
        """

        """
        gouwucheTitle = "智选购票 尽在购物车"
        gouwucheContent = "管家AI帮您实时监测价格波动，帮您发现低价"
        if cabin == 'Y':
            priceRange_low, priceRange_high = self.priceRange(org, dst, isReturn_type, isDirect_type, departDate_date, currentDate_date)
            historyPriceData = self.historyPrice(org, dst, isReturn_type, isDirect_type, departDate_date, currentDate_date)
            if mongoDict:
                lowPriceDate = mongoDict.get('lowPriceDate')
                lowPrice = mongoDict.get('lowPrice')
                highPriceDate = mongoDict.get('highPriceDate', lowPriceDate)
                highPrice = mongoDict.get('highPrice', lowPrice)
                departPrice = mongoDict.get('departPrice', highPrice)
                futurePriceData = {currentDate_str: currentPrice,
                                   highPriceDate: highPrice,
                                   lowPriceDate: lowPrice,
                                   departDate_str: departPrice}
                if lowPrice:
                    if (currentPrice - lowPrice) / currentPrice > 0.1:
                        trend = 2
                        adviseTitle = '预测未来价格可能会下降'
                        adviseContent = '通过大数据分析，未来价格下降概率较大。大数据预测也会有误差，仅供参考。'
                    elif abs(currentPrice - lowPrice) / currentPrice <= 0.1:
                        trend = 0
                        adviseTitle = '预测未来价格可能趋于平稳'
                        adviseContent = '通过大数据分析，未来价格趋于平稳。现在可以预订，大数据预测也会有误差，仅供参考。'
                    else:  # (currentPrice-lowPrice)/currentPrice < -0.1
                        trend = 1
                        adviseTitle = '预测未来价格可能会上涨'
                        adviseContent = '通过大数据分析，未来价格上涨概率较大，建议现在预订。大数据预测也会有误差，仅供参考'
                else:  #没有找到lowPrice的值
                    trend = -1
                    adviseTitle = ''
                    adviseContent = ''
                    futurePriceData = {}
            else:  #monggoDB找不到_id的情况
                trend = -1
                adviseTitle = ''
                adviseContent = ''
                futurePriceData = {}
        else:   #cabin不等于'Y'的情况
            trend = '-1'
            adviseTitle = ''
            adviseContent = ''
            historyPriceData = {}
            futurePriceData ={}
            priceRange_low = ''
            priceRange_high = ''
        quxianDict = dict(historyPriceData, **futurePriceData)
        keyList_sub = list(quxianDict.keys())
        keyList_sub.sort()
        quxianList = []
        for key in keyList_sub:
            quxianList.append(dict({key:quxianDict[key]}))
        returnDict = {'trend': trend,
                      'adviseTitle': adviseTitle,
                      'adviseContent': adviseContent,
                      'quxian': quxianList,
                      'gouwucheTitle': gouwucheTitle,
                      'gouwucheContent': gouwucheTitle,
                      'priceRange_low': priceRange_low,
                      'priceRange_high': priceRange_high,
                      'currentPrice': currentPrice}
        keyList = list(returnDict.keys())
        keyList.remove('trend')
        for key in keyList:  #检查返回的json数据，如果value为空，则删掉
            if not returnDict.get(key):
                del returnDict[key]
        logger.info('returnDict: {}'.format(returnDict))
        return returnDict

    @lru_cache(maxsize=1024 * 1024 * 1024, typed=False)
    def mongoFind(self, _id):
        mongoDict = cursor1.find_one({'_id': _id})
        return mongoDict

    def getParams(self):
        org = request.args.get('acity')  # 出发地
        dst = request.args.get('dcity')  # 到达地
        departDate_str = request.args.get('date')  # 出发日期
        cabin = request.args.get('cabin', 'Y')
        rdate = request.args.get('rdate')  # 返回日期
        isDirect = request.args.get('isdirect')  # 是否直飞
        fn = request.args.get('fn')
        currentPrice = int(float(request.args.get('lowprice')))
        if rdate == None:
            isReturn = False
            isReturn_type = 'OW'
        else:
            isReturn = True
            isReturn_type = 'RT'
        # cabin = request.args.get('cabin') #舱位
        if isDirect == 'false':
            isDirect_type = 'T'
        else:
            isDirect_type = 'D'
        if fn:
            fn_type = 'true'
        else:
            fn_type = 'false'
        currentDate_str = datetime.now().strftime('%Y-%m-%d')
        currentDate_date = datetime.now()
        departDate_date = datetime.strptime(departDate_str, '%Y-%m-%d')
        _id = org + '_' + dst + '_' + isReturn_type + '_' + 'Y' + '_' + isDirect_type + '_' + departDate_str
        return _id, cabin, org, dst, isReturn_type, isDirect_type, departDate_str, departDate_date,\
               currentDate_str, currentDate_date, currentPrice, isDirect, rdate, fn, fn_type


    @lru_cache(maxsize=1024 * 1024 * 1024, typed=False)
    def requestsDict(self, url, cabin, org, dst, departDate_str, currentPrice, isDirect, rdate, fn, fn_type):
        params = {'cmd': 'pricePrediction',
                  'from': 'hbgj',
                  'gzip': '0',
                  'dcity': dst,
                  'acity': org,
                  'date': departDate_str,
                  'st': '301',
                  'isdirect': isDirect,
                  'rdate': rdate,
                  'lowprice': currentPrice,
                  'fn': fn}
        response = requests.get(url, params)
        requestsDict_org = response.json()
        if fn:  #如果fn有值，则展示fn为'true'，否则不展示
            requestsDict = {'trend': requestsDict_org.get('trend'),
                          'adviseTitle':requestsDict_org.get('adviseTitle'),
                          'adviseContent': requestsDict_org.get('adviseContent'),
                          'quxian': requestsDict_org.get('quxian'),
                          'gouwucheTitle': requestsDict_org.get('gouwucheTitle'),
                          'gouwucheContent': requestsDict_org.get('gouwucheTitle'),
                          'currentPrice': requestsDict_org.get('currentPrice'),
                          'fn': fn_type}
        else:
            requestsDict = {'trend': requestsDict_org.get('trend'),
                            'adviseTitle': requestsDict_org.get('adviseTitle'),
                            'adviseContent': requestsDict_org.get('adviseContent'),
                            'quxian': requestsDict_org.get('quxian'),
                            'gouwucheTitle': requestsDict_org.get('gouwucheTitle'),
                            'gouwucheContent': requestsDict_org.get('gouwucheTitle'),
                            'currentPrice': requestsDict_org.get('currentPrice')}
        return requestsDict


    def run(self):
        app = Flask(__name__)
        @app.route('/intl_fline/', methods=['GET','POST'])  #国际航线
        def intl_fline():
            self.timer.start()
            _id, cabin, org, dst, isReturn_type, isDirect_type, departDate_str, departDate_date, \
            currentDate_str, currentDate_date, currentPrice, isDirect, rdate, fn, fn_type = self.getParams()
            mongoDict = self.mongoFind(_id)
            logger.info('requestId: {}'.format(_id))
            returnDict = self.responseResult(_id, cabin, org, dst, isReturn_type, isDirect_type, departDate_str, departDate_date, currentDate_str, currentDate_date, currentPrice, mongoDict)
            costTime = self.timer.cost()
            returnDict['cost'] = costTime
            returnDict['status'] = 200
            return jsonify(returnDict)

        @app.route('/transfor/', methods=['GET', 'POST'])  #国际航班
        def transfor():
            self.timer.start()
            _id, cabin, org, dst, isReturn_type, isDirect_type, departDate_str, departDate_date, \
            currentDate_str, currentDate_date, currentPrice, isDirect, rdate, fn, fn_type = self.getParams()
            logger.info('requestId: {}'.format(_id))
            url = 'http://10.0.1.104:10037/'
            requestsDict = self.requestsDict(url, cabin, org, dst, departDate_str, currentPrice, isDirect, rdate, fn, fn_type)
            logger.info('returnJson: {}'.format(requestsDict))
            costTime = self.timer.cost()
            requestsDict['cost'] = costTime
            requestsDict['status'] = 200
            return jsonify(requestsDict)

        app.run(host='10.0.1.104', port=5000)



if __name__ == '__main__':
    # collectionName1 = 'historyLowPrice_flightLine'
    # collectionName2 = 'intl_pricePredict'
    # 生产内网：172.29.1.172
    # 生产外网：120.133.0.172
    # 测试内网：10.0.1.212
    # 测试外网：123.56.222.127
    clientMongo1 = MongoClient("10.0.1.212:27017", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR')
    clientMongo2 = MongoClient("172.29.1.172:27017", authSource="flight",
                               username='search', password='search@huoli123', authMechanism='MONGODB-CR')
    # cursor1 = clientMongo.flight.intl_pricePredict
    cursor1 = clientMongo1.flight.test_insert
    cursor2 = clientMongo2.flight.historyLowPrice_flightLine
    cursor3 = clientMongo1.flight.priceRange_statistic_intl
    app = App()
    app.run()