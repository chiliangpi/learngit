import logging
from logging import handlers
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from flask import Flask, request, abort, make_response, jsonify
from pymongo import MongoClient


def logger():
    logFileName = '/data/search/predict-2019/script/intl_pricePrediction.log'
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    formatter = logging.Formatter(fmt='[%(asctime)s] - %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    rotating_file = handlers.RotatingFileHandler(filename=logFileName,  mode='a', maxBytes=1024*1024*1024, backupCount=3)
    rotating_file.setLevel(logging.WARNING)
    rotating_file.setFormatter(formatter)
    logger.addHandler(rotating_file)


def historyPrice(org, dst, isReturn_type, isDirect_type, departDate_date, currentDate_date):
    """
    单程返回历史价格，往返不返回历史价格；
    依据_id查找到一条mongodict数据，首先锁定当前日期前一个月的数据，然后取出最近的14条有效数据（最多14条），
    如果有效数据>3条，则返回与当前日期最邻近的价格，其余价格的最大值和最小值；如果有效数据<=3条，则不必比较大小，直接顺次返回数据
    """
    monthDay = datetime.strftime(departDate_date, '%m-%d')
    _id_monthDay = org + '_' + dst + '_' + isReturn_type + '_' + 'Y' + '_' + isDirect_type + '_' + monthDay
    mongoDict = cursor2.find_one({'_id': _id_monthDay}, {'_id': 0})
    if mongoDict:
        history30Days_date = pd.date_range(end=currentDate_date+timedelta(days=-1), periods=30, freq='D').map(lambda x: datetime.strftime(x, '%Y-%m-%d'))
        history14Days_date = []
        count = 0
        for day in history30Days_date:
            if mongoDict.__contains__(day):
                history14Days_date.append(day)
                count += 1
                if count == 14:
                    break
        history14Days_date.sort(reverse=True)
        if count == 0:
            beforeCurrentPrice = ''
            beforeCurrentDate = ''
            H_highPriceDate = ''
            H_highPrice = ''
            H_lowPriceDate = ''
            H_lowPrice = ''
        elif count == 1:
            beforeCurrentDate = history14Days_date[0]
            beforeCurrentPrice = int(mongoDict[beforeCurrentDate])
            H_highPriceDate = ''
            H_highPrice = ''
            H_lowPriceDate = ''
            H_lowPrice = ''
        elif count == 2:
            beforeCurrentDate = history14Days_date[0]
            beforeCurrentPrice = int(mongoDict[beforeCurrentDate])
            H_highPriceDate = history14Days_date[1]
            H_highPrice = int(mongoDict[H_highPriceDate])
            H_lowPriceDate = ''
            H_lowPrice = ''
        elif count == 3:
            beforeCurrentDate = history14Days_date[0]
            beforeCurrentPrice = int(mongoDict[beforeCurrentDate])
            H_highPriceDate = history14Days_date[1]
            H_highPrice = int(mongoDict[H_highPriceDate])
            H_lowPriceDate = history14Days_date[2]
            H_lowPrice = int(mongoDict[H_lowPriceDate])
        else:  #count>=4
            beforeCurrentDate = history14Days_date[0]
            beforeCurrentPrice = int(mongoDict[beforeCurrentDate])
            H_lowPrice = 99999
            H_highPrice = 0
            for day in history14Days_date[1:]:
                if H_lowPrice > int(mongoDict.get(day, 99999)):
                    H_lowPrice = int(mongoDict[day])
                    H_lowPriceDate = day
                if H_highPrice < int(mongoDict.get(day, 0)):
                    H_highPrice = int(mongoDict[day])
                    H_highPriceDate = day
    else:  #找不到数据
        beforeCurrentDate = ''
        beforeCurrentPrice = ''
        H_highPriceDate = ''
        H_highPrice = ''
        H_lowPriceDate = ''
        H_lowPrice  = ''
    return beforeCurrentDate, beforeCurrentPrice, H_highPriceDate, H_highPrice, H_lowPriceDate, H_lowPrice

def future_quxian(departDate_str, depart_intervalDays, currentDate_date, currentPrice, departDate_date, departPrice, mongoDict):
    """
    depart_intervalDays>=1时，才会调用该函数
    """
    if depart_intervalDays == 1:
        #查询日期是起飞的前一天，将起飞日期价格看做最低价，与当前价格进行比较
        lowPrice = departPrice
        lowPriceDate = departDate_str
        trend, adviseTitle, adviseContent = estimate_trend(currentPrice, lowPrice)
        highPrice = ''
        highPriceDate = ''
    elif depart_intervalDays >= 2:
        highPrice, highPriceDate, lowPrice, lowPriceDate = lowAndHigh_price(mongoDict, currentDate_date, departDate_date)
        trend, adviseTitle, adviseContent = estimate_trend(currentPrice, lowPrice)
    return trend, adviseTitle, adviseContent, highPrice,highPriceDate, lowPrice, lowPriceDate


def estimate_trend(currentPrice, lowPrice):
    if (currentPrice-lowPrice)/currentPrice > 0.1:
        trend = 2
        adviseTitle = '预测未来价格可能会下降'
        adviseContent = '通过大数据分析，未来价格下降概率较大。大数据预测也会有误差，仅供参考。'
    elif abs(currentPrice-lowPrice)/currentPrice <= 0.1:
        trend = 0
        adviseTitle = '预测未来价格可能趋于平稳'
        adviseContent = '通过大数据分析，未来价格趋于平稳。现在可以预订，大数据预测也会有误差，仅供参考。'
    else: #(currentPrice-lowPrice)/currentPrice < -0.1
        trend = 1
        adviseTitle = '预测未来价格可能会上涨'
        adviseContent = '通过大数据分析，未来价格上涨概率较大，建议现在预订。大数据预测也会有误差，仅供参考'
    return trend, adviseTitle, adviseContent


def lowAndHigh_price(mongoDict, currentDate_date, departDate_date):
    """
    遍历当前日期至起飞日期（倒序），遇到高价、低价则保留
    如果起飞日期的价格是最高价（或最低价），则最高价（或最低价）覆盖起飞价
    :param mongoDict:
    :return:
    """
    highPrice = 0
    lowPrice = 99999
    ##日期倒序排列，从最大的日期开始遍历
    dateRange = pd.date_range(start=currentDate_date + timedelta(days=1), end=departDate_date,
                              freq='D').map(lambda x: datetime.strftime(x, '%Y-%m-%d'))[::-1]
    for day in dateRange:
        if highPrice < int(mongoDict.get(day, 0)):
            highPrice = int(mongoDict[day])
            highPriceDate = day
        if lowPrice > int(mongoDict.get(day, 99999)):
            lowPrice = int(mongoDict[day])
            lowPriceDate = day
    return highPrice, highPriceDate, lowPrice, lowPriceDate

def priceRange(org, dst, isReturn_type, isDirect_type, departDate_date):
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


def responseResult(_id, org, dst, isReturn_type, departDate_date, depart_intervalDays, isDirect_type, departDate_str, currentDate_date, currentDate_str, currentPrice, cabin):
    """

    """
    gouwucheTitle = "智选购票 尽在购物车"
    gouwucheContent = "管家AI帮您实时监测价格波动，帮您发现低价"
    if cabin == 'Y':
        priceRange_low, priceRange_high = priceRange(org, dst, isReturn_type, isDirect_type, departDate_date)
        beforeCurrentDate, beforeCurrentPrice, H_highPriceDate, H_highPrice, H_lowPriceDate, H_lowPrice = historyPrice(org, dst, isReturn_type, isDirect_type, departDate_date, currentDate_date)
        if depart_intervalDays == 0:
            trend = 1
            adviseTitle = '预测未来价格可能会上涨'
            adviseContent = '通过大数据分析，未来价格上涨概率较大，建议现在预订。大数据预测也会有误差，仅供参考'
            departPrice = currentPrice
            highPrice = ''
            highPriceDate = ''
            lowPrice = ''
            lowPriceDate = ''
        elif depart_intervalDays <= 90:    #当前距起飞日期小于90天的情况
            if isDirect_type == 'D':
                mongoDict = cursor1.find_one({'_id': _id}, {'_id': 0})
                if mongoDict:
                    if mongoDict.__contains__(departDate_str):
                        departPrice = int(mongoDict.get(departDate_str))
                    else:
                        departPrict = ''
                    trend, adviseTitle, adviseContent, highPrice, highPriceDate, lowPrice, lowPriceDate = future_quxian(departDate_str, depart_intervalDays, currentDate_date, currentPrice, departDate_date, departPrice, mongoDict)
                else:#数据库中数据不存在
                    trend = ''
                    adviseTitle = ''
                    adviseContent = ''
                    departPrice = ''
                    departDate_str = ''
                    highPrice = ''
                    highPriceDate = ''
                    lowPrice = ''
                    lowPriceDate = ''

            elif isDirect_type == 'T':
                #如果查询的是中转，需要考虑中转和直飞两种情况，取两个数据集的最低值
                _id_Direct = org + '_' + dst + '_' + isReturn_type + '_' + 'Y' + '_' + 'D' + '_' + departDate_str
                mongoDict1 = cursor1.find_one({'_id': _id}, {'_id': 0})
                mongoDict2 = cursor1.find_one({'_id': _id_Direct}, {'_id': 0})
                if mongoDict1 and mongoDict2: #如果两个_id都存在
                    mongoDict = {}
                    for day in pd.date_range(start=currentDate_date+timedelta(days=1), end=departDate_date, freq='D').map(lambda x: datetime.strftime(x, '%Y-%m-%d')):
                        if mongoDict1.__contains__(day) & mongoDict2.__contains__(day):
                            mongoDict[day] = min(int(mongoDict1.get(day)), int(mongoDict2.get(day)))
                        elif mongoDict1.__contains__(day):
                            mongoDict[day] = int(mongoDict1.get(day))
                        elif mongoDict2.__contains__(day):
                            mongoDict[day] = int(mongoDict2.get(day))
                        else:  ##MongoDB中找到的两条记录，都没有'day'对应的价格
                            continue
                    if mongoDict.__contains__(departDate_str):
                        departPrice = int(mongoDict.get(departDate_str))
                    else:
                        departPrict = ''
                    trend, adviseTitle, adviseContent, highPrice, highPriceDate, lowPrice, lowPriceDate = future_quxian(departDate_str, depart_intervalDays, currentDate_date, currentPrice, departDate_date, departPrice, mongoDict)
                elif mongoDict1:
                    mongoDict = mongoDict1
                    if mongoDict.__contains__(departDate_str):
                        departPrice = int(mongoDict.get(departDate_str))
                    else:
                        departPrict = ''
                    trend, adviseTitle, adviseContent, highPrice, highPriceDate, lowPrice, lowPriceDate = future_quxian(departDate_str, depart_intervalDays, currentDate_date, currentPrice, departDate_date, departPrice, mongoDict)
                elif mongoDict2:
                    mongoDict = mongoDict2
                    if mongoDict.__contains__(departDate_str):
                        departPrice = int(mongoDict.get(departDate_str))
                    else:
                        departPrict = ''
                    trend, adviseTitle, adviseContent, highPrice, highPriceDate, lowPrice, lowPriceDate = future_quxian(departDate_str, depart_intervalDays, currentDate_date, currentPrice, departDate_date, departPrice, mongoDict)
                else:#数据库中数据不存在
                    trend = ''
                    adviseTitle = ''
                    adviseContent = ''
                    departPrice = ''
                    departDate_str = ''
                    highPrice = ''
                    highPriceDate = ''
                    lowPrice = ''
                    lowPriceDate = ''

        else:#depart_intervalDays > 90
            trend = -1
            adviseTitle = ''
            adviseContent = ''
            departPrice = ''
            departDate_str = ''
            highPrice = ''
            highPriceDate = ''
            lowPrice = ''
            lowPriceDate = ''

    else:   #cabin!='Y'
        trend = ''
        adviseTitle = ''
        adviseContent = ''
        departPrice = ''
        departDate_str = ''
        highPrice = ''
        highPriceDate = ''
        lowPrice = ''
        lowPriceDate = ''
        beforeCurrentDate = ''
        beforeCurrentPrice = ''
        H_highPriceDate = ''
        H_highPrice = ''
        H_lowPriceDate = ''
        H_lowPrice = ''
        priceRange_low = ''
        priceRange_high = ''
    return jsonify({'trend': trend,
                    'adviseTitle':  adviseTitle,
                    'adviseContent':  adviseContent,
                    'quxian': {
                                H_highPriceDate: H_highPrice,
                                H_lowPriceDate: H_lowPrice,
                                beforeCurrentDate: beforeCurrentPrice,
                                currentDate_str: currentPrice,
                                highPriceDate: highPrice,
                                lowPriceDate: lowPrice,
                                departDate_str: departPrice
                               },
                    'gouwucheTitle': gouwucheTitle,
                    'gouwucheContent': gouwucheTitle,
                    'priceRange_low': priceRange_low,
                    'priceRange_high': priceRange_high
                    })



def run():
    app = Flask(__name__)
    @app.route('/', methods=['GET','POST'])
    def do_Get():

        org = request.args.get('acity') #出发地
        dst = request.args.get('dcity') #到达地
        departDate_str = request.args.get('date') #出发日期
        rdate = request.args.get('rdate') #返回日期
        currentPrice = int(float(request.args.get('lowprice')))
        if rdate == None:
            isReturn = False
            isReturn_type = 'OW'
        else:
            isReturn = True
            isReturn_type = 'RT'
        # cabin = request.args.get('cabin') #舱位
        cabin = 'Y'
        isDirect = request.args.get('isdirect')#是否直飞
        if isDirect == 'false':
            isDirect_type = 'T'
        else:
            isDirect_type = 'D'
        currentDate_str = datetime.now().strftime('%Y-%m-%d')
        currentDate_date = datetime.now()
        departDate_date = datetime.strptime(departDate_str, '%Y-%m-%d')
        _id = org + '_' + dst + '_' + isReturn_type + '_' + 'Y' + '_' + isDirect_type + '_' + departDate_str

        depart_intervalDays = (departDate_date - currentDate_date).days
        responseJsonify = responseResult(_id, org, dst, isReturn_type, departDate_date, depart_intervalDays, isDirect_type, departDate_str, currentDate_date, currentDate_str, currentPrice, cabin)
        return responseJsonify
    app.run(host='10.0.1.104', port=5000)


if __name__ == '__main__':
    logger()
    # collectionName1 = 'historyLowPrice_flightLine'
    # collectionName2 = 'intl_pricePredict'
    # 内网：172.29.1.172
    # 外网：120.133.0.172
    # 测试内网：10.0.1.212
    # 测试外网：123.56.222.127
    clientMongo1 = MongoClient("10.0.1.212:27017", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR')
    clientMongo2 = MongoClient("172.29.1.172:27017", authSource="flight",
                               username='search', password='search@huoli123', authMechanism='MONGODB-CR')
    # cursor1 = clientMongo.flight.intl_pricePredict
    cursor1 = clientMongo1.flight.test_insert2
    cursor2 = clientMongo2.flight.historyLowPrice_flightLine
    cursor3 = clientMongo1.flight.priceRange_statistic_intl
    run()









