# coding:utf-8
# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime
from flask import Flask, request, abort, make_response


def quxianPoint(df):
    lowPrice = df.predictPrice.min()
    lowPrice_date = df[df.predictPrice == lowPrice][predictDate].max()
    highPrice = df.predictPrice.max()
    highPrice_date = df[df.predictPrice == highPrice][predictDate].max()
    departPrice = df[df.predictDate == departDate][predictPrice].max()
    return lowPrice, lowPrice_date, highPrice, highPrice_date, departPrice

def estimate_trend(currentPrice, lowPrice, lowPrice_date):  #价格趋势trend判断逻辑
    turn_intervalDays = ((datetime.strptime(departDate, '%Y-%m-%d') - datetime.strptime(lowPrice_date, '%Y-%m-%d'))).days
    if (currentPrice - lowPrice) / currentPrece >= 0.1:  #最低价格低于当前价格10%以上
        advice = "建议观望一段时间"
        reason = "出行时间在15天和90天之间，预测价格低于当前价格10%以上"
        trend = 2
        if turn_intervalDays < 7:  #当前日期距最低价日期小于一周
            message="价格在未来%s天是波动的,为您节省%s元；这段期间最低价格是%s元" %(turn_intervalDays, (currentPrice-lowPrice), lowPrice)
        else:  #当前日期距最低价日期大于一周
            message="价格在未来%s周内是波动的,为您节省%s元；这段期间最低价格是%s元" %((turn_intervalDays//7), (currentPrice-lowPrice), lowPrice)
    elif (currentPrice-lowPrice)/currentPrece > -0.1:  #最低价格与当前价格相差不大
        advice = "建议现在购买"
        reason = "出行时间在15天和90天之间，预测低价与现在价格相差不大"
        message = "预测价格与现在相差不大；
        trend = 0
    else:  #最低价格高于当前价格10%以上
        advice = "建议尽快购买"
        reason = "出行时间在15天和90天之间，预测价格高于当前价格10%以上"
        trend = 1
        if turn_intervalDays < 7:  #当前日期距最低价日期小于一周
            message="价格在未来%s天是波动的，最少比现在多花费%s元;这段期间最低价格是%s元" %(turn_intervalDays, (lowPrice-currentPrice), lowPrice)
        else:  #当前日期距最低价日期大于一周
            message="价格在未来%s周内是波动的，最少比现在多花费%s元;这段期间最低价格是%s元" %((turn_intervalDays//7), (lowPrice-currentPrice), lowPrice)
    return advice, reason, trend, message

def isDirect_False(isReturn_type):  #详情页没有选择直飞，就将直飞和中转两种情况都考虑在内
    id_1 = org + '_' + dst + '_' + isReturn_type + 'Y' + 'T' + '_' + departDate
    id_2 = org + '_' + dst + '_' + isReturn_type + 'Y' + 'D' + '_' + departDate
    if dict_txt.__contains__(id_1) and dict_txt.__contains__(id_2):  #当前查询条件下，数据库中直飞和中转的数据都存在
        df1 = pd.DataFrame(dict_txt.get(id_1))
        df1['id_'] = id_1
        df2 = pd.DataFrame(dict_txt.get(id_2))
        df2['id_'] = id_2
        df = pd.concat([df1, df2], axis=0)
        lowPrice, lowPrice_date, highPrice, highPrice_date, departPrice = quxianPoint(df)
        quxian = {currentDate: currentPrice, lowPrice_date: lowPrice, highPrice_date: highPrice, departDate: departPrice}
        dayKey = df[(df.predictPrice == lowPrice) & (df.predictDate == lowPrice_date)]['id_'].values[0]  #取出最低价格对应的航线编号
        advice, reason, trend, message = estimate_trend(currentPrice, lowPrice, lowPrice_date)
    elif dict_txt.__contains__(id_1) and not dict_txt.__contains__(id_2):  #当前查询条件下，数据库中只有中转的数据存在
        df = pd.DataFrame(dict_txt.get(id_1))
        lowPrice, lowPrice_date, highPrice, highPrice_date, departPrice = quxianPoint(df)
        quxian = {currentDate: currentPrice, lowPrice_date: lowPrice, highPrice_date: highPrice, departDate: departPrice}
        dayKey = id_1
        advice, reason, trend, message = estimate_trend(currentPrice, lowPrice, lowPrice_date)
    elif not dict_txt.__contains__(id_1) and dict_txt.__contains__(id_2):  #当前查询条件下，数据库中只有直飞的数据存在
        df = pd.DataFrame(dict_txt.get(id_2))
        lowPrice, lowPrice_date, highPrice, highPrice_date, departPrice = quxianPoint(df)
        quxian = {currentDate: currentPrice, lowPrice_date: lowPrice, highPrice_date: highPrice, departDate: departPrice}
        dayKey = id_2
        advice, reason, trend, message = estimate_trend(currentPrice, lowPrice, lowPrice_date)
    else:  #当前查询条件下，数据库中找不到相关数据
        advice = '建议持续关注'
        reason = '缺少历史数据，建议持续关注'
        message = "缺少历史数据，建议持续关注"
        trend = -1
        quxian = ""
        dayKey = ""
    return advice, \
           reason, \
           trend, \
           message,\
           dayKey,\
           quxian

def isDirect_True(isReturn_type):
    id_ = org + '_' + dst + '_' + isReturn_type + 'Y' + 'D' + '_' + departDate
    if dict_txt.__contains__(id_):
        df = pd.DataFrame(dict_txt.get(id_))
        lowPrice, lowPrice_date, highPrice, highPrice_date, departPrice = quxianPoint(df)
        quxian = {currentDate: currentPrice, lowPrice_date: lowPrice, highPrice_date: highPrice, departDate: departPrice}
        dayKey = id_
        advice, reason, trend, message = estimate_trend(currentPrice, lowPrice, lowPrice_date)
    else:
        advice = '建议持续关注'
        reason = '缺少历史数据，建议持续关注'
        message = "缺少历史数据，建议持续关注"
        trend = -1
        quxian = ""
        dayKey = ""
    return advice, \
           reason, \
           trend, \
           message,\
           dayKey,\
           quxian


def responseResult(org,dst,departDate,rdate,cabin,isDirect,currentPrice,currentDate,depart_intervalDays):
    gouwucheTitle = "智选购票 尽在购物车"
    gouwucheContent = "管家AI帮您实时监测价格波动，帮您发现低价"
    if cabin == 'Y':
        if depart_intervalDays < 15:    #当前距起飞日期小于15天的情况
            advise = "建议尽快购买"
            message = "当前时间距离起飞日期较近，建议尽快购买"
            reason = "出发日期15天内的建议购买"
            trend = 1
            quxian = ""
            dayKey = ""
        elif depart_intervalDays <= 90:  #当前距起飞日期大于15且小于90天的情况
            if isReturn == True:  #选择往返的情况
                isReturn_type = 'RT'
                if isDirect = False:  #往返情况下，直飞和中转都考虑
                    advice, reason, trend, message, dayKey, quxian = isDirect_False(isReturn_type)
                else:  #往返情况下，只考虑直飞
                    advice, reason, trend, message, dayKey, quxian = isDirect_True(isReturn_type)
            else:  #选择单程的情况
                isReturn_type = 'OW'
                if isDirect = False:  #单程情况下，直飞和中转都考虑
                    advice, reason, trend, message, dayKey, quxian = isDirect_False(isReturn_type)
                else:  #单程情况下，只考虑直飞
                    advice, reason, trend, message, dayKey, quxian = isDirect_True(isReturn_type)
        else:  #当前日期距起飞大于90天的情况
            advise = "建议观望一段时间"
            message = "当前时间距离起飞日期较长，建议观望"
            reason = "出发日期在90天以后的，建议观望"
            trend = 0
            quxian = ""
            dayKey = ""
    else:  #没有选择经济舱，则不返回内容
        advise = ""
        message = ""
        reason = "没有选择经济舱，不返回内容"
        trend = ""
        quxian = ""
        dayKey = ""
    #最终返回jsonify
    return jsonify({"date": DepartDate,
                    "dayKey": dayKey,
                    "intervalKey": "",
                    "riseData": {"message": message, "startDate": currentDate, "endDate": departDate},
                    "currentPrice": "待定",
                    "advise": advise,
                    "gouwucheTitle": gouwucheTitle,
                    "gouwucheContent": gouwucheContent,
                    "reason": reason,
                    "avgPrice": '待定',
                    "adviseTitle": "",
                    "adviseContent": "",
                    "trend": trend,
                    "quxian": quxian
                    })

def run():
    app = Flask(__name__)
    @app.route('/', methods=['GET','POST'])
    def do_Get():
        org = request.args.get('org') #出发地
        dst = request.args.get('dst') #到达地
        departDate = request.args.get('date') #出发日期
        rdate = request.args.get('rdate') #返回日期
        if rdate == None:
            isReturn = False
            isReturn_type = 'OW'
        else:
            isReturn = True
            isReturn_type = 'RT'
        cabin = request.args.get('cabin') #舱位
        isDirect = request.args.get('isDirect')#是否直飞
        if isDirect == False:
            isDirect_type = 'T'
        else:
            isDirect_type = 'D'
        currentPrice
        currentDate = datetime.strftime(datetime.today(), '%Y-%m-%d')
        depart_intervalDays = (datetime.strptime(departDate, '%Y-%m-%d') - datetime.today()).days
        responseJsonify = responseResult(org,dst,departDate,rdate,cabin,isDirect,currentPrice,currentDate,depart_intervalDays)
        return responseJsonify

if __name__ == "__main__":
    with open("/data/search/predict-2019/dict_txt.txt") as f:
        dict_txt = eval(f.read())
    run()