# coding:utf-8
# -*- coding:utf-8 -*-

from datetime import datetime, timedelta

generateDate = datetime.today()
# generateDate = datetime.strptime('2019-07-26', "%Y-%m-%d")
generateDate_str1 = datetime.strftime(generateDate, "%Y%m%d")
generateDate_str2 = datetime.strftime(generateDate, "%Y-%m-%d")
yesterday_str1 = datetime.strftime(generateDate-timedelta(days=1), "%Y%m%d")
yesterday_str2 = datetime.strftime(generateDate-timedelta(days=1), "%Y-%m-%d")
localFileName_org_ticketHistory_data = "/data/search/predict-2019/trainTicket_predict/data_preprocess/org_ticketHistory_{}.csv".format(generateDate_str2)
sparkDirName_org_ticketHistory_data = "/crawler/org_ticketHistory_{}.csv".format(generateDate_str2)

localFileName_org_trainStation_data = "/data/search/predict-2019/trainTicket_predict/data_preprocess/org_trainStation_{}.csv".format(generateDate_str2)
sparkDirName_org_trainStation_data = "/crawler/org_trainStation_{}.csv".format(generateDate_str2)

localFileName_org_TrainPass_data = "/data/search/predict-2019/trainTicket_predict/data_preprocess/org_TrainPass_{}.csv".format(generateDate_str2)
sparkDirName_org_TrainPass_data = "/crawler/org_TrainPass_{}.csv".format(generateDate_str2)



#日志保存地址
logFileName = '/data/search/predict-2019/trainTicket_predict/data_preprocess/script/log_info.txt'


params = {
       "generateDate": generateDate,
       "generateDate_str1": generateDate_str1,
       "generateDate_str2": generateDate_str2,
       "yesterday_str1": yesterday_str1,
       "yesterday_str2": yesterday_str2,
       "localFileName_org_ticketHistory_data": localFileName_org_ticketHistory_data,
       "sparkDirName_org_ticketHistory_data": sparkDirName_org_ticketHistory_data,
       "localFileName_org_trainStation_data": localFileName_org_trainStation_data,
       "sparkDirName_org_trainStation_data": sparkDirName_org_trainStation_data,
       "localFileName_org_TrainPass_data": localFileName_org_TrainPass_data,
       "sparkDirName_org_TrainPass_data": sparkDirName_org_TrainPass_data,
       "logFileName": logFileName
        }