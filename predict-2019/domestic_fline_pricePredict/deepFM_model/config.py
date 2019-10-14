# coding:utf-8
# -*- coding:utf-8 -*-
from datetime import datetime, timedelta

#dfm_params
init_learning_rate = 0.001
decay_rate = 0.98
embedding_size = 256
dropout_fm = [0.8, 0.8]
deep_layers = [256, 128, 64]
dropout_deep = [0.8, 0.8, 0.8, 0.8]
batch_size = 512
l2_reg = 0.0


epoches = 1
threshold = 0.6
#遍历mongoDB中的historyLowPrice_fn_domestic表中数据，得到的原始数据列名
org_columnNames = ['fn', 'org', 'dst', 'departDate', 'queryDate', 'fc', 'price', 'city_code', 'futureMinPrice',
                   'trend', 'org_latitude', 'org_longitude', 'dst_latitude', 'dst_longitude', 'distance', 'orgPrice',
                   'discount', 'seatLeft', 'departtime', 'arrivetime', 'isShare']
#spark将原始数据预处理后，即喂入模型的数据列名
columnNames = ['fn', 'departDate', 'queryDate', 'city_code', 'trend', 'departTime', 'arriveTime', 'isShare',
               'depart_isVacation', 'depart_vacationDays', 'depart_dayofvacation', 'depart_isFestival',
               'depart_dayofvacation_extend', 'departYear', 'departMonth', 'depart_dayofmonth', 'depart_weekofyear',
               'depart_dayofweek', 'depart_dayofweek_bucket', 'departQuarter', 'intervalDays', 'intervalMonths',
               'intervalWeeks', 'intervalQuarters', 'query_isVacation', 'query_vacationDays', 'query_dayofvacation',
               'query_isFestival', 'query_dayofvacation_extend', 'queryYear', 'queryMonth', 'query_dayofmonth',
               'query_weekofyear', 'query_dayofweek', 'query_dayofweek_bucket', 'queryQuarter', 'stringIndexer_fn',
               'stringIndexer_fc', 'stringIndexer_org', 'stringIndexer_dst', 'stringIndexer_depart_festival',
               'stringIndexer_query_festival', 'discountOffBucket', 'distanceBucket', 'seatLeftBucket']
#经preprocess处理后，需要去掉的字符串属性
dropFeatures = ['city_code', 'org_latitude', 'org_longitude', 'dst_latitude', 'dst_longitude', 'queryDate', 'orgPrice',
                'org', 'query_calday_ln', 'fn', 'query_festival', 'dst', 'depart_calday_ln', 'futureMinPrice', 'fc',
                'price', 'depart_festival', 'departDate', 'distance', 'seatLeft', 'discount', 'discountOff']
#baseColumn的列信息，作为样本的识别信息
baseColumns = ['fn', 'city_code', 'departDate', 'queryDate']
label = 'trend'
numericCols = []
categoryCols = list(set(columnNames)-set(numericCols))
#通过generateDate控制生成哪一天起飞的数据；正常情况下，generateDate为当前日期（today）
generateDate = datetime.today()
# generateDate = datetime.strptime('2019-07-26', "%Y-%m-%d")
generateDate_str1 = datetime.strftime(generateDate, "%Y%m%d")
generateDate_str2 = datetime.strftime(generateDate, "%Y-%m-%d")
yesterday_str1 = datetime.strftime(generateDate-timedelta(days=1), "%Y%m%d")
yesterday_str2 = datetime.strftime(generateDate-timedelta(days=1), "%Y-%m-%d")
sparkHost = 'hdfs://10.0.4.217:8020'
hdfsHost = 'http://10.0.4.217:9870'
sparkDirName_org_trainData_yesterday = "/crawler/domestic_org_train_union_data_{}.csv".format(yesterday_str1)
sparkDirName_org_trainData_add = "/crawler/domestic_org_train_add_data_{}.csv".format(generateDate_str1)
sparkDirName_org_trainData_union = "/crawler/domestic_org_train_union_data_{}.csv".format(generateDate_str1)
sparkDirName_trainData = "/crawler/domestic_DNN_trainData_{}.parquet/".format(generateDate_str1)
sparkDirName_trainData_test = "/crawler/domestic_DNN_trainData_test.parquet/"
sparkDirName_trainSampleData_test = "/crawler/domestic_DNN_trainSampleData_test.parquet/"
sparkDirName_valData_test = "/crawler/domestic_DNN_valData_test.parquet/"
# sparkDirName_trainSampleData = "/crawler/domestic_DNN_trainSampleData_{}.parquet/".format(generateDate_str1)
# sparkDirName_valSampleData = "/crawler/domestic_DNN_valSampleData_{}.parquet/".format(generateDate_str1)
localFileName_org_trainData_add = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_train_add_data_{}.csv".format(generateDate_str1)
localFileName_org_valSampleData = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_valSample_data_{}.csv".format(generateDate_str1)
featureDict_fileName = '/data/search/predict-2019/DNN_predict/script_deepFM/deepFM_feature_dict_{}.json'.format(generateDate_str1)
sparkDirName_org_lowPrice_data = "/crawler/domestic_org_lowPrice_data_{}.csv".format(generateDate_str1)
sparkDirName_org_orgPrice_data = "/crawler/domestic_org_orgPrice_data_{}.csv".format(generateDate_str1)
sparkDirName_org_Airport_data = "/crawler/domestic_org_Airport_data_{}.csv".format(generateDate_str1)
sparkDirName_org_seatLeft_data = "/crawler/domestic_org_seatLeft_data_{}.csv".format(generateDate_str1)
sparkDirName_org_infoBase_data = "/crawler/domestic_org_infoBase_data_{}.csv".format(generateDate_str1)
localFileName_org_lowPrice_data = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_lowPrice_data_{}.csv".format(generateDate_str1)
localFileName_org_orgPrice_data = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_orgPrice_data_{}.csv".format(generateDate_str1)
localFileName_org_Airport_data = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_Airport_data_{}.csv".format(generateDate_str1)
localFileName_org_seatLeft_data = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_seatLeft_data_{}.csv".format(generateDate_str1)
localFileName_org_infoBase_data = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_infoBase_data_{}.csv".format(generateDate_str1)
#lowPrice_data和seatLeft_data每天部分增加，Airport_data、orgPrice_data和infoBase_data每天执行同样的代码
sparkDirName_org_lowPrice_data_add = "/crawler/domestic_org_lowPrice_add_data_{}.csv".format(generateDate_str1)
sparkDirName_org_seatLeft_data_add = "/crawler/domestic_org_seatLeft_add_data_{}.csv".format(generateDate_str1)
localFileName_org_lowPrice_data_add = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_lowPrice_add_data_{}.csv".format(generateDate_str1)
localFileName_org_seatLeft_data_add = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_seatLeft_add_data_{}.csv".format(generateDate_str1)
#org_lowPrice_onlineData是从lowPrice表中摘取的信息，后续还需要将seatLeft，orgPrice等信息拼接上，才能构成org_onlineData数据，最后通过spark生成模型能够使用的数据
sparkDirName_org_lowPrice_onlineData = "/crawler/domestic_org_lowPrice_online_data_{}.csv".format(generateDate_str1)
localFileName_org_lowPrice_onlineData = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_lowPrice_online_data_{}.csv".format(generateDate_str1)
sparkDirName_org_onlineData = "/crawler/domestic_org_online_data_{}.csv".format(generateDate_str1)
localFileName_org_onlineData = "/data/search/predict-2019/domestic_data_preprocess/domestic_org_online_data_{}.csv".format(generateDate_str1)
sparkDirName_onlineData = "/crawler/domestic_DNN_onlineData_{}.parquet/".format(generateDate_str1)
localFileName_lowPrice_idList = '/data/search/predict-2019/domestic_data_preprocess/domestic_lowPrice_idList_{}'.format(generateDate_str1)

#模型部分增加的文件信息
localFileName_deepFM_result = '/data/search/predict-2019/DNN_predict/script_deepFM/deepFM_results/deepFM_result_{}.csv'.format(generateDate_str1)
localDirName_deepFM_model = '/data/search/predict-2019/DNN_predict/script_deepFM/deepFM_models/deepFM_model'
DNN_model_choice_num = '1-200'
mongoTableName_deepFM = 'domestic_price_trend_result'
#日志信息地址
# logFileName = '/data/search/predict-2019/DNN_predict/script_deepFM/script_first_run/log_info.txt'
logFileName = '/data/search/predict-2019/DNN_predict/script_deepFM/script_everyday_run/log_info.txt'


params = {
        "epoches": epoches,
        "threshold": threshold,
        "org_columnNames": org_columnNames,
        "columnNames": columnNames,
        "dropFeatures": dropFeatures,
        "baseColumns": baseColumns,
        "numericCols": numericCols,
        "categoryCols": categoryCols,
        "label": label,
        "sparkHost": sparkHost,
        "hdfsHost": hdfsHost,
        "generateDate": generateDate,
        "generateDate_str1": generateDate_str1,
        "generateDate_str2": generateDate_str2,
        "yesterday_str1": yesterday_str1,
        "yesterday_str2": yesterday_str2,
        "sparkDirName_org_trainData_yesterday": sparkDirName_org_trainData_yesterday,
        "sparkDirName_org_trainData_add": sparkDirName_org_trainData_add,
        "sparkDirName_org_trainData_union": sparkDirName_org_trainData_union,
        "sparkDirName_org_onlineData": sparkDirName_org_onlineData,
        "sparkDirName_trainData": sparkDirName_trainData,
        "sparkDirName_trainData_test": sparkDirName_trainData_test,
        "sparkDirName_trainSampleData_test": sparkDirName_trainSampleData_test,
        "sparkDirName_valData_test": sparkDirName_valData_test,
        "sparkDirName_onlineData": sparkDirName_onlineData,
        "localFileName_org_trainData_add": localFileName_org_trainData_add,
        "localFileName_org_onlineData": localFileName_org_onlineData,
        "localFileName_org_valSampleData": localFileName_org_valSampleData,
        "featureDict_fileName": featureDict_fileName,
        "logFileName": logFileName,
        "sparkDirName_org_lowPrice_data": sparkDirName_org_lowPrice_data,
        "sparkDirName_org_lowPrice_onlineData": sparkDirName_org_lowPrice_onlineData,
        "sparkDirName_org_orgPrice_data": sparkDirName_org_orgPrice_data,
        "sparkDirName_org_Airport_data": sparkDirName_org_Airport_data,
        "sparkDirName_org_seatLeft_data": sparkDirName_org_seatLeft_data,
        "sparkDirName_org_infoBase_data": sparkDirName_org_infoBase_data,
        "localFileName_org_lowPrice_data": localFileName_org_lowPrice_data,
        "localFileName_org_lowPrice_onlineData": localFileName_org_lowPrice_onlineData,
        "localFileName_org_orgPrice_data": localFileName_org_orgPrice_data,
        "localFileName_org_Airport_data": localFileName_org_Airport_data,
        "localFileName_org_seatLeft_data": localFileName_org_seatLeft_data,
        "localFileName_org_infoBase_data": localFileName_org_infoBase_data,
        "localFileName_lowPrice_idList": localFileName_lowPrice_idList,
        "sparkDirName_org_lowPrice_data_add": sparkDirName_org_lowPrice_data_add,
        "sparkDirName_org_seatLeft_data_add": sparkDirName_org_seatLeft_data_add,
        "localFileName_org_lowPrice_data_add": localFileName_org_lowPrice_data_add,
        "localFileName_org_seatLeft_data_add": localFileName_org_seatLeft_data_add,
        "localFileName_deepFM_result": localFileName_deepFM_result,
        "localDirName_deepFM_model": localDirName_deepFM_model,
        "DNN_model_choice_num": DNN_model_choice_num,
        "mongoTableName_deepFM": mongoTableName_deepFM
    }







