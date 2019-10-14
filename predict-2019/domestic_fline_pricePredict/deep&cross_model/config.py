# coding:utf-8
# -*- coding:utf-8 -*-
# coding:utf-8
# -*- coding:utf-8 -*-
columnNames = ['fn', 'fc', 'org', 'dst', 'departDate', 'queryDate', 'price',
       'orgPrice', 'futureMinPrice', 'distance', 'seatLeft', 'departTime',
       'isShare', 'trend', 'discountOff', 'depart_calday_ln',
       'depart_festival', 'depart_isVacation', 'depart_vacationDays',
       'depart_dayofvacation', 'depart_isFestival',
       'depart_dayofvacation_extend', 'departYear', 'departMonth',
       'depart_dayofmonth', 'depart_weekofyear', 'depart_dayofweek',
       'depart_dayofweek_bucket', 'departQuarter', 'intervalDays',
       'intervalMonths', 'intervalWeeks', 'intervalQuarters',
       'query_calday_ln', 'query_festival', 'query_isVacation',
       'query_vacationDays', 'query_dayofvacation', 'query_isFestival',
       'query_dayofvacation_extend', 'queryYear', 'queryMonth',
       'query_dayofmonth', 'query_weekofyear', 'query_dayofweek',
       'query_dayofweek_bucket', 'queryQuarter', 'stringIndexer_fn',
       'stringIndexer_fc', 'stringIndexer_org', 'stringIndexer_dst',
       'stringIndexer_depart_festival', 'stringIndexer_query_festival',
       'discountOffBucket', 'distanceBucket', 'seatLeftBucket']

dropFeatures = ['id', 'orgPrice', 'org', 'query_calday_ln', 'fn', 'query_festival', 'dst', 'depart_calday_ln', 'futureMinPrice', 'fc', 'queryDate',
                'price', 'depart_festival', 'departDate', 'distance', 'seatLeft', 'discountOff']

baseColumns = ['id', 'queryDate']

label = 'trend'

numericCols = []

categoryCols = list(set(columnNames)-set(numericCols))

epoches = 1
threshold = 0.5

init_learning_rate = 0.001
decay_rate = 0.98
embedding_size = 64
deep_layers = [256, 128, 64]
cross_layers_num = 3
dropout_deep = [0.8, 0.8, 0.8, 0.8]
batch_size = 512


sparkHost = 'hdfs://10.0.4.217:8020'
hdfsHost = 'http://10.0.4.217:9870'
sparkDirName_totalData = '/crawler/domestic_shuffled_featureData_20190919.parquet/'
sparkDirName_trainData = '/crawler/domestic_DNN_trainData_20190919.parquet/'
sparkDirName_trainSampleData = '/crawler/domestic_trainSampleData_20190919.parquet/'
sparkDirName_valSampleData = '/crawler/domestic_DNN_valData_queryDate=2019-07-25_20190919.parquet/'
featureDict_fileName = '/data/search/predict-2019/DNN_predict/script_DCN/DCN_featureDict.json'
logFileName = '/data/search/predict-2019/DNN_predict/script_DCN/log_predict.txt'
DNN_pred_dirName = '/data/search/predict-2019/DNN_predict/script_DCN/DCN_pred_result/'
DNN_model_fileName = '/data/search/predict-2019/DNN_predict/script_DCN/DNN_model/model'
DNN_model_choice_num = '1-200'
sparkDirName_reTrainData = '/crawler/domestic_DNN_valData_departDate=2019-07-25_20190919.parquet'
sparkDirName_onlineData = '/crawler/domestic_DNN_valData_queryDate=2019-07-26_20190919.parquet/'
queryDate = '20190726'



##########
#本地配置
##########
# sparkHost = ''
# hdfsHost = ''
# sparkDirName_totalData = '/Users/changfu/Desktop/DNN_predict/domestic_trainSampleData_20190910.parquet'
# sparkDirName_trainData = '/Users/changfu/Desktop/DNN_predict/domestic_trainSampleData_20190910.parquet'
# sparkDirName_valData = '/Users/changfu/Desktop/DNN_predict/domestic_trainSampleData_20190910.parquet'
# sparkDirName_trainSampleData = '/Users/changfu/Desktop/DNN_predict/domestic_trainSampleData_20190910.parquet'
# sparkDirName_valSampleData = '/Users/changfu/Desktop/DNN_predict/domestic_trainSampleData_20190910.parquet'
# featureDict_fileName = '/Users/changfu/Desktop/DNN_predict/DCN_featureDict.json'
# logFileName = '/Users/changfu/Desktop/DNN_predict/log_predict.txt'
# DNN_model_fileName = '/Users/changfu/Desktop/DNN_predict/DNN_model/model'
# #
# init_learning_rate = 0.001
# decay_rate = 0.98
# embedding_size = 8
# deep_layers = [32, 16, 8]
# cross_layers_num = 3
# dropout_deep = [0.5, 0.5, 0.5, 0.5]
# batch_size = 128


