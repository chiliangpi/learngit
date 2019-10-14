# coding:utf-8
# -*- coding:utf-8 -*-
import utils
from logConfig import logger
from pymongo import MongoClient
import pandas as pd


clientMongo = MongoClient("172.29.1.172", authSource="flight",
                              username='search', password='search@huoli123', authMechanism='MONGODB-CR', connect=False)
cursor_ticket = clientMongo.flight.trainTicketHistory
cursor_station = clientMongo.domesticStation
cursor_pass = clientMongo.gtgjTrainPassData

def org_ticketHistory_data(params):
    with open(params["localFileName_org_ticketHistory_data"], 'w') as f:
        columns = ['queryDatetime', 'hasTicket', 'id']
        f.write(','.join(columns))
        f.write('\n')
        f.seek(0, 2)
        for sample in cursor_ticket.find({}):
            id = sample.get('_id')
            del sample["_id"]
            try:
                del sample["ut"]
                del sample["noticket"]
                del sample["hasticket"]
            except:
                pass
            df = pd.DataFrame.from_dict(sample, orient='index').reset_index().rename(columns={'index': 'queryDatetime', 0: 'hasTicket'})
            df['id'] = id
            df.to_csv(params["localFileName_org_ticketHistory_data"], mode='a', header=False, index=False)
    logger.info("====\"{}\" finished====".format(params["localFileName_org_ticketHistory_data"].split('/')[-1]))
    utils.delete_before2_localData(params["localFileName_org_ticketHistory_data"], params)
    utils.upload_to_hdfs(params["localFileName_org_ticketHistory_data"], params["sparkDirName_org_ticketHistory_data"], params)

def org_trainStation_data(params):
    with open(params["localFileName_org_trainStation_data"], 'w') as f:
        columns = ['_id', 'jianpin', 'code', 'quanpin', 'name', 'citycn', 'latitude', 'longitude', 'citycode', 'realdistancesmap', 'site', 'distancesmap']
        f.write(','.join(columns))
        f.write('\n')
        f.seek(0, 2)
        for sample in cursor_station.find({}):
            content = ','.join(map(str, sample.values()))
            f.write(content + '\n')
        logger.info("====\"{}\" finished====".format(params["localFileName_org_trainStation_data"].split('/')[-1]))
        utils.delete_before2_localData(params["localFileName_org_trainStation_data"], params)
        utils.upload_to_hdfs(params["localFileName_org_trainStation_data"], params["sparkDirName_org_trainStation_data"],
                             params)

def org_TrainPass_data(params):
    with open(params["localFileName_org_TrainPass_data"], 'w') as f:
        columns = ['tn', 'arriveTime', 'stationCode', 'departTime', 'stationName', 'orderNum']
        f.write(','.join(columns))
        f.write('\n')
        f.seek(0, 2)
        for sample in cursor_pass.find({}):
            tn = sample.get("_id")
            List = sample.get("array")
            counter = 0
            for sub in List:
                counter += 1
                content = ','.join([tn] + list(map(str, sub.values())) + [str(counter)])
                f.write(content + '\n')
    logger.info("====\"{}\" finished====".format(params["localFileName_org_TrainPass_data"].split('/')[-1]))
    utils.delete_before2_localData(params["localFileName_org_TrainPass_data"], params)
    utils.upload_to_hdfs(params["localFileName_org_TrainPass_data"], params["localFileName_org_TrainPass_data"], params)










            


