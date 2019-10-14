# coding:utf-8
# -*- coding:utf-8 -*-

import config
import org_ticketHistory_data_generate

params = config.params

org_ticketHistory_data_generate.org_ticketHistory_data(params)
org_ticketHistory_data_generate.org_trainStation_data(params)
org_ticketHistory_data_generate.org_TrainPass_data(params)


