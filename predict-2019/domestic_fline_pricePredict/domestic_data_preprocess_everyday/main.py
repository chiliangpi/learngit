# coding:utf-8
# -*- coding:utf-8 -*-

import domestic_org_generate_data_everyday
import config
from multiprocessing import Pool, Process


params = config.params


#利用多进程获取数据，节省一些时间
p = Pool(4)
p.apply_async(domestic_org_generate_data_everyday.lowPrice_train_data_add, args=(params,))
p.apply_async(domestic_org_generate_data_everyday.seatleft_data_add, args=(params,))
p.apply_async(domestic_org_generate_data_everyday.infoBase_data, args=(params,))
p.apply_async(domestic_org_generate_data_everyday.lowPrice_online_data, args=(params,))
p.apply_async(domestic_org_generate_data_everyday.orgPrice_data, args=(params,))
p.apply_async(domestic_org_generate_data_everyday.globalAirport_data, args=(params,))
p.close()
p.join()

# #调试情况下，顺序获取数据
# domestic_org_generate_data_everyday.lowPrice_train_data_add(params)
# domestic_org_generate_data_everyday.seatleft_data_add(params)
# domestic_org_generate_data_everyday.infoBase_data(params)
# domestic_org_generate_data_everyday.lowPrice_online_data(params)
# domestic_org_generate_data_everyday.orgPrice_data(params)
# domestic_org_generate_data_everyday.globalAirport_data(params)
