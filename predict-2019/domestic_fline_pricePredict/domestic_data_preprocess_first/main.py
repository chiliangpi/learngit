# coding:utf-8
# -*- coding:utf-8 -*-

import domestic_org_generate_data_everyday
import config
from multiprocessing import Pool, Process
import domestic_org_data_first_generate

params = config.params

#利用多进程获取数据，节省一些时间
p = Pool(3)
p.apply_async(domestic_org_data_first_generate.infoBase_data, args=(params,))
p.apply_async(domestic_org_data_first_generate.seatleft_data, args=(params,))
p.apply_async(domestic_org_generate_data_everyday.lowPrice_online_data, args=(params,))
p.apply_async(domestic_org_data_first_generate.orgPrice_data, args=(params,))
p.apply_async(domestic_org_data_first_generate.globalAirport_data, args=(params,))
p.close()
p.join()
#lowPrice_data里面是多进程，需要单独执行
domestic_org_data_first_generate.lowPrice_data_main(params)


# #调试情况下，可以顺序获取数据
# domestic_org_data_first_generate.infoBase_data(params)
# domestic_org_data_first_generate.globalAirport_data(params)
# domestic_org_data_first_generate.orgPrice_data(params)
# domestic_org_data_first_generate.lowPrice_data_main(params)
# domestic_org_data_first_generate.seatleft_data(params)
# domestic_org_generate_data_everyday.lowPrice_online_data(params)