from AlignClient import AlignClient ,AlignUtil
import logging
from utility import helper
import json
import csv 

modelCode = "salesAlign";
baseUrl = "www.sales-align.com:80"
apiPrefix='/Diageo/api/v1/'




userName = "gkerekes@pmsi-consulting.com";

password = "gez37KER";




logger = logging.getLogger('DataSync')
helper.setup_logger('C:\\temp\\', 'DataSync',logger, True,logging.INFO)

client =  AlignClient(baseUrl,apiPrefix,modelCode,userName,password,logger)

filters = [ {"fieldName":"Outlet Country","value":["Spain"]}];

outletItemTypeID = client.getOutleItemTypeId();

count = 0
pageSize = 1000

data  = client.getItems(outletItemTypeID,'',count,pageSize, filters)

totalCount = data["TotalCount"]

processdData =  AlignUtil.transformDataFromAlignSA(data['Data'],data['Fields'],True,True)

file = open('outputSpain.csv', 'w+')
try:
    first = True
    while totalCount > count:
        for r in processdData:              
            if first == True:
                first = False
                writer = csv.DictWriter(file, fieldnames=r.keys(),delimiter=',', lineterminator='\n')
                writer.writeheader()
            writer.writerow(r)
        count = count+pageSize
        data  = client.getItems(outletItemTypeID,'',count, pageSize, filters)    
        processdData =  AlignUtil.transformDataFromAlignSA(data['Data'],data['Fields'],True,True)      
finally:
    file.close()
