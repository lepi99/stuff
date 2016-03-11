__author__ = 'rfernandes'

import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'ReverseGeocoder', logger)
es_client = ES(['84.40.63.146:9200'], logger)


index1 = 'horeca_master'
mapping1 = 'horeca_master'

dt=pd.read_excel("spainCrmMatchesFinal.xlsx", sheetname="finalList",index_col=None, na_values=['NA'])

crmMatchL=[]
print(dt)
print("GHherrereerere")
gg=0
aa=0
for index, ii in dt.iterrows():
#for ii in crmMatchL:
    #print(ii)
    qry = ESQueryBuilder()
    #qry.add_missing_field_filter("must", "geopoint")
    #print(ii['place_id_target'])
    #print(ii['place_id_target'][:-4])
    qry.add_term_query('must', 'parent_place_id', ii['place_id_target'][:-4], [])
    qry.add_term_query('must', 'is_deleted', 'false', [])
    qry.add_term_query('must', 'country_combined_', 'United King', [])
    qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
    # get doc counts for query
#    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    #qry.add_size(count)
    data = es_client.get_data_for_qry(index1, mapping1, qry.es_query)
    if data !=None:
        if len(data['source'])==0:
            aa+=1
            #print(ii['place_id_target'])
        if len(data['source'])>1:
            print("there are more than 1 of this")
        if len(data['source'])==1:
            gg+=1
            #print("heeloo")
            #print(data['hits'])
            dd= data['hits'][0]
            idN=dd['_id']
            recordN=dd['_source']

            recordN['existing_outlet_id']=ii['place_id_source']
            recordN['existing_outlet']='true'
            print(idN)

            es_client.index_doc_with_id(recordN, index1,mapping1, idN)

print(aa)
print(gg)