__author__ = 'rfernandes'


import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'testVelocity', logger)
es_client = ES(['84.40.63.146:9200'], logger)
index = 'horeca_master'
mapping = 'horeca_master'


qry = ESQueryBuilder()
#qry.add_missing_field_filter("must", "geopoint")
qry.add_term_query('must', 'is_deleted', 'false')
qry.add_term_query('must', 'country_combined_', 'United Kingdom')
# qry.add_term_query('must', 'base_id', str(delePmsi))
qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
# get doc counts for query
count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
qry.add_size(count)
data = es_client.get_data_for_qry(index, mapping, qry.es_query)

data = data['source']
couta=0
cout0=0
cout1=0
cout2=0
cout3=0
cout4=0
cout5=0
cout6=0
for doc in data:
    master_id=doc['base_id']
    if 'pmsi_subtype_simple' in doc:
      cout0+=1
      if doc['pmsi_type_simple'].lower() in  ['restaurant','cafe','café']:
        cout1+=1
        doc['pmsi_priority_']=''
        doc['pmsi_priority']=''
        doc['pmsi_priority_prev']=doc['pmsi_priority']
        es_client.index_doc_with_id(doc, index, mapping, master_id)

      elif doc['pmsi_subtype_simple'].lower() in  ['pizza restaurant','tapas bar']:
        cout2+=1
        #print(doc['pmsi_type_simple'])
        #print(doc['pmsi_subtype_simple'])
        doc['pmsi_priority_']=''
        doc['pmsi_priority']=''
        doc['pmsi_priority_prev']=doc['pmsi_priority']
        es_client.index_doc_with_id(doc, index, mapping, master_id)

      elif doc['pmsi_subtype_simple'].lower() in  ['restaurant','cafe','café']:
        cout3+=1
        #print(doc['pmsi_type_simple'])
        #print(doc['pmsi_subtype_simple'])
        doc['pmsi_priority_']=''
        doc['pmsi_priority']=''
        doc['pmsi_priority_prev']=doc['pmsi_priority']
        es_client.index_doc_with_id(doc, index, mapping, master_id)

      elif doc['pmsi_type_simple'].lower() in  ['fast food']:
        couta+=1
        #print("------------------------------------------")
        #print(doc['pmsi_type_simple'])
        #print(doc['pmsi_subtype_simple'])
        doc['pmsi_priority_']=''
        doc['pmsi_priority']=''
        doc['pmsi_priority_prev']=doc['pmsi_priority']
        es_client.index_doc_with_id(doc, index, mapping, master_id)
      else:
        cout4+=1
        #print("------------------------------------------")
        #print(doc['pmsi_type_simple'])
        #print(doc['pmsi_subtype_simple'])
        if 'cafe' in doc['Name_m'].lower() or 'bistro' in doc['Name_m'].lower() or 'deli' in doc['Name_m'].lower() or 'studio' in doc['Name_m'].lower() or 'café' in doc['Name_m'].lower():
            cout5+=1
            # print(doc['Name_m'])
            doc['pmsi_priority_']='Low-priority'
            doc['pmsi_priority']='Low-priority'
            es_client.index_doc_with_id(doc, index, mapping, master_id)
            #print("")
    else:
        print("------------------------------------------")
        #print(doc['pmsi_type_simple'])
        #print(doc['pmsi_subtype_simple'])
        print(doc['Name_m'])
        cout6+=1
    # [x.update({"pmsi_priority_" : x['pmsi_priority'],"pmsi_social_rank_" : x['pmsi_social_rank']}) for x in data]
    #print("ee")
    # es_client.index_doc_with_id(doc, index, mapping, master_id)
#es_client.bulk_index(es_client.es_client, index, mapping, data, "base_id", bulk_size=1000)
print("cout0 ",cout0)
print("cout1 ",cout1)
print("cout2 ",cout2)
print("cout3 ",cout3)
print("couta ",couta)
print("cout4 ",cout4)
print("cout5 ",cout5)
print("cout6 ",cout6)
