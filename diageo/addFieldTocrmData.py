__author__ = 'rfernandes'

import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'ReverseGeocoder', logger)
es_client = ES(['84.40.63.146:9200'], logger)


index = 'diageo_intouch'
mapping = 'spain_crm_updated_11062015'

qry = ESQueryBuilder()
#qry.add_missing_field_filter("must", "geopoint")
qry.add_sort_order([{ "field_name": "COD OUTLET", "order": "asc" }])
# get doc counts for query
count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
qry.add_size(count)
data = es_client.get_data_for_qry(index, mapping, qry.es_query)

for dd in data['hits']:
    idN=dd['_id']
    recordN=dd['_source']
    if recordN['Phone Number'] != None:
        recordN['phone_trimmed']=recordN['Phone Number'].replace(" ","")[-6:]
    else:
        recordN['phone_trimmed']=recordN['Phone Number']
    es_client.index_doc_with_id(recordN, index,mapping, idN)