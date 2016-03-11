__author__ = 'rfernandes'

import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'ReverseGeocoder', logger)
es_client = ES(['84.40.63.146:9200'], logger)
from fb_graph_api import graph_api

index = 'horeca_master'
mapping = 'horeca_master'
fb = graph_api(logger)
qry = ESQueryBuilder()
#qry.add_missing_field_filter("must", "geopoint")

qry.add_term_query('must', 'api_source', 'fb', [])
qry.add_term_query('must', 'details_fetched_source', 'set', [])
qry.add_term_query('must_not', 'is_community_page', 'true', [])
qry.add_term_query('must', 'country_combined_', 'Thailand', [])
qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
# get doc counts for query
count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
qry.add_size(count)
data = es_client.get_data_for_qry(index, mapping, qry.es_query)
fb_tokens = ['681143818643788|U_-YXwnMJXBU6Gfy71eLs-Bzk2o', '284165735125183|9NK4pA4Nupqx9hlpmEe31NASv0c']
key_switch = 0
for hh in data['hits']:

    print(hh)
    idc=hh['_id']
    print(hh['_source'])
    gg=fb.get_place_details(hh['_source']['parent_place_id'], fb_tokens[key_switch])
    print(gg)
    if 'cover' in gg:
        hh['_source']['cover_image']=gg['cover']['source']
        print(hh['_source']['cover_image'])
        print(gg['cover']['source'])
        es_client.index_doc_with_id(hh['_source'], index,mapping, idc)
        #print("22"+22)
    else:
        if 'cover_image' in hh['_source']:
            print()
            print("----deleted--------")
            del hh['_source']['cover_image']
            print(hh['_source'])
            es_client.index_doc_with_id(hh['_source'], index,mapping, idc)
    if key_switch == 0:
            key_switch = 1
    else:
            key_switch = 0

print("finish")
#dd=pd.DataFrame(data['source'])
#dd.to_csv('ThaiFile_horeca.csv')
#print(count)