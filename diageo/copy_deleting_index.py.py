__author__ = 'rfernandes'

import logging
import os
from pylastic import ESWrapper, ESQueryBuilder
import sys
from utility import helper as um
#from PandaWrapper import PylasticPanda, JsonToPanda
import pandas as pd
import json
from elasticsearch import Elasticsearch


logger = logging.getLogger('TTT')
um.setup_logger('c:\\Temp\\', 'TTT', logger)
logger.info('start')

## create the connection:

es_client = ESWrapper(['es01.online-pmsi.com:9200'], logger)




index = 'horeca_master'
mapping = 'horeca_master'

print("aa")

#es_client.es_client.delete_by_query()

def get_master_record(country):
    # build the query
    qry = ESQueryBuilder()
    qry.add_term_query('must', 'country_combined_', country)
    qry.add_term_query('must', 'is_deleted', 'false')
    ##qry.add_term_query('must', 'api_source', 'google')
    #qry.add_geo_bounding_box_filter("must", "geopoint",  23.494, 46.795, 23.695, 46.745)
    #qry.add_missing_field_filter("must_not", "existing_outlet")
    #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
    #qry.add_missing_field_filter('must', 'outlet_owner')
    qry.add_sort_order([{ "field_name": "pmsi_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)

    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results['source']
    return data


#path = 'c:\\Users\\gkerekes\\SharePoint\\Clients - Documents\\2015\\Diageo\\UK project\\CRM Matching\\'

#ex = pd.io.excel.read_excel(path + 'ONTRADE OUTLET MASTER - August 2015 v3 (FINAL).xlsx', 'crm_vars')


#d = [{k:ex.values[i][v] for v,k in enumerate(ex.columns)} for i in range(len(ex)) ]


## full master data

master = get_master_record('Finland')


es_client.es_client.delete_by_query('horeca_match_place', 'horeca_match',{"query": {"match_all": {}}})
es_client.bulk_index(es_client.es_client, 'horeca_match_place', 'horeca_match', master, 'pmsi_id')


