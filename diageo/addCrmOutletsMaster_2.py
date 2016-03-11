# -*- coding: utf-8 -*-
"""
Created on Tue May 26 12:45:43 2015

@author: gkerekes
"""

import logging
import os
from pylastic import ESWrapper, ESQueryBuilder
import sys
from utility import helper as um
#from PandaWrapper import PylasticPanda, JsonToPanda
import pandas as pd
import json
from elasticsearch import Elasticsearch
#sys.path.append('c:\\Users\\gkerekes\\Documents\\GitHub\\python-incubator\\diageo_outlet\\')
#import DG_functions

## logger settings

logger = logging.getLogger('TTT')
um.setup_logger('c:\\Temp\\', 'TTT', logger)
logger.info('start')

## create the connection:

es_client = ESWrapper(['es01.online-pmsi.com:9200'], logger)




index = 'horeca_master'
mapping = 'horeca_master'


def get_master_record(country):
    # build the query
    qry = ESQueryBuilder()
    qry.add_term_query('must', 'country_combined_', country)
    #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
    qry.add_missing_field_filter('must', 'outlet_owner')
    qry.add_sort_order([{ "field_name": "pmsi_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)
    
    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results['source']
    
    return data


#path = 'c:\\Users\\rfernandes\\\Documents\\Rfernandes\\Matchalytic\\'

# ex = pd.io.excel.read_excel(path + 'UK_crm_things_to_change_End.xlsx', 'bads')
# ex = pd.io.excel.read_excel(path + 'UK_crm_things_to_change_End.xlsx', 'bads2')
ex = pd.io.excel.read_excel('UK_crm_things_to_change_End.xlsx', 'good_stuff_to_redoo_in_mast_2')
print(ex)
#d=ex.to_dict(orient='records')
#d = [{k:ex.values[i][v] for v,k in enumerate(ex.columns)} for i in range(len(ex)) ]


## full master data

#master = get_master_record('United Kingdom')
n = 0
for index2, crm_entries in ex.iterrows():
#for crm_entries in d:
    print(crm_entries)
    print(crm_entries['current_match'])
    crm_entry_placeid = str(crm_entries['current_match'])
    #dc = [x for x in master if x['pmsi_id'] == crm_entry_placeid]
    qry = ESQueryBuilder()
    qry.add_term_query('must', 'country_combined_', 'United Kingdom')
    qry.add_term_query('must', 'pmsi_id', crm_entry_placeid)
    qry.add_term_query('must', 'is_deleted', 'false')
    #qry.add_term_query('must', 'existing_outlet', 'true')
    #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
    #qry.add_missing_field_filter('must', 'outlet_owner')
    qry.add_sort_order([{ "field_name": "pmsi_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)

    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results['source']
    if len(data) >= 1:
        for doc in data:
            master_id = doc['pmsi_id']
            #doc.update({"existing_outlet": False, "existing_outlet_id": "", "operator": '', "trade_type": ''})
            doc.update({"existing_outlet": True, "existing_outlet_id": crm_entries['place_id_source'], "operator": crm_entries['Outlet_Owner'], "trade_type": crm_entries['Trade_Type']})
            print(doc)
            n+=1
            print(n)
            es_client.index_doc_with_id(doc, index, mapping, master_id)

print(n)

# n = 0
# for crm_entries in d:
#     crm_entry_placeid = str(crm_entries['current_match'])
#     dc = [x for x in master if x['pmsi_id'] == crm_entry_placeid]
#     if len(dc) >= 1:
#         for doc in dc:
#             master_id = doc['pmsi_id']
#             doc.update({"existing_outlet": False, "existing_outlet_id": "", "operator": '', "trade_type": ''})
#             # doc.update({"existing_outlet": True, "existing_outlet_id": crm_entries['place_id_source']})# , "operator": crm_entries['outlet_operator']})
#             #es_client.index_doc_with_id(doc, index, mapping, master_id)
#             n+=1
