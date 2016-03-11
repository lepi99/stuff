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
import pandas
import decimal

## logger settings

logger = logging.getLogger('TTT')
um.setup_logger('c:\\Temp\\', 'TTT', logger)
logger.info('start')

## create the connection:

es_client = ESWrapper(['es01.online-pmsi.com:9200'], logger)




############# move master data to a separate table

"""

index = 'horeca_master'
mapping = 'horeca_master'


# build the query

qry = ESQueryBuilder()
qry.add_term_query("must", "country_combined_", "Belgium", [])
qry.add_sort_order([{ "field_name": "place_id", "order": "asc" }])

# get doc counts for query
count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)

# query

#qry.add_size(count)
#qry.add_size(100) # restrict size for testing

results = es_client.get_data_for_qry(index, mapping, qry.es_query, total=count)
results = results['source']


es_client.bulk_index(es_client.es_client, 'diageo_intouch', 'belgium_master_base', results, 'pmsi_id')

"""

############# append the previously matched Id's to the new master


index = 'horeca_master'
mapping = 'horeca_master'


def get_master_record(country):
    # build the query
    qry = ESQueryBuilder()
    qry.add_term_query('must', 'country_combined_', country)
    qry.add_term_query('must', 'is_deleted', "false")
    qry.add_missing_field_filter('must', 'existing_outlet_id')
    qry.add_sort_order([{ "field_name": "pmsi_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)
    
    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results['source']
    
    return data



#path = 'c:\\Users\\gkerekes\\SharePoint\\Clients - Documents\\Diageo\\Switzerland\\'

ex = pd.io.excel.read_excel('UKCRMMatchesLess6Km.xlsx', 'GOOD_just_codes_2')


d = [{k:ex.values[i][v] for v,k in enumerate(ex.columns)} for i in range(len(ex)) ]


## full master data

master = get_master_record('United Kingdom')



n = 0
for crm_entries in d:
    # crm_entry_placeid = str(crm_entries['pmsi_id'])
    # crm_entry_crmid = crm_entries['existing_outlet_id']
    crm_entry_placeid = str(crm_entries['place_id_target'])
    crm_entry_crmid = crm_entries['place_id_source']
    doc = [x for x in master if x['pmsi_id'] == crm_entry_placeid]
    if len(doc) == 1:
        doc = doc[0]
        master_id = doc['pmsi_id']
        doc.update({"existing_outlet_id": crm_entry_crmid, "existing_outlet": True,"outlet_owner":crm_entries['Outlet_Owner'],"trade_type":crm_entries['Trade_Type'],"operator":crm_entries['Operator']})
        es_client.index_doc_with_id(doc, index, mapping, master_id)
        n+=1
    else:
        print(crm_entry_placeid," - ",crm_entry_crmid)

print(n)

############# index the crm table for matching


"""

path = 'c:\\Users\\gkerekes\\SharePoint\\Clients - Documents\\Diageo\\Netherlands\\'

ex = pandas.io.excel.read_excel(path + 'Last 2 months called BP univers  Benelux.xls', 'OnTrade Be', encoding='utf-8') #, keep_default_na = False)

s = ex.to_dict('records')


for docs in s:
    for keys in docs.keys():
        if str(docs[keys]) == 'nan':
            docs[keys] = ''
    outlet_id = docs['Klantnr']
    if docs['latitude'] != '':
        geopoint = str(docs['latitude']) + "," + str(docs['longitude'])
        docs.update({"geopoint" : geopoint})
    es_client.index_doc_with_id(docs, 'diageo_intouch', 'belgium_crm_updated_19062015', outlet_id)

"""

