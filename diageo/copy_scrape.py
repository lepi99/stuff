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
import pandas
import decimal
import random
## logger settings
import time
logger = logging.getLogger('TTT')
um.setup_logger('c:\\Temp\\', 'TTT', logger)
logger.info('start')

## create the connection:

es_client = ESWrapper(['es01.online-pmsi.com:9200'], logger)

index = 'horeca_raw'
mapping = 'horeca_raw'

def get_master_record(scrpd):
    # build the query
    qry = ESQueryBuilder()
    qry.add_term_query('must', 'scrape_id', scrpd)
    #qry.add_term_query('must', 'is_deleted', "false")
    #qry.add_missing_field_filter('must', 'existing_outlet_id')
    qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)

    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results['source']
    return data

#time.sleep(60*60*5)
ss=get_master_record('157')
for ii in ss:
    ii['scrape_id']='170'
    id=ii['place_id']+'_170'
    ii['doc_id']=id
    print(ii)
    es_client.index_doc_with_id(ii, index,mapping, id)

time.sleep(60*10)
ss=get_master_record('158')
for ii in ss:
    ii['scrape_id']='171'
    id=ii['place_id']+'_171'
    ii['doc_id']=id
    print(ii)
    es_client.index_doc_with_id(ii, index,mapping, id)

time.sleep(60*10)
ss=get_master_record('156')
for ii in ss:
    ii['scrape_id']='172'
    id=ii['place_id']+'_172'
    ii['doc_id']=id
    print(ii)
    es_client.index_doc_with_id(ii, index,mapping, id)