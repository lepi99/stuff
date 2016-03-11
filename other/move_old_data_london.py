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




index = 'horeca_raw'
mapping = 'horeca_raw'

def get_master_record_box1_new(country,api_source,scrape_id):
    # build the query
    qry = ESQueryBuilder()
    #qry.add_term_query('must', 'country_combined', country)
    #qry.add_term_query('must', 'is_deleted', 'false')

    qry.add_term_query('must', 'scrape_id', scrape_id)
    qry.add_term_query('must', 'api_source', api_source)
    #qry.add_term_query('must', 'outlet_classify', 'False')
    ##qry.add_term_query('must', 'api_source', 'google')
    #qry.add_geo_bounding_box_filter("must", "geopoint",  15.824, 45.849, 16.108, 45.766) #zagreb
    qry.add_geo_bounding_box_filter("must", "geopoint",  -0.50, 51.64, 0.15, 51.35) #London

    #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
    #qry.add_missing_field_filter('must', 'outlet_owner')
    qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)

    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results

    return data

def get_master_record_box1_old(country,api_source,scrape_id):
    # build the query
    qry = ESQueryBuilder()
    #qry.add_term_query('must', 'country_combined', country)
    #qry.add_term_query('must', 'is_deleted', 'false')

    qry.add_term_query('must', 'scrape_id', scrape_id)
    qry.add_term_query('must', 'api_source', api_source)
    qry.add_term_query('must', 'outlet_classify', 'false')
    ##qry.add_term_query('must', 'api_source', 'google')
    #qry.add_geo_bounding_box_filter("must", "geopoint",  15.824, 45.849, 16.108, 45.766) #zagreb
    qry.add_geo_bounding_box_filter("must", "geopoint",  -0.50, 51.64, 0.15, 51.35) #London

    #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
    #qry.add_missing_field_filter('must', 'outlet_owner')
    qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)

    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results

    return data

def get_master_record_box1_master_card(country,indexType):
    # build the query
    qry = ESQueryBuilder()
    #qry.add_term_query('must', 'country_combined', country)
    #qry.add_term_query('must', 'is_deleted', 'false')

    #qry.add_term_query('must', 'scrape_id', scrape_id)
    #qry.add_term_query('must', 'api_source', api_source)
    #qry.add_term_query('must', 'outlet_classify', 'False')
    ##qry.add_term_query('must', 'api_source', 'google')
    #qry.add_geo_bounding_box_filter("must", "geopoint",  15.824, 45.849, 16.108, 45.766) #zagreb
    qry.add_geo_bounding_box_filter("must", "geopoint",  -0.50, 51.64, 0.15, 51.35) #London

    #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
    #qry.add_missing_field_filter('must', 'outlet_owner')
    qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(indexType, indexType, qry.es_query)
    # query
    qry.add_size(count)

    results = es_client.get_data_for_qry(indexType, indexType, qry.es_query, page_size = 5000)
    data = results

    return data

gg=get_master_record_box1_master_card('United Kingdom','horeca_master')
#gg=get_master_record_box1_new('United Kingdom','google','139')
#for ss in gg['hits']:

#es_client.bulk_index(es_client.es_client, 'off_trade_raw', 'off_trade_raw', gg, 'pmsi_id')
#print(gg)
for ss in gg['source']:
    #ss['scrape_id']=1
    id=ss['pmsi_id']+"_0"
    print(ss)
    print(id)
    es_client.index_doc_with_id(ss, "horeca_master_card","horeca_master_card", id)

# ff=get_master_record_box1_old('United Kingdom','fb','130')
# for kk in ff['source']:
#     kk['scrape_id']=1
#     id=kk['place_id']+"_1"
#     if kk['pmsi_type_simple'] == []:
#        print(kk)
#        print(id)
#        print(kk['pmsi_type_simple'])
#        es_client.index_doc_with_id(kk, "off_trade_raw","off_trade_raw", id)