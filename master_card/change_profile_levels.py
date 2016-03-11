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

logger = logging.getLogger('TTT')
um.setup_logger('c:\\Temp\\', 'TTT', logger)
logger.info('start')

## create the connection:

es_client = ESWrapper(['es01.online-pmsi.com:9200'], logger)


index = 'horeca_master_card'
mapping = 'horeca_master_card'


def get_master_record(country):
    # build the query
    qry = ESQueryBuilder()
    qry.add_term_query('must', 'country_combined_', country)
    qry.add_term_query('must', 'is_deleted', "false")
    #qry.add_missing_field_filter('must', 'existing_outlet_id')
    qry.add_sort_order([{ "field_name": "pmsi_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)

    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results#['source']

    return data

ff=get_master_record('United Kingdom')

for ii in ff['source']:

    if ii['pmsi_social_rank'] =='High-profile':
        ii['pmsi_social_rank'] ='High'
    elif ii['pmsi_social_rank'] =='Medium-profile':
        ii['pmsi_social_rank'] ='Medium'
    elif  ii['pmsi_social_rank'] =='Low-profile':
        ii['pmsi_social_rank'] ='Low'

    if ii['pmsi_social_rank_'] =='High-profile':
        ii['pmsi_social_rank_'] ='High'
    elif ii['pmsi_social_rank_'] =='Medium-profile':
        ii['pmsi_social_rank_'] ='Medium'
    elif  ii['pmsi_social_rank_'] =='Low-profile':
        ii['pmsi_social_rank_'] ='Low'


    if ii['Premiumness_rank'] =='High-profile':
        ii['Premiumness_rank'] ='High'
    elif ii['Premiumness_rank'] =='Medium-profile':
        ii['Premiumness_rank'] ='Medium'
    elif  ii['Premiumness_rank'] =='Low-profile':
        ii['Premiumness_rank'] ='Low'


    if ii['Business_Activeness_rank'] =='High-profile':
        ii['Business_Activeness_rank'] ='High'
    elif ii['Business_Activeness_rank'] =='Medium-profile':
        ii['Business_Activeness_rank'] ='Medium'
    elif  ii['Business_Activeness_rank'] =='Low-profile':
        ii['Business_Activeness_rank'] ='Low'

    if ii['Social_change'] > 5:
        ii['Social_change_rank']='green'
    elif ii['Social_change'] < -5:
        ii['Social_change_rank']='red'
    else:
        ii['Social_change_rank']='yellow'

    if ii['Premiumness_change'] > 5:
        ii['Premiumness_change_rank']='green'
    elif ii['Premiumness_change'] < -5:
        ii['Premiumness_change_rank']='red'
    else:
        ii['Premiumness_change_rank']='yellow'

    if ii['Business_activeness_change'] > 5:
        ii['Business_activeness_change_rank']='green'
    elif ii['Business_activeness_change'] < -5:
        ii['Business_activeness_change_rank']='red'
    else:
        ii['Business_activeness_change_rank']='yellow'

    #ii['Social_change']=ii['Social_change']*10
    #ii['Premiumness_change']=ii['Premiumness_change']*10
    #ii['Business_activeness_change']=ii['Business_activeness_change']*10

    print(ii)
    id=ii['pmsi_id']+"_0"
    es_client.index_doc_with_id(ii, "horeca_master_card","horeca_master_card", id)
