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
#'pmsi_social_rank_'
#'pmsi_social_score'
for ii in ff['source']:
    if random.random()>0.90:
        vp=70
    else:
       vp=15

    if random.random()>0.90:
        vba=70
    else:
        vba=5

    ii['Premiumness']=min(random.gauss(ii['pmsi_social_score'],15),100)
    ii['Business_Activeness']=min(random.gauss(ii['pmsi_social_score'],5),100)
    ii['Social_change']=random.random()*3-1.5
    ii['Premiumness_change']=random.random()*1.2-0.6
    ii['Business_activeness_change']=random.random()*2-1




    if ii['Premiumness'] > 80:
        ii['Premiumness_rank']="High-profile"
    elif ii['Premiumness'] > 40:
        ii['Premiumness_rank']="Medium-profile"
    elif ii['Premiumness'] > 20:
        ii['Premiumness_rank']="Low-profile"
    else:
         ii['Premiumness_rank']="No profile"

    if ii['Business_Activeness'] > 80:
        ii['Business_Activeness_rank']="High-profile"
    elif ii['Business_Activeness'] > 40:
        ii['Business_Activeness_rank']="Medium-profile"
    elif ii['Business_Activeness'] > 20:
        ii['Business_Activeness_rank']="Low-profile"
    else:
         ii['Business_Activeness_rank']="No profile"


    if ii['Social_change'] > 0.5:
        ii['Social_change_rank']='green'
    elif ii['Social_change'] < -0.5:
        ii['Social_change_rank']='green'
    else:
        ii['Social_change_rank']='yellow'

    if ii['Premiumness_change'] > 0.5:
        ii['Premiumness_change_rank']='green'
    elif ii['Premiumness_change'] < -0.5:
        ii['Premiumness_change_rank']='green'
    else:
        ii['Premiumness_change_rank']='yellow'

    if ii['Business_activeness_change'] > 0.5:
        ii['Business_activeness_change_rank']='green'
    elif ii['Business_activeness_change'] < -0.5:
        ii['Business_activeness_change_rank']='green'
    else:
        ii['Business_activeness_change_rank']='yellow'

    id=ii['pmsi_id']+"_0"
    es_client.index_doc_with_id(ii, "horeca_master_card","horeca_master_card", id)
