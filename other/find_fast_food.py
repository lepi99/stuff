__author__ = 'rfernandes'

import logging
import os
from pylastic import ESWrapper, ESQueryBuilder
import sys
from utility import helper as um
from PandaWrapper import PylasticPanda, JsonToPanda
import pandas as pd
import json
import numpy
from elasticsearch import Elasticsearch
sys.path.append(os.path.dirname(os.getcwd()))
#sys.path.append('C:\\Users\\rfernandes\\Documents\\rfernandes\\Hulk_TheOutletMapper\\OutletDataCleaning\\')
#import master_insert.categorise_opening_hours as opening
#import DG_functions as DG_functions
from datetime import datetime
from time import gmtime, strftime
import random
import arrow
## logger settings

logger = logging.getLogger('DataCleanup')
um.setup_logger('c:\\Temp\\', 'DataCleanup', logger,add_console=True, log_level=logging.INFO)
logger.info('start')


es_client = ESWrapper(['84.40.63.146:9200','84.40.63.147:9200'], logger)
# es_client = ESWrapper(['es01.online-pmsi.com:9200'], logger)
# es_panda_client = PylasticPanda(['es01.online-pmsi.com:9200'], logger)
es_panda_client = PylasticPanda(['84.40.63.146:9200','84.40.63.147:9200'], logger)

def get_country_outlets(current_version=None,country=None):

            qry = ESQueryBuilder()
            qry.add_term_query('must_not', 'outlet_state_indicator', "closed")
            qry.add_term_query('must', 'version', str(current_version))
            qry.add_term_query('must', 'country_combined_', country)
            qry.add_term_query('must_not', 'is_deleted', 'true')
            count = es_client.get_doc_count_for_qry('test_horeca_master','test_horeca_master', qry.es_query)
            qry.add_size(count)
            doc_response_cur = es_client.get_data_for_qry('test_horeca_master','test_horeca_master', qry.es_query)
            #ls = list(doc_response_cur.pmsi_id)
            #cur = list(set(ls))
            return doc_response_cur

chain_restaurants = {"Papa John's ":["papa john's","Papa Johns"],"Portugália":["portugalia","portugália"],"Wok to Walk":["wok to walk"],"100 Montaditos":["100 montaditos"],"Carluccio's":["carluccio's"],"Real Greek":["real greek"],"Chiquito":["chiquito"],"frankie & benny's":["frankie and benny","frankie benny's","frankie & bennys"],"Pizza Express":["pizza express"],"Angus Steakhouse":["angus steakhouse"],"Giraffe":["giraffe"],"Prezzo":["prezzo"],"Zizzi":["zizzi"],"Pret A Manger":["pret a manger"],"J D Wetherspoon":["j d wetherspoon","jd wetherspoon"],"Bella Italia":["bella italia"],"Nando's":["nando's","nandos"],"Gourmet Burger Kitchen":["gourmet burger kitchen","gbk"],"Wagamama":["wagamama"],"Bakers Oven":["bakers oven"],"Greggs":["greggs"],"Pans":["pans"],"Bocatta":["bocatta"],"Yo! Sushi":["yo! sushi","yo!sushi"],"Goody’s":["goody’s","goodys"],"Spudulike":["spudulike"],"Sibylla":["sibylla"],"Kochlöffel":["kochlöffel","kochloffel"],"Flunch":["flunch"],"Kotipizza":["kotipizza"],"Chicken Cottage":["chicken cottage"],"Hesburger":["hesburger"],"Max Burgers":["max burgers"],"Wimpy":["wimpy"],"Supermac's":["supermac's",'supermacs','supermac'],'Telepizza':['telepizza'],'KFC':['kfc'], 'Burger King':['burger king','burgerking'], "Mcdonald's":['mcdonalds',"mcdonald's","mc donalds","macdonalds",'mcdonald'], 'Pizza-Hut':['pizza-hut','pizza hut'], 'Subway':['subway'],"Domino's":["domino's","dominos"],}

chain_bar_pubs={"Bar Room Bar":["bar room bar"],"O'Neill's":["o'neill's","o'neills"],"All Bar One":["all bar one"],"Punch Taverns":["punch taverns"]}

chain_cafe={'Starbucks':['starbucks'],"Costa Coffe":["costa coffe"],"Caffè Nero":["caffe nero","caffè nero"],"Café Rouge":["café rouge","cafe rouge"],"AMT Coffee":["amt coffee"]}

def chain_and_fast_food(current_version=None,country=None):
    dd=get_country_outlets(current_version=current_version,country=country)

    for aa in dd['hits']:
        idN=aa['_id']
        ss=aa['_source']
        print(ss)
        restInsertFlag=False
        barInsertFlag=False
        cafeInsertFlag=False
        if ss['pmsi_type_simple'] == 'Restaurant':
            for jj in chain_restaurants:
                for jj2 in chain_restaurants[jj]:
                    if jj2 in ss['Name_m']:
                        restInsertFlag=True
                        ss['is_chain']='true'
                        ss['chain_name']=jj

        if ss['pmsi_type_simple'] == 'Bar':
            for jj in chain_bar_pubs:
                for jj2 in chain_bar_pubs[jj]:
                    if jj2 in ss['Name_m']:
                        barInsertFlag=True
                        ss['is_chain']='true'
                        ss['chain_name']=jj

        if ss['pmsi_type_simple'] == 'Cafe':
            for jj in chain_cafe:
                for jj2 in chain_cafe[jj]:
                    if jj2 in ss['Name_m']:
                        cafeInsertFlag=True
                        ss['is_chain']='true'
                        ss['chain_name']=jj

        if restInsertFlag or barInsertFlag or cafeInsertFlag:
            es_client.index_doc_with_id(ss,'test_horeca_master','test_horeca_master', idN)
        # if idN == aa['pmsi_id_version']:
        #     print(aa['pmsi_id_version'])
        if 'pmsi_keywords' in ss:
          if 'kebab' in ss['pmsi_keywords'] or 'kebabs' in ss['pmsi_keywords'] or 'kebab' in ss['Name_m']:
            print(ss['pmsi_type_simple'])
            print(ss['Name_m'])
            print(ss['pmsi_keywords'])
            print(ss['pmsi_keywords_phrases'])
        #print(idN)
        #print(ss)