__author__ = 'rfernandes'

import logging
#import csv
#from math import isnan
#import pandas as pd
from utility import helper
from pylastic import ESWrapper, ESQueryBuilder
#import uuid
#import random
#import arrow
#import json
import unicodedata

class match_salesalign_with_crm():

    #{Name:"",Phone:"",Address:"",email:"",post_code:"",lat_lon:[],lon_lat:[],lat:"",lon:""}
    def __init__(self, country=None):
        self.logger = logging.getLogger('crm_matching')
        helper.setup_logger('c:\\temp\\', 'crm_matching', self.logger)
        self.config_file_path='mappings_config.json'
        self.es = ESWrapper(['es01.online-pmsi.com'], self.logger)

        self.country = country

        self.master_index = 'horeca_master'
        self.master_type = 'horeca_master'

        self.master_matching_index = 'master_matching_reference'
        self.master_matching_type = 'master_matching_reference'

        self.crm_index = 'crm_matching'
        self.crm_type = 'crm_matching2'

        self.crm_matching_index = 'crm_matching_reference'
        self.crm_matching_type = 'crm_matching_reference'

        self.version=None



#print("aa")

#es_client.es_client.delete_by_query()

    def get_master_record(self,country,version):
        # build the query
        qry = ESQueryBuilder()
        qry.add_term_query('must', 'country_combined_', country)
        qry.add_term_query('must', 'version', str(version))
        qry.add_term_query('must', 'is_deleted', 'false')
        ##qry.add_term_query('must', 'api_source', 'google')
        #qry.add_geo_bounding_box_filter("must", "geopoint",  23.494, 46.795, 23.695, 46.745)
        #qry.add_missing_field_filter("must_not", "existing_outlet")
        #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
        #qry.add_missing_field_filter('must', 'outlet_owner')
        qry.add_sort_order([{ "field_name": "pmsi_id", "order": "asc" }])
        # get doc counts for query
        #count = self.es.get_doc_count_for_qry(self.master_index, self.master_type, qry.es_query)
        # query
        #qry.add_size(count)

        results = self.es.search(self.master_index, self.master_type, qry.es_query, total = 0)
        data = results['source']

        return data

    def get_crm_record(self,country,version_crm):
        # build the query
        qry = ESQueryBuilder()
        qry.add_term_query('must', 'country', country)
        qry.add_term_query('must', 'version', str(version_crm))
        #qry.add_term_query('must', 'is_deleted', 'false')
        ##qry.add_term_query('must', 'api_source', 'google')
        #qry.add_geo_bounding_box_filter("must", "geopoint",  23.494, 46.795, 23.695, 46.745)
        #qry.add_missing_field_filter("must_not", "existing_outlet")
        #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
        #qry.add_missing_field_filter('must', 'outlet_owner')
        qry.add_sort_order([{ "field_name": "CRM_id", "order": "asc" }])
        # get doc counts for query
        #count = self.es.get_doc_count_for_qry(self.crm_index, self.crm_type, qry.es_query)
        # query
        #qry.add_size(count)

        results = self.es.search(self.crm_index, self.crm_type, qry.es_query, total = 0)
        data = results['source']

        return data

#path = 'c:\\Users\\gkerekes\\SharePoint\\Clients - Documents\\2015\\Diageo\\UK project\\CRM Matching\\'

#ex = pd.io.excel.read_excel(path + 'ONTRADE OUTLET MASTER - August 2015 v3 (FINAL).xlsx', 'crm_vars')


#d = [{k:ex.values[i][v] for v,k in enumerate(ex.columns)} for i in range(len(ex)) ]


## full master data
    def strip_accents(self,s):
       return ''.join(c for c in unicodedata.normalize('NFD', s)
                      if unicodedata.category( c ) != 'Mn')

    def insert_master_in_mastching_elastic(self,master_data):
        data=["aa"]
        while len(data)>0:
            qryD = ESQueryBuilder()
            qryD.add_match_all_query()
            results = self.es.search(self.master_matching_index, self.master_matching_type, qryD.es_query, total = 0)
            data = results['source']
    #        results = self.es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
            self.es.bulk_delete(self.es.es_client,self.master_matching_index, self.master_matching_type, data,'pmsi_id')
        #self.es.bulk_delete_by_query(self.master_matching_index, self.master_matching_type, qryD.es_query)
        clean_master=[]
        for mm in master_data:
            if "existing_outlet_id" in mm:
                del mm["existing_outlet_id"]
            mm["Name_m"]=self.strip_accents(mm["Name_m"])
            if "street" in mm:
                mm["street"]=self.strip_accents(mm["street"])
            if "phone" in mm:
                mm["phone_t"]=mm["phone"].replace(" ","").replace(".","").replace("-","").replace(",","")[-7:]
            clean_master.append(mm)
            #self.es.index_doc_with_id(mm, self.master_matching_index,self.master_matching_index,mm['pmsi_id'])
            #self.es.search(self.es.es_client, self.master_matching_index, self.master_matching_type, master_data, 'pmsi_id')
        self.es.bulk_index(self.es.es_client, self.master_matching_index, self.master_matching_type, clean_master, 'pmsi_id')

    def insert_crm_in_mastching_elastic(self,crm_data):
        qryD = ESQueryBuilder()
        qryD.add_match_all_query()
        results = self.es.search(self.crm_matching_index, self.crm_matching_type, qryD.es_query, total = 0)
        data = results['source']
#        results = self.es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
        self.es.bulk_delete(self.es.es_client,self.crm_matching_index, self.crm_matching_type, data,'unique_id')
        new_crm=[]
        for mm in crm_data:
            mm["Name"]=self.strip_accents(mm["Name"])
            mm["Address"]=self.strip_accents(mm["Address"])
            if "Phone" in mm:
                mm["phone_t"]=mm["Phone"].replace(" ","").replace(".","").replace("-","").replace(",","")[-7:]
            new_crm.append(mm)
        self.es.bulk_index(self.es.es_client, self.crm_matching_index, self.crm_matching_type, new_crm, 'unique_id')

    def prepare_matching(self,country,master_version,crm_version):
        mr=self.get_master_record(country,master_version)
        cr=self.get_crm_record(country,crm_version)
        self.insert_master_in_mastching_elastic(mr)
        self.insert_crm_in_mastching_elastic(cr)

# master = get_master_record('Italy')
#
#
# self.es_client.bulk_index(es_client.es_client, 'diageo_intouch', 'italy_master_20151007', master, 'pmsi_id')
# self.es_client.bulk_index(es_client.es_client, 'diageo_intouch', 'italy_master_20151007', master, 'pmsi_id')
msc=match_salesalign_with_crm(country="Sweden")
# print(msc.strip_accents( "Bonêt d'âne, Le"))
msc.prepare_matching("Sweden",1,5)
# msc.prepare_matching("Belgium",0,15)

if False:
        logger = logging.getLogger('crm_matching')
        helper.setup_logger('c:\\temp\\', 'crm_matching', logger)

        es = ESWrapper(['es01.online-pmsi.com'], logger)

        country = 'Belgium'
        master_index = 'horeca_master'
        master_type = 'horeca_master'
        qry = ESQueryBuilder()
        qry.add_term_query('must', 'country_combined_', country)
        qry.add_term_query('must', 'version', str(0))
        qry.add_term_query('must', 'is_deleted', 'false')
        ##qry.add_term_query('must', 'api_source', 'google')
        #qry.add_geo_bounding_box_filter("must", "geopoint",  23.494, 46.795, 23.695, 46.745)
        #qry.add_missing_field_filter("must_not", "existing_outlet")
        #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
        #qry.add_missing_field_filter('must', 'outlet_owner')
        qry.add_sort_order([{ "field_name": "pmsi_id", "order": "asc" }])
        # get doc counts for query
        count = es.get_doc_count_for_qry(master_index, master_type, qry.es_query)
        # query
        qry.add_size(count)

        results = es.get_data_for_qry(master_index, master_type, qry.es_query)
        data = results['source']