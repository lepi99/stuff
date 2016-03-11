__author__ = 'rfernandes'

import arrow
from MatchingMod import Matcher
from pylastic import ESQueryBuilder,ESWrapper
import json
import uuid
import logging
import os
from utility import helper
import pandas as pd
from sklearn.externals import joblib
import sys
from fuzzywuzzy import fuzz
import time
import unicodedata
import math
from math import isnan

class matcherWorker():

    def __init__(self,indexMatch='outlet_matching',mapMatch='matches_test',pathFiles=None,
                 masterRefindexMatch='master_matching_reference',masterRefmapMatch='master_matching_reference',
                 CRMindexMatch='crm_matching_reference',CRMmapMatch='crm_matching_reference',
                 config_file_path='configuration.json',
                 changesFile='changesMade.txt',
                 esList=['84.40.63.146:9200','84.40.63.147:9200']):#,'es02.online-pmsi.com:80','es03.online-pmsi.com:80','es04.online-pmsi.com:80']):
                 # esList=['es01.online-pmsi.com:9200']):#,'es02.online-pmsi.com:80','es03.online-pmsi.com:80','es04.online-pmsi.com:80']):
        self.logger = logging.getLogger('Crawler')

        helper.setup_logger('c:\\temp\\', 'Crawler', self.logger, add_console=False, log_level=logging.ERROR)
        #self.logger.setLevel('INFO')  # to over-ride this, needs to be after the setup
        self.logger.propagate = True
        self.config_file_path=config_file_path
        self.m=Matcher(es_endpoint=esList)#,logger=self.logger)
        # self.es_client = ESWrapper(['es01.online-pmsi.com:9200'])
        self.es_client = ESWrapper(['84.40.63.148:9200','84.40.63.149:9200'],self.logger)
        # self.es_client = ESWrapper(['es01.online-pmsi.com:9200'], self.logger)
        self.masterRefindexMatch=indexMatch
        self.masterRefmapMatch=mapMatch

        self.CRMindexMatch=indexMatch
        self.CRMmapMatch=mapMatch

        self.changesFile=changesFile
        #self.weights_path_m =self.getLocation(pathFiles)

    # def getLocation(self,pathFiles):
    #     if pathFiles == None:
    #       if sys.argv[0] == '':
    #         return 'C:\\Users\\rfernandes\\Documents\\Rfernandes\\horeca_classification'
    #       return os.path.dirname(sys.argv[0])
    #     else:
    #         return pathFiles

    def load_config(self,c_file_path=None):
        if c_file_path == None:
            c_file_path=self.config_file_path
        try:
            self.m.logger.info("Loading config...")
            with open(c_file_path, "r") as file:
                data = file.read()
                return json.loads(data)
        except Exception as ex:
            self.m.logger.error("Error while loading config - " + str(ex))


    def strip_accents(self,s):
       return ''.join(c for c in unicodedata.normalize('NFD', s)
                      if unicodedata.category( c ) != 'Mn')

    def get_crm_data(self,config):
        qry = ESQueryBuilder().add_match_all_query()
        print(config)
        doc_count = self.m.elastic.get_doc_count_for_qry(config['source_idx'], config['source_mapping'], qry.es_query)
        m_data = self.m.elastic.get_data_for_qry(config['source_idx'], config['source_mapping'],
                                             qry.es_query,total=doc_count)
        return m_data['source']

    def get_master_data(self,config):
        qry = ESQueryBuilder().add_match_all_query()
        doc_count = self.m.elastic.get_doc_count_for_qry(config['target_idx'], config['target_mapping'], qry.es_query)
        m_data = self.m.elastic.get_data_for_qry(config['target_idx'], config['target_mapping'],
                                             qry.es_query,total=doc_count)
        return m_data['source']

    def get_records_to_match(self,config, mapping_name):
        # m_data = m.elastic.get_all_data(config['source_idx'], m_config['source_mapping'])

        qry = ESQueryBuilder().add_match_all_query()#.add_size(20)
        #qry.add_term_query('must', 'outlet_classify', 'true')
        #qry.add_term_query('must', 'details_fetched', 'true')

        #Hack to speed up the bits I need - Sorry Rodrigo!!
        #qry.add_term_query('must', 'pmsi_type_simple', 'Nightclub')
        #qry.add_term_query('must', 'pmsi_type_simple', 'Hotel')

        #qry.add_term_query('must', 'scrape_id', str(scrape_id))




        #sort_order = [{"field_name": "place_id", "order": "desc" }]
        #qry.add_sort_order(sort_order)

        doc_count = self.m.elastic.get_doc_count_for_qry(config['source_idx'], config['source_mapping'], qry.es_query)
        m_data = self.m.elastic.get_data_for_qry(config['source_idx'], config['source_mapping'],
                                             qry.es_query,total=doc_count)
        m_records = []
        for hit in m_data['hits']:
            #if hit["_source"]["ned_flag"] == "new":
             record = {
                'src_hit': hit,
                #'scrape_id': scrape_id,
                'match_run_id': config['match_run_id'],
                'config': config,
                'same_source': config['same_source'],
                'matching_rules_id': mapping_name
             }

             m_records.append(record)
        #print(len(record))
        return m_records



    def distance_on_unit_sphere(self,longlat1, longlat2):
       if len(longlat1)==2 and len(longlat2)==2:
            #print("-----------")
            #print(longlat1)
            #print(longlat2)
          # if not(isnan(longlat1[0])) and not(isnan(longlat2[0])):
            lat1=longlat1[1]
            long1=longlat1[0]
            lat2=longlat2[1]
            long2=longlat2[0]

            # Convert latitude and longitude to
            # spherical coordinates in radians.
            degrees_to_radians = math.pi/180.0

            # phi = 90 - latitude
            phi1 = (90.0 - lat1)*degrees_to_radians
            phi2 = (90.0 - lat2)*degrees_to_radians

            # theta = longitude
            theta1 = long1*degrees_to_radians
            theta2 = long2*degrees_to_radians

            # Compute spherical distance from spherical coordinates.

            # For two locations in spherical coordinates
            # (1, theta, phi) and (1, theta', phi')
            # cosine( arc length ) =
            #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
            # distance = rho * arc length

            cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) +
                   math.cos(phi1)*math.cos(phi2))
            arc = math.acos( cos )

            # Remember to multiply arc by the radius of the earth
            # in your favorite set of units to get length.
            return arc*6373
          # else:
          #    return -1
       else:
        return -1

    def do_similarity_metrics(self,records):#40,records40_80,records80):
        # col40_80= ['api_source_api','completeness','elastic_score','elastic_score_percentage','phone_trimmed_field_weighted_score','self_phone_trimmed_field_weighted_score','self_total_field_weighted_score','total_field_weighted_score','phone_equal']
        # col40=    ['Name_m_field_weighted_score', 'aggregated_score' ,'elastic_score','elastic_score_percentage' ,'phone_trimmed_field_weighted_score','qry_details_val' ,'street_field_weighted_score', 'total_field_weighted_score' ,'phone_equal']

        allrecordsPd=pd.DataFrame(records)
        del records


#        dataLess40_i,data40_80_i,dataMore80_i=self.buildVariables(allrecordsPd)
        #del allrecordsPd
        # myDataTestI=allrecordsPd[['Target','Name_m_field_weighted_score','aggregated_score', 'api_source', 'api_source_api',  'completeness', 'completeness_tpi', 'elastic_score', 'elastic_score_percentage', 'geo_distance_meters',  'phone_trimmed_field_weighted_score',  'qry_details_val', 'self_Name_m_field_weighted_score', 'self_phone_trimmed_field_weighted_score',  'self_street_field_weighted_score', 'self_total_field_weighted_score', 'street_field_weighted_score',  'total_field_weighted_score','zip_equal', 'streetLength', 'nameLength', 'source_equal', 'phone_equal', 'website_equal']]
        # myDataTest=myDataTestI[['Name_m_field_weighted_score','aggregated_score', 'api_source', 'api_source_api',  'completeness', 'completeness_tpi', 'elastic_score', 'elastic_score_percentage', 'geo_distance_meters',  'phone_trimmed_field_weighted_score',  'qry_details_val', 'self_Name_m_field_weighted_score', 'self_phone_trimmed_field_weighted_score',  'self_street_field_weighted_score', 'self_total_field_weighted_score', 'street_field_weighted_score',  'total_field_weighted_score','zip_equal', 'streetLength', 'nameLength', 'source_equal', 'phone_equal', 'website_equal']]
        # myDataTest.ix[myDataTest['api_source']=='google','api_source']=1
        # myDataTest.ix[myDataTest['api_source']=='fb','api_source']=0
        # myDataTest.ix[myDataTest['api_source_api']=='google','api_source_api']=1
        # myDataTest.ix[myDataTest['api_source_api']=='fb','api_source_api']=0


        #dataMore80=dataMore80_i

        allrecordsPd['fuzzyMatch_Name']=allrecordsPd.apply(lambda x: fuzz.token_set_ratio(self.strip_accents(x['name']),self.strip_accents(x['name_tpi'])), axis=1)
        #l40_80['fuzzyMatch2']=l40_80.apply(lambda x: fuzz.token_sort_ratio(x['name'],x['name_tpi']), axis=1)
        #l40_80['fuzzyMatch3']=l40_80.apply(lambda x: fuzz.partial_ratio(x['name'],x['name_tpi']), axis=1)

        allrecordsPd['fuzzyMatch_street']=allrecordsPd.apply(lambda x: fuzz.token_set_ratio(self.strip_accents(x['street']),self.strip_accents(x['street_tpi'])), axis=1)
        allrecordsPd['distance']=allrecordsPd.apply(lambda x: self.distance_on_unit_sphere(x['crm_geopoint'], x['crm_geopoint_tpi']), axis=1)

        allrecordsPd['master_street_size']=allrecordsPd.apply(lambda x: len(x['street_tpi']), axis=1)
        allrecordsPd['zip_tpi']=allrecordsPd['zip_tpi'].astype(str)
        allrecordsPd['zip']=allrecordsPd['zip'].astype(str)
        allrecordsPd['same_post_code']=allrecordsPd['zip_tpi'][allrecordsPd['zip_tpi']==allrecordsPd['zip']]
        allrecordsPd['same_phone']=allrecordsPd['phone_trimmed']==allrecordsPd['phone_trimmed_tpi']
        allrecordsPd['same_phone'][allrecordsPd['phone_trimmed']=='Not Found']=False
        allrecordsPd['same_phone'][allrecordsPd['phone_trimmed']=='']=False
        print(allrecordsPd['same_phone'])
        allrecordsPd['CRM_id']=allrecordsPd['CRM_id'].astype(str)

        # print(allrecordsPd['zip_tpi']==allrecordsPd['zip'])
        #l40['fuzzyMatch1']=l40.apply(lambda x: fuzz.token_set_ratio(x['name'],x['name_tpi']), axis=1)
        #l40['fuzzyMatch2']=l40.apply(lambda x: fuzz.token_sort_ratio(x['name'],x['name_tpi']), axis=1)
        #l40['fuzzyMatch3']=l40.apply(lambda x: fuzz.partial_ratio(x['name'],x['name_tpi']), axis=1)

        #l40[(l40['fuzzyMatch1']>89)&(l40['geo_distance_meters']<40)].to_csv("dataLessL40_i.csv")
        #l40_80[(l40_80['fuzzyMatch1']>89)&(l40_80['geo_distance_meters']<40)].to_csv("dataLess40_80_i.csv")
        #dataMore80_i.to_csv("dataMore80_i.csv")
        #dataLess40_i.to_csv("dataLess40_i.csv")
        #data40_80_i.to_csv("data40_80_i.csv")

        #print("AA"+22)
        print(allrecordsPd['zip_tpi'])
        print(allrecordsPd['zip'])
        return allrecordsPd#dataLess40_i[dataLess40_i['Prob']>0.5],data40_80_i[data40_80_i['Prob']>0.4],dataMore80_i,l40_80[(l40_80['fuzzyMatch1']>89)&(l40_80['geo_distance_meters']<40)],l40[(l40['fuzzyMatch1']>89)&(l40_80['geo_distance_meters']<40)]


    def insertionCheck(self,indexC=None,mapC=None,idC=None,pmsi_idC=None,place_idC=None,is_deletedC=None,numbC=""):
        delr=0
        while True:
            delr+=1
            query1 = ESQueryBuilder()
            query1.add_term_query('must', '_id', idC)
            query1.add_term_query('must', 'pmsi_id', str(pmsi_idC))
            query1.add_term_query('must', 'place_id', place_idC)
            # queryi.add_term_query('must', 'details_fetched', 'true')
            query1.add_term_query('must', 'is_deleted', is_deletedC)
            #sort_order = [{"field_name": "place_id", "order": "desc" }]
            #query1.add_sort_order(sort_order)
            try:
                doc_responseCheckD = self.es_client.get_data_for_qry(indexC,mapC, query1.es_query, 1)
            except:
                # del self.es_client
                # self.es_client =  ESWrapper(['84.40.63.148:9200','84.40.63.149:9200'],self.logger)
                print("------error try------",numbC)
                time.sleep(60)
                doc_responseCheckD = self.es_client.get_data_for_qry(indexC,mapC, query1.es_query, 1)
            if len(doc_responseCheckD['source'])>0:
                break
            elif delr>100:
                #print("delr is greater than 100")
                break


    def getEs_Object(self,indexC2=None,mapC2=None,field_idC=None,is_deletedC='false',fieldType=None,numC=""):
        queryi = ESQueryBuilder()
        queryi.add_term_query('must', fieldType, field_idC)
        queryi.add_term_query('must', 'is_deleted', is_deletedC)

        try:
            doc_responseF = self.es_client.get_data_for_qry(indexC2,mapC2, queryi.es_query, 1)
        except:
            print("------error try------",numC)
            time.sleep(60)
            # del self.es_client
            # self.es_client = ESWrapper(['84.40.63.148:9200','84.40.63.149:9200'],self.logger)
            doc_responseF = self.es_client.get_data_for_qry(indexC2,mapC2, queryi.es_query, 1)
        return doc_responseF

    def get_matches(self,dataF):
        dataF=dataF[dataF["distance"]<8]
        print(dataF)
        print(dataF.shape)
        goog_stuf_flag = True
        for ii in range(10):

           ff=dataF[(((dataF["fuzzyMatch_Name"]>(99-ii)) & (dataF["fuzzyMatch_street"]>(99-ii))) & (dataF["master_street_size"]>2))]
           if ff.shape[0]>0:
                ff=ff.sort(['fuzzyMatch_Name', 'fuzzyMatch_street'], ascending=[0, 0])
                bff=ff#.groupby('_id_tpi').first()
                matched_ids=set(list(bff["_id_tpi"]))
                #ff=ff.drop(ff['place_id'] in matched_ids)
                dataF = dataF.loc[~dataF['_id_tpi'].isin(matched_ids)]

           if goog_stuf_flag and ff.shape[0]>0:
               goog_stuf=bff
               goog_stuf_flag=False
           elif ff.shape[0]>0:
               goog_stuf=pd.concat([goog_stuf,bff])


        for ii in range(10):
            dataF2=dataF[(dataF["distance"]<ii/20) & (dataF["distance"]>-1)]

            for ii2 in range(10):
               ff=dataF2[(dataF2["fuzzyMatch_Name"]>(99-ii2))]
               if ff.shape[0]>0:
                    ff=ff.sort(['fuzzyMatch_Name', 'fuzzyMatch_street'], ascending=[0, 0])
                    bff=ff#.groupby('_id').first()
                    matched_ids=set(list(bff["_id_tpi"]))
                    #ff=ff.drop(ff['place_id'] in matched_ids)
                    dataF = dataF.loc[~dataF['_id_tpi'].isin(matched_ids)]
                    dataF2 = dataF2.loc[~dataF2['_id_tpi'].isin(matched_ids)]

               if goog_stuf_flag and ff.shape[0]>0:
                   goog_stuf=bff
                   goog_stuf_flag=False
               elif ff.shape[0]>0:
                   goog_stuf=pd.concat([goog_stuf,bff])

        dataF2=dataF[dataF["distance"]<1]
        for ii in range(15):

           ff=dataF2[(dataF2["fuzzyMatch_Name"]>(89-ii)) & (dataF2["same_phone"])]
           if ff.shape[0]>0:
                ff=ff.sort(['fuzzyMatch_Name', 'fuzzyMatch_street'], ascending=[0, 0])
                bff=ff#.groupby('_id').first()
                matched_ids=set(list(bff["_id_tpi"]))
                #ff=ff.drop(ff['place_id'] in matched_ids)
                dataF = dataF.loc[~dataF['_id_tpi'].isin(matched_ids)]
                dataF2 = dataF2.loc[~dataF2['_id_tpi'].isin(matched_ids)]

           if goog_stuf_flag and ff.shape[0]>0:
               goog_stuf=bff
               goog_stuf_flag=False
           elif ff.shape[0]>0:
               goog_stuf=pd.concat([goog_stuf,bff])

        dataF2=dataF.copy()#[dataF["distance"]<1]
        for ii in range(15):

           ff=dataF2[((dataF2["fuzzyMatch_Name"]>(89-ii))& (dataF["fuzzyMatch_street"]>(96)))]
           if ff.shape[0]>0:
                ff=ff.sort(['fuzzyMatch_Name', 'fuzzyMatch_street'], ascending=[0, 0])
                bff=ff#.groupby('_id').first()
                matched_ids=set(list(bff["_id_tpi"]))
                #ff=ff.drop(ff['place_id'] in matched_ids)
                dataF = dataF.loc[~dataF['_id_tpi'].isin(matched_ids)]
                dataF2 = dataF2.loc[~dataF2['_id_tpi'].isin(matched_ids)]

           if goog_stuf_flag and ff.shape[0]>0:
               goog_stuf=bff
               goog_stuf_flag=False
           elif ff.shape[0]>0:
               goog_stuf=pd.concat([goog_stuf,bff])

        goog_stuf.to_csv("goog_stuf.csv")
        return goog_stuf


    def build_matched_record(self,crm, master,geopointFlag=True):
        finalRecord={}
        finalRecord["_id"]=crm["unique_id"]
        finalRecord["CRM_id"]=crm["CRM_id"]
        finalRecord["_id_tpi"]=master["pmsi_id"]
        #finalRecord["city"]=crm["city"]
        #finalRecord["city_tpi"]=master["city"]
        if geopointFlag:
            finalRecord["crm_geopoint"]=crm["geopoint2"]
        finalRecord["crm_geopoint_tpi"]=master["geopoint"]
        finalRecord["name"]=crm["Name"]
        finalRecord["name_tpi"]=master["Name_m"]
        #if "Phone" in crm:
        #    finalRecord["phone_trimmed"]=crm["Phone"]
        #finalRecord["phone_trimmed_tpi"]=master["phone_trimmed"]
        finalRecord["street"]=crm["Address"]
        finalRecord["street_tpi"]=master["street"]
        #finalRecord["zip"]=crm["post_code"]
        #finalRecord["zip_tpi"]=master["zip"]
        return finalRecord



    def search_all_matches(self,crm_data,master_data,matched_ids):
      finalMatches=[]
      for mm in master_data:
        if mm["pmsi_id"] not in matched_ids:
         # for cc in crm_data:
          #print(cc["unique_id"])
          #print(matched_ids)
          possible_matches=[]
          # if cc["unique_id"] not in matched_ids:
          for cc in crm_data:

           if 'geopoint2' in cc:
              #print("-----------------------------")
                 print(cc)
            #for mm in master_data:
              #if mm["pmsi_id"] not in matched_ids:
                 tREc=""
                 p_dist=self.distance_on_unit_sphere(cc['geopoint2'], mm['geopoint'])
                 if tREc=="" and p_dist < 0.01 and p_dist > -1:
                     dist_names=fuzz.token_set_ratio(self.strip_accents(cc['Name']),self.strip_accents(mm['Name_m']))
                     if dist_names > 80:#
                        dist_streets=fuzz.token_set_ratio(self.strip_accents(cc['Address']),self.strip_accents(mm['street']))
                        if dist_streets > 80:#
                            tREc=self.build_matched_record(cc ,mm)
                            tREc['distance']=p_dist
                            tREc['fuzzyMatch_Name']=dist_names
                            tREc['fuzzyMatch_street']=dist_streets
                            tREc['master_street_size']=len(mm['street'])
                            possible_matches.append(tREc)
                 if tREc=="" and p_dist < 0.1 and p_dist > -1:
                     dist_names=fuzz.token_set_ratio(self.strip_accents(cc['Name']),self.strip_accents(mm['Name_m']))
                     if dist_names > 90:#
                        dist_streets=fuzz.token_set_ratio(self.strip_accents(cc['Address']),self.strip_accents(mm['street']))
                        if dist_streets > 90:#
                            tREc=self.build_matched_record(cc ,mm)
                            tREc['distance']=p_dist
                            tREc['fuzzyMatch_Name']=dist_names
                            tREc['fuzzyMatch_street']=dist_streets
                            tREc['master_street_size']=len(mm['street'])
                            possible_matches.append(tREc)
                 if tREc=="" and p_dist < 0.1 and p_dist > -1:
                     dist_names=fuzz.token_set_ratio(self.strip_accents(cc['Name']),self.strip_accents(mm['Name_m']))
                     if dist_names > 75:#
                        dist_streets=fuzz.token_set_ratio(self.strip_accents(cc['Address']),self.strip_accents(mm['street']))
                        if dist_streets > 96:#
                            tREc=self.build_matched_record(cc ,mm)
                            tREc['distance']=p_dist
                            tREc['fuzzyMatch_Name']=dist_names
                            tREc['fuzzyMatch_street']=dist_streets
                            tREc['master_street_size']=len(mm['street'])
                            possible_matches.append(tREc)
                 if tREc=="" and p_dist < 1 :
                     dist_names=fuzz.token_set_ratio(self.strip_accents(cc['Name']),self.strip_accents(mm['Name_m']))
                     if dist_names > 80 and len(mm['street'])>2:#
                        dist_streets=fuzz.token_set_ratio(self.strip_accents(cc['Address']),self.strip_accents(mm['street']))
                        if dist_streets > 95:#
                            tREc=self.build_matched_record(cc ,mm)
                            tREc['distance']=p_dist
                            tREc['fuzzyMatch_Name']=dist_names
                            tREc['fuzzyMatch_street']=dist_streets
                            tREc['master_street_size']=len(mm['street'])
                            possible_matches.append(tREc)
                 if tREc==""  and  p_dist < 0.01:
                     dist_names=fuzz.token_set_ratio(self.strip_accents(cc['Name']),self.strip_accents(mm['Name_m']))
                     if dist_names > 50 and len(mm['street'])<2:#
                        #dist_streets=fuzz.token_set_ratio(self.strip_accents(cc['Address']),self.strip_accents(mm['street']))
                        #if dist_streets > 80:#
                            tREc=self.build_matched_record(cc ,mm)
                            tREc['distance']=p_dist
                            tREc['fuzzyMatch_Name']=dist_names
                            #tREc['fuzzyMatch_street']=dist_streets
                            tREc['master_street_size']=len(mm['street'])
                            possible_matches.append(tREc)

              # if len(possible_matches)>0:
              #   finalMatches.extend(possible_matches)
              #   possible_matches=[]
                #print(possible_matches)
                     # ff=dataF[(((dataF["fuzzyMatch_Name"]>(99-ii)) & (dataF["fuzzyMatch_street"]>(99-ii))) & (dataF["master_street_size"]>2))]
                     #print(p_dist)
           else:
              #for mm in master_data:
                 #p_dist=self.distance_on_unit_sphere(cc['geopoint2'], mm['geopoint'])
                 if True:#p_dist < 0.5:
                     dist_names=fuzz.token_set_ratio(self.strip_accents(cc['Name']),self.strip_accents(mm['Name_m']))
                     if dist_names > 95:#
                        dist_streets=fuzz.token_set_ratio(self.strip_accents(cc['Address']),self.strip_accents(mm['street']))
                        if dist_streets > 95 and len(mm['street'])>2:#
                            tREc=self.build_matched_record(cc ,mm,False)
                            #tREc['distance']=p_dist
                            tREc['fuzzyMatch_Name']=dist_names
                            tREc['fuzzyMatch_street']=dist_streets
                            tREc['master_street_size']=len(mm['street'])
                            possible_matches.append(tREc)
                     elif tREc=="" and dist_names > 85:#
                        dist_streets=fuzz.token_set_ratio(self.strip_accents(cc['Address']),self.strip_accents(mm['street']))
                        if dist_streets > 70 and len(mm['street'])>2:#
                            #print("22"+22)
                            print("------here-------------")
                            print(cc)
                            print(mm)
                            if 'phone_t' in cc and 'phone_t' in mm:
                              print("-----yaaahhhhhhjh---------------")
                              if cc['phone_t'] != 'Not Found' and cc['phone_t']==mm['phone_t']:

                                tREc=self.build_matched_record(cc ,mm,False)
                                #tREc['distance']=p_dist
                                tREc['fuzzyMatch_Name']=dist_names
                                tREc['fuzzyMatch_street']=dist_streets
                                tREc['master_street_size']=len(mm['street'])
                                possible_matches.append(tREc)

        # for ii in range(15):
        #
        #    ff=dataF2[(dataF2["fuzzyMatch_Name"]>(89-ii)) & (dataF2["same_phone"])]
        #    if ff.shape[0]>0:
        #         ff=ff.sort(['fuzzyMatch_Name', 'fuzzyMatch_street'], ascending=[0, 0])
        #         bff=ff#.groupby('_id').first()
        #         matched_ids=set(list(bff["_id_tpi"]))
        #         #ff=ff.drop(ff['place_id'] in matched_ids)
        #         dataF = dataF.loc[~dataF['_id_tpi'].isin(matched_ids)]
        #         dataF2 = dataF2.loc[~dataF2['_id_tpi'].isin(matched_ids)]
        #
        #    if goog_stuf_flag and ff.shape[0]>0:
        #        goog_stuf=bff
        #        goog_stuf_flag=False
        #    elif ff.shape[0]>0:
        #        goog_stuf=pd.concat([goog_stuf,bff])




          if len(possible_matches)>0:
              print("-----some results----------")
              finalMatches.extend(possible_matches)
              #possible_matches=[]
            #print(possible_matches)
        return finalMatches
#######################################
    def do_matching_modeling(self,m_records,configs,indexMatch=None,mapMatch=None):
        if indexMatch == None:
            indexMatch=self.masterRefindexMatch
        if mapMatch == None:
            mapMatch=self.masterRefmapMatch


        #response = self.m.elastic.get_data_for_qry(indexMatch, mapMatch, query.es_query)

            #basePmsiId=currentPmsi -1
        # elif len(response['source'])>0 and 'pmsi_id' in response['source'][0]:
        #     currentPmsi= response['source'][0]['pmsi_id']+1
        #     basePmsiId=currentPmsi -1
        # elif  len(response['source'])>0:
        #     print("this can be a problem - record without pmsi_id")
        # else:
        #     print("this can be a problem - matchingDatabase is empty")
        allMatchest=[]
        allrecords=[]
        unmatchOutlets={}
        asd=0
        print("Start matching")
        for record in m_records:#[60000:]:
             asd+=1
             if asd%100==0:
                print("progress bar - matching",asd)
                #if asd== 2000:
                 #    break

             # queryM = ESQueryBuilder()
             # queryM.add_term_query('must', 'place_id', record['src_hit']['_source']['place_id'])
             # # queryM.add_term_query('must', 'details_fetched', 'true')
             # queryM.add_term_query('must', 'is_deleted', 'false')
             # #sort_order = [{"field_name": "place_id", "order": "desc" }]
             # #queryM.add_sort_order(sort_order)
             # doc_responseM = self.m.elastic.get_data_for_qry(indexMatch,mapMatch, queryM.es_query, 1)

             # if len(doc_responseM["source"])==0:
             if True:#len(doc_responseM["source"])==0:

                m_response_i = self.m.get_matched_response(record)

                if len(m_response_i)==0:
                    time.sleep(60)
                    m_response_i = self.m.get_matched_response(record)
                    print("----response is null ---------------------------")
                    #print(m_response_i)
                    #print(record)
                #unmatchOutlets[m_response_i[0]['place_id']]=[]
                if m_response_i[0]['status'] == 'matched':
                    #unmatchOutlets[m_response_i[0]['place_id']]=[]
                #else:
                    #for jj in m_response_i:
                    #print("extend - ",str(len(m_response_i)))
                    tt=[]
                    print("-------------------------------")
                    print(len(m_response_i))
                    for hh in m_response_i:
                        hh['fuzzyMatch_street']=fuzz.token_set_ratio(self.strip_accents(hh['street']),self.strip_accents(hh['street_tpi']))
                        hh['fuzzyMatch_Name']=fuzz.token_set_ratio(self.strip_accents(hh['name']),self.strip_accents(hh['name_tpi']))
                        if (hh['fuzzyMatch_Name'] >50 and hh['fuzzyMatch_street'] >10) or (hh['fuzzyMatch_Name'] >50 and len(hh['street_tpi']) >0):
                           tt.append(hh)
                    # allrecords.extend(m_response_i)
                    print(len(tt))
                    allrecords.extend(tt)
                    #   allrecords.append({k:jj[k] for k in ['_id', '_id_tpi', 'place_id', 'place_id_tpi','api_source','name','name_tpi','street','status','phone_trimmed','phone_trimmed_tpi','api_source_api','completeness','elastic_score','phone_trimmed_field_weighted_score','self_phone_trimmed_field_weighted_score','self_total_field_weighted_score','phone_equal','Name_m_field_weighted_score','aggregated_score','elastic_score_percentage', 'qry_details_val', 'street_field_weighted_score', 'total_field_weighted_score','geo_distance_meters'] if k in jj})
                        #allrecords.extend(jj[ ['api_source_api','completeness','elastic_score','elastic_score_percentage','phone_trimmed_field_weighted_score','self_phone_trimmed_field_weighted_score','self_total_field_weighted_score','total_field_weighted_score','phone_equal','Name_m_field_weighted_score','aggregated_score','elastic_score_percentage', 'qry_details_val', 'street_field_weighted_score', 'total_field_weighted_score'] ])


                    #allMatchest.extend(m_response_i)
                #else:
                    #allMatchest.extend(m_response_i)
             #else:

                 #print("somethingWrong?")
        del m_records

        #print(len(allrecords))
        #print(len(unmatchOutlets))
        # allMAtchestPd=pd.DataFrame(allMatchest)
        allMAtchestPd=pd.DataFrame(allrecords)
        # allMAtchestPd.to_csv("AllMatchest.csv")
        allMAtchestPd.to_csv("AllMatchest4.csv")
        #del allMAtchestPd
        #del allMatchest
        resultMatches=self.do_similarity_metrics(allrecords) # maybe reduce the columns of the dataframes to only the necessary ones
        del allrecords
        resultMatches.to_csv("AllMatchest_with_metrics4.csv")
        goodmatches=self.get_matches(resultMatches)
        print(goodmatches)
        #matched_ids=list(goodmatches["_id_tpi"])
        #crm_data=self.get_crm_data(configs)
        #master_data=self.get_master_data(configs)
        #new_matches=self.search_all_matches(crm_data,master_data,matched_ids)
        # new_matches=self.search_all_matches(crm_data,master_data,[])
        #goodmatchesFinal=pd.concat([goodmatches,pd.DataFrame(new_matches)])
        #matched_ids=goodmatchesFinal
        matched_ids=list(goodmatches["_id_tpi"])
        otherPossibleMatches = resultMatches.loc[~resultMatches['_id_tpi'].isin(matched_ids)]
        otherPossibleMatchesMain=otherPossibleMatches[((otherPossibleMatches["fuzzyMatch_Name"]>50)) & (otherPossibleMatches["fuzzyMatch_street"]>50)]
        otherPossibleMatchesMain2=otherPossibleMatches[((otherPossibleMatches["fuzzyMatch_Name"]>60)) & (otherPossibleMatches["master_street_size"]<2)]
        otherPossibleMatchesMain=pd.concat([otherPossibleMatchesMain,otherPossibleMatchesMain2])
        otherPossibleMatchesOther=otherPossibleMatches[((otherPossibleMatches["fuzzyMatch_Name"]<=50)) & (otherPossibleMatches["fuzzyMatch_street"]<=50)]
        otherPossibleMatchesMain.to_csv("OtherPossibleMatches_main_Final.csv")
        otherPossibleMatchesOther.to_csv("OtherPossibleMatches_other_Final.csv")
#        matches=self.get_matches(resultMatches)
#        matchDict={}
        print("model done")
        goodmatches.to_csv("AllMatchest_Final.csv")

        #print(less40_80)
        fco=0
        #print(22+"22")
        #for outletDF in [modelM80.sort('Prob',ascending=False).to_dict("records"),model40_80.sort('Prob',ascending=False).to_dict("records"),modelL40.sort('Prob',ascending=False).to_dict("records"),less40_80.sort('Prob',ascending=False).to_dict("records"),lessL_40.sort('Prob',ascending=False).to_dict("records")]:
          #for outletE in outletDF:
    #
    # def bubble_sort(self,items):
    #     """ Implementation of bubble sort """
    #     for i in range(len(items)):
    #         for j in range(len(items)-1-i):
    #             if items[j]['elastic_score_percentage'] < items[j+1]['elastic_score_percentage']:
    #                 items[j], items[j+1] = items[j+1], items[j]
    #     return items


    def get_master_record(self,scrape_id):
        # build the query
        index = 'horeca_raw'
        mapping = 'horeca_raw'
        qry = ESQueryBuilder()
        qry.add_term_query('must', 'scrape_id', str(scrape_id))
        # qry.add_term_query('must', 'country_combined_', country)
        # qry.add_term_query('must', 'outlet_classify', 'false')
        qry.add_term_query('must', 'outlet_classify', 'true')
        qry.add_term_query('must', 'details_fetched', 'true')

        #Hack to speed up the bits I need - Sorry Rodrigo!!
        #qry.add_term_query('must', 'pmsi_type_simple', 'Nightclub')
        #qry.add_term_query('must', 'pmsi_type_simple', 'Hotel')

        qry.add_term_query('must', 'scrape_id', str(scrape_id))
        qry.add_sort_order([{ "field_name": "place_id", "order": "asc" }])
        # get doc counts for query
        count = self.es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
        # query
        qry.add_size(count)

        results = self.es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
        data = results['source']
        return data

    def matchingMachine(self,config_file_path=None):

        if config_file_path == None:
            config_file_path=self.config_file_path

        configs = self.load_config(config_file_path)
        #configs = self.load_config(os.path.join(os.path.dirname(os.getcwd()),'GitHub','horeca_matching',config_file_path))

        if configs is not None:
            for m_config in configs:
                if not m_config['active']:
                    self.m.logger.info('Not active -> %s', m_config['id'])
                    continue

                self.m.logger.info('Working on -> %s', m_config['id'])
                same_source = False
                if 'same_source' not in m_config:
                    m_config['same_source'] = same_source
                results_mapping_name = str(m_config['id']).replace(' ', '').strip().lower()
                match_run_id = m_config['match_run_id']

                if match_run_id is None:
                    match_run_id = 6 ## set this from scrapeid
                    m_config['match_run_id'] = match_run_id
                    if not m_config['do_match']:
                        match_run_id -= 1
                    elif m_config['do_match']:



                            # print("22"+22)
                            records = self.get_records_to_match(m_config, results_mapping_name)

                            #print("22"+22)
                            print("start matching")
                            aa=0
                            while aa < len(records):
                                print("Current bulk ---- ",aa, " ( of",len(records),")")
                                list_matched=self.do_matching_modeling(records[aa:aa+5000],m_config)

                                aa+=5000
                            print("is finished")




matchW=matcherWorker()
matchW.matchingMachine(config_file_path='configuration.json')
# matchW.matchingMachine(scrape_id=[126],redoMatch=True,config_file_path='C:\\Users\\rfernandes\\Documents\\Rfernandes\\horeca_matching\\configuration.json')
# matchW.matchingMachine(scrape_id=[101],redoMatch=True,config_file_path='C:\\Users\\rfernandes\\Documents\\Rfernandes\\horeca_matching\\configuration.json')
# matchW.matchingMachine(scrape_id=[125],redoMatch=True,config_file_path='C:\\Users\\rfernandes\\Documents\\Rfernandes\\horeca_matching\\configuration.json')
# matchW.matchingMachine(scrape_id=[112],redoMatch=True,config_file_path='C:\\Users\\rfernandes\\Documents\\Rfernandes\\horeca_matching\\configuration.json')
# matchW.matchingMachine(scrape_id=[116],redoMatch=True,config_file_path='C:\\Users\\rfernandes\\Documents\\Rfernandes\\horeca_matching\\configuration.json')
# matchW.matchingMachine(scrape_id=[127],redoMatch=True,config_file_path='C:\\Users\\rfernandes\\Documents\\Rfernandes\\horeca_matching\\configuration.json')

#matchW.matchingMachine(scrape_id=[100],redoMatch=False)
#matchW.makeMatchingChanges(changeM={"type":"place_id","objects":["ChIJB9EV6n80GQ0RY1ueMKZ9jaw","117823401633061"]},changesFile="modifications.txt")


