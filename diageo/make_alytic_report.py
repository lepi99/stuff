__author__ = 'rfernandes'

import logging
from pylastic import ESWrapper, ESQueryBuilder
from utility import helper as um
import pandas as pd

logger = logging.getLogger('TTT')
um.setup_logger('c:\\Temp\\', 'TTT', logger)
logger.info('start')

## create the connection:

es_client = ESWrapper(['es01.online-pmsi.com:9200'], logger)




# index = 'crm_matching'
# index = 'master_matching_reference'
index = 'horeca_master'
# index = 'test_horeca_master'
# mapping = 'crm_matching2'
# mapping = 'master_matching_reference'
mapping = 'horeca_master'
# mapping = 'test_horeca_master'

print("aa")

#es_client.es_client.delete_by_query()

def get_master_record(country):
    # build the query
    qry = ESQueryBuilder()
    qry.add_term_query('must', 'country_combined_', country)
    #qry.add_term_query('must', 'country', country)
    # qry.add_term_query('must', 'version', version)
    qry.add_term_query('must', 'is_deleted', 'false')
    # qry.add_term_query('must', 'country_combined_', country)
    #qry.add_term_query('must', 'is_deleted', 'false')
    ##qry.add_term_query('must', 'api_source', 'google')
    #qry.add_geo_bounding_box_filter("must", "geopoint",  23.494, 46.795, 23.695, 46.745)
    #qry.add_missing_field_filter("must_not", "existing_outlet")
    #qry.add_missing_field_filter('must_not', 'existing_outlet_id')
    #qry.add_missing_field_filter('must', 'outlet_owner')
    # qry.add_sort_order([{ "field_name": "pmsi_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    # query
    qry.add_size(count)

    results = es_client.get_data_for_qry(index, mapping, qry.es_query, page_size = 5000)
    data = results['source']
    return data


countries_version=['Sweden','Greece','Portugal']
# countries_version=[['Sweden','1'],['Greece','1'],['Portugal','1']]

for ct in countries_version:#add the new features
    dd=get_master_record(ct)
    allrec=[]
    for rr in dd:
        #print(rr)
        newRec={}
        newRec['Created_on']=rr['created_on'] #check this
        newRec['Unique_id']=rr['pmsi_id_version']
        newRec['Outlet_id']=rr['base_id']
        newRec['Name']=rr['Name_m']
        newRec['Country']=rr['country_combined_']
        newRec['Outlet_state']=rr['outlet_state_indicator']

        newRec['Existing_outlet']='false'
        if 'existing_outlet' in rr:
            #print(rr)
            # if len(str(rr['existing_outlet_id']).strip())> 0:
            if rr['existing_outlet'] == True:
               #print("yes")
               #print(rr)
               #print(newRec)
               newRec['Existing_outlet']=rr['existing_outlet']
               newRec['Existing_outlet_id']=rr['existing_outlet_id']

        #newRec['City']=rr['city']
        newRec['Geopoint']=rr['geopoint']
        if 'zip' in rr:
            newRec['Zip']=rr['zip']

        newRec['Version']=rr['version']
        newRec['Type']=rr['pmsi_type_simple']
        if 'pmsi_subtype_simple' in  rr:
            newRec['Subtype']=rr['pmsi_subtype_simple']
        else:
            newRec['Subtype']=rr['pmsi_type_simple']

        if True:#rr['outlet_state_indicator'] != 'closed':
            if 'pmsi_keywords' in rr:
                newRec['Keywords']=rr['pmsi_keywords']
            # else:
            #     newRec['Keywords']=[]
            if 'pmsi_keywords_phrases' in rr:
                newRec['Key_phrases']=rr['pmsi_keywords_phrases']

            if 'pmsi_priority_' in rr:
                newRec['Priority']=rr['pmsi_priority_']

            if 'neighbourwood_rank_' in  rr:
                newRec['Urban_rural']=rr['neighbourwood_rank_']

            if 'neighbourwood' in  rr:
                newRec['Urban_rural_score']=rr['neighbourwood']

            if 'name_chain' in  rr:
                newRec['Name_chain']=rr['name_chain']
            newRec['is_chain']='false'
            if 'is_chain' in  rr:
                newRec['is_chain']=rr['is_chain']

            if 'fb_likes' in rr or 'fb_reviews' in rr or 'fb_checkins' in rr:
                #fb_history={}
                #newRec['time_stamp']=rr['created_on']
                if 'fb_likes' in rr:
                    newRec['FB_likes']=rr['fb_likes']
                if 'fb_reviews' in rr:
                    newRec['FB_reviews']=rr['fb_reviews']
                if 'fb_checkins' in rr:
                    newRec['FB_checkins']=rr['fb_checkins']
                #outletD['social_history']['fb_history'].append(fb_history)

            #outletD['social_history']['google_history']=[]
            #if 'goog_rating' in rr or 'goog_reviews' in rr or 'goog_views' in rr:
                #google_history={}
                #outletD['social_history']['google_history']={}
                #google_history['time_stamp']=rr['created_on']
            if 'goog_rating' in rr:
                    newRec['Google_rating']=rr['goog_rating']
            if 'goog_reviews' in rr:
                    newRec['Google_reviews']=rr['goog_reviews']
            if 'goog_views' in rr:
                    newRec['Google_views']=rr['goog_views']
                #rr['social_history']['google_history'].append(google_history)

            #rr['social_history']['pmsi_social_score']=[]
            if 'pmsi_social_rank_' in rr:
                #social_score_history={}
                #outletD['social_history']['social_score_history']={}
                #social_score_history['time_stamp']=rr['created_on']
                newRec['Social_rank']=rr['pmsi_social_rank_']
                #outletD['social_history']['pmsi_social_score'].append(social_score_history)
            if 'pmsi_social_score' in rr:
                #social_score_history={}
                #outletD['social_history']['social_score_history']={}
                #social_score_history['time_stamp']=rr['created_on']
                newRec['Social_score']=rr['pmsi_social_score']
            #outletD['social_history']['Premiumness']=[]
            if 'Premiumness' in rr:
                #Premiumness={}
                #outletD['social_history']['social_score_history']={}
                #social_score_history['time_stamp']=rr['created_on']
                newRec['Premiumness']=rr['Premiumness']
                #outletD['social_history']['Premiumness'].append(Premiumness)

            #outletD['social_history']['Business_Activeness']=[]
            if 'Business_Activeness' in rr:
                #Business_Activeness={}
                #outletD['social_history']['social_score_history']={}
                #Business_Activeness['time_stamp']=rr['created_on']
                newRec['Business_Activeness']=rr['Business_Activeness']
                #outletD['social_history']['Business_Activeness'].append(Business_Activeness)
        allrec.append(newRec)

    es_client.bulk_index(es_client.es_client, 'horeca_report', 'horeca_report', allrec, 'Unique_id')
        # print(rr['social_history'])
    #ddff=pd.DataFrame(dd)
#ddff[['Name_m','street','city','phone','pmsi_id','zip','geopoint']].to_csv("Bel_data.csv")