__author__ = 'rfernandes'

import logging
from pylastic import ESWrapper, ESQueryBuilder
from utility import helper as um
import pandas as pd
import numpy as np
logger = logging.getLogger('TTT')
um.setup_logger('c:\\Temp\\', 'TTT', logger)
logger.info('start')
import time

## create the connection:

es_client = ESWrapper(['es01.online-pmsi.com:9200'], logger)




# index = 'crm_matching'
# index = 'master_matching_reference'
index = 'horeca_report'
# index = 'test_horeca_master'
# mapping = 'crm_matching2'
# mapping = 'master_matching_reference'
mapping = 'horeca_report'
# mapping = 'test_horeca_master'

print("aa")

#es_client.es_client.delete_by_query()


def get_master_record_version(country,version):
    # build the query
    qry = ESQueryBuilder()
    qry.add_term_query('must', 'Country', country)
    #qry.add_term_query('must', 'country', country)
    qry.add_term_query('must', 'Version', version)
    #qry.add_term_query('must', 'is_deleted', 'false')
    qry.add_term_query('must_not', 'Outlet_state', 'closed')
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

def get_doc_by_id(base_id,version):
    qry = ESQueryBuilder()
    #qry.add_term_query("must", "base_id", id, exclude_words=[])
    #qry.add_qry_string_query("must", "base_id", id, "AND", [])
    qry.add_term_query('must', 'Outlet_id', base_id)
    qry.add_term_query('must', 'Version', str(version))
    #qry.add_term_query('must', 'is_deleted', 'false')
    try:
        results = es_client.get_data_for_qry(index, mapping, qry.es_query)
    except:
        time.sleep(60)
        results = es_client.get_data_for_qry(index, mapping, qry.es_query)
    results = results['source']
    return results

countries_version=['Greece']
# countries_version=['Sweden']
p_version='0'
c_version='1'

for ct in countries_version:#add the new features
    dd0=get_master_record_version(ct,p_version)
    dd1=get_master_record_version(ct,c_version)

    dd0df=pd.DataFrame(dd0)
    #dd1df=pd.DataFrame(dd1)

    # id0=set(dd0df['Outlet_id'].tolist())
    # id1=set(dd1df['Outlet_id'].tolist())
    # it01=id0.intersection(id1)
    # new01=
    #print(dd1)
    for oo in dd1:
        #print(oo['pmsi_social_score'])
        if oo['Outlet_state']=='existing':
           ww=dd0df[dd0df['Outlet_id']==oo['Outlet_id']]
           if ww.shape[0] >0:

            print(oo['Social_score'])
            print(ww['Social_score'])
            print(ww.shape)
            oo['social_score_change']=float(oo['Social_score'])-float(ww['Social_score'])
            if 'FB_checkins' in oo:#
                print(ww['FB_checkins'])
                print(float(ww['FB_checkins']))
                if pd.isnull(float(ww['FB_checkins'])):
                    fb_checkins=0
                else:
                    fb_checkins=float(ww['FB_checkins'])
                oo['fb_checkins_change']=oo['FB_checkins']-fb_checkins
                print("---------------------------------")
                print(oo['fb_checkins_change'])
                print(float(ww['FB_checkins']))
                print(fb_checkins)
                print(oo['fb_checkins_change'])
            else:
                oo['fb_checkins_change']=0

            #
            # print("---------------------------------")
            # print(oo['pmsi_social_score'])
            # print(float(ww['pmsi_social_score']))
            # print(oo['social_score_change'])

            #print(ww.shape)
           else:
               oo['fb_checkins_change']=oo['FB_checkins']
               oo['social_score_change']=oo['Social_score']
        elif oo['Outlet_state']=='new':
            oo['social_score_change']=oo['Social_score']
            if 'FB_checkins' in oo:#
                #print(ww['FB_checkins'])
                #print(float(ww['FB_checkins']))
                oo['fb_checkins_change']=oo['FB_checkins']
                # print("---------------------------------")
                # print(oo['fb_checkins_change'])
                # print(float(ww['fb_checkins']))
                # print(fb_checkins)
                # print(oo['fb_checkins_change'])
            else:
                oo['fb_checkins_change']=0

        else:
           oo['social_score_change']=0

    results_df_nested=pd.DataFrame(dd1)
    results_df_nested['sc_r']=np.random.uniform(0.001, 0.099, len(results_df_nested))
    results_df_nested['social_score_change_r']=results_df_nested['social_score_change']+results_df_nested['sc_r']
    results_df_nested['fb_checkins_change_r']=results_df_nested['fb_checkins_change']+results_df_nested['sc_r']

    results_df_nested['social_score_change_rank']=pd.qcut(results_df_nested['social_score_change_r'],5, labels=["social_change_0","social_change_1","social_change_2","social_change_3","social_change_4"])
    results_df_nested['fb_checkins_change_rank']=pd.qcut(results_df_nested['fb_checkins_change_r'],5, labels=["fb_checkins_change_0","fb_checkins_change_1","fb_checkins_change_2","fb_checkins_change_3","fb_checkins_change_4"])
    #results_df_nested['neighbourwood_rank']=results_df_nested['neighbourwood_rank_']
    results_df_nested['top_change']='no'
    results_df_nested['top_social_score_change']='no'
    results_df_nested['top_fb_checkins_change']='no'
    rn_A_social_score=results_df_nested.sort(['social_score_change_r'], ascending=False)
    print("-------------------------")
    print(rn_A_social_score)
    rn_A_social_score.iloc[:10]['top_change']='top'
    rn_A_social_score.iloc[:10]['top_social_score_change']='top_10'
    rn_A_social_score.iloc[-10:]['top_change']='bottom'
    rn_A_social_score.iloc[-10:]['top_social_score_change']='bottom_10'
    #rn_a_social_score=results_df_nested.sort(['social_score_change_r'], ascending=True)
    rn_A_social_score=rn_A_social_score.sort(['fb_checkins_change_r'], ascending=False)
    rn_A_social_score.iloc[:10]['top_change']='top'
    rn_A_social_score.iloc[:10]['top_fb_checkins_change']='top_10'
    rn_A_social_score.iloc[-10:]['top_change']='bottom'
    rn_A_social_score.iloc[-10:]['top_fb_checkins_change']='bottom_10'
    #rn_a_fb_checkins=results_df_nested.sort(['fb_checkins_change_r'], ascending=True)
    data = rn_A_social_score[["Outlet_id","Unique_id","top_change","top_social_score_change","top_fb_checkins_change","social_score_change_rank","fb_checkins_change_rank","social_score_change","fb_checkins_change"]].to_dict(outtype='records')
    allrecordsC=[]
    for t in range(0, len(data)):
      #if t> 40000:
       flags = data[t]
       print(flags)
       idd=flags['Unique_id']
       # if c_version == int(0):
       #      idd=flags['base_id']
       # else:
       #      idd=flags['base_id']+'_'+str(c_version)
       base_id = flags['Outlet_id']
       #pmsi_id_version = flags['pmsi_id_version']
       #flags['Unique_id']=idd
       print(base_id)
       #print(version)

       base_data = get_doc_by_id(base_id,c_version)[0]
       base_data.update(flags)
       print(base_data)
       allrecordsC.append(base_data)
       # print("aa")
       #es_client.index_doc_with_id(base_data, index, mapping, idd)
    es_client.bulk_index(es_client.es_client, index, mapping, allrecordsC, "Unique_id")




