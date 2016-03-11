__author__ = 'rfernandes'

import json
import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
from pandas import ExcelWriter
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'ReverseGeocoder', logger)
es_client = ES(['84.40.63.146:9200'], logger)

if True:
    index = 'horeca_master'
    mapping = 'horeca_master'

    qry = ESQueryBuilder()
    #qry.add_missing_field_filter("must", "geopoint")
    #qry.add_term_query('must', 'api_source', 'fb', [])
    #qry.add_term_query('must', 'details_fetched_source', 'set', [])
    #qry.add_term_query('must_not', 'is_community_page', 'true', [])
    qry.add_term_query('must', 'country_combined_', 'Thailand', [])
    qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
    #qry.add_fields(['lng_lat','base_id','Address','Name_m','category','city','country_combined_','fb_checkins','fb_likes','fb_place_id','fb_reviews','fb_visits','fb_website','geopoint','goog_rating','goog_reviews','goog_views','google_website','is_community_page','phone','pmsi_keywords_phrases','pmsi_local_occasion_','pmsi_occasion_','pmsi_priority','pmsi_social_rank','pmsi_social_score','pmsi_subtype','pmsi_subtype_simple','pmsi_type_simple','url','website','were_here_count','zip'])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)

    qry.add_size(count)
    data = es_client.get_data_for_qry(index, mapping, qry.es_query)
    print(json.dumps(qry.es_query))
    print("-------------------------")
    print(data['source'][0])
    dd=pd.DataFrame(data['source'])
    print(dd['geopoint'])
    #dd['geopoint']=dd['geopoint'].str
    #dd['geopoint']=dd['geopoint'].apply(str)
    #dd['Lat']=dd['geopoint'][1]
    #dd['Long']=dd['geopoint'][0]
    #foo = lambda x: pd.Series([i for i in reversed(x.split(','))])
    #rev = dd['geopoint'].apply(foo)
    for ccl in list(dd.columns.values):
        dd[ccl]=dd[ccl].apply(str)
    print(dd)
    writer = ExcelWriter('ThaiFile_All_horecaaa.xlsx')
    dd.to_excel(writer,'all')
    writer.save()
    dd.to_csv('ThaiFile_All_horecacsvaa.csv')
    print(dd)
    print(dd['geopoint'])
    print(count)

if False:
    index = 'horeca_raw'
    mapping = 'horeca_raw'

    qry = ESQueryBuilder()
    #qry.add_missing_field_filter("must", "geopoint")
    qry.add_term_query('must', 'scrape_id', '121', [])
    #qry.add_term_query('must', 'country', 'Thailand', [])
    qry.add_sort_order([{ "field_name": "place_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    qry.add_size(count)
    data = es_client.get_data_for_qry(index, mapping, qry.es_query)

    dd=pd.DataFrame(data['source'])
    dd.to_csv('Thailand_horeca121.csv')