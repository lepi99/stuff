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

index = 'off_trade_raw'
mapping = 'off_trade_raw'

qry = ESQueryBuilder()
#qry.add_missing_field_filter("must", "geopoint")
qry.add_term_query('must', 'scrape_id', '1', [])
#qry.add_term_query('must', 'country', 'Thailand', [])
qry.add_sort_order([{ "field_name": "place_id", "order": "asc" }])
# get doc counts for query
count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
qry.add_size(count)
data = es_client.get_data_for_qry(index, mapping, qry.es_query)

dd=pd.DataFrame(data['source'])
dd[['name','Name','Address','street','pmsi_type_simple','main_category','Type_Tag','google_plus_types','types','Website','api_source','is_community_page','likes','checkins','Details_fetch_error','details_error']].to_csv('London_off_trade_important_b_2.csv')