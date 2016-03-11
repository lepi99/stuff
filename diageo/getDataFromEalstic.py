__author__ = 'rfernandes'

import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'ReverseGeocoder', logger)
es_client = ES(['84.40.63.146:9200'], logger)




index = 'horeca_master'
mapping = 'horeca_master'


qry = ESQueryBuilder()
#qry.add_qry_string_query()
#qry.add_missing_field_filter("must", "geopoint")
qry.add_term_query('must', 'is_deleted', 'false')
qry.add_term_query('must', 'country_combined_', 'Portugal')
#qry.add_term_query('must', 'api_source', 'fb')
#qry.add_missing_field_filter("must_not", "existing_outlet")
qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
#qry.add_fields(['base_id','geopoint'])
# get doc counts for query
count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
qry.add_size(count)
data = es_client.get_data_for_qry(index, mapping, qry.es_query)

ff=pd.DataFrame(data['source'])
ff[['base_id','geopoint','Name_m','Address','url','website','existing_outlet_id']].to_csv("PortugalCrmMaster.csv")
