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




index = 'crm_matching'
# index = 'crm_matching_reference'
# index = 'master_matching_reference'
# index = 'horeca_master'
# index = 'test_horeca_master'
mapping = 'crm_matching2'
# mapping = 'crm_matching_reference'
# mapping = 'master_matching_reference'
# mapping = 'horeca_master'
# mapping = 'test_horeca_master'

print("aa")

#es_client.es_client.delete_by_query()

def get_master_record(country):
    # build the query
    qry = ESQueryBuilder()
    #qry.add_term_query('must', 'country_combined_', country)
    qry.add_term_query('must', 'country', country)
    qry.add_term_query('must', 'version', '2')
    #qry.add_term_query('must', 'is_deleted', 'false')
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

finlL=[]
dd=get_master_record('Sweden')
ddff=pd.DataFrame(dd)
ddff.to_csv("crm_data_sweden.csv")
# ddff.to_csv("master_data.csv")