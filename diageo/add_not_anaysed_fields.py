
import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'testVelocity', logger)
es_client = ES(['84.40.63.146:9200'], logger)
index = 'horeca_master'
mapping = 'horeca_master'


# build the query
while True:
    qry = ESQueryBuilder()
    qry.add_match_all_query()
    #qry.add_missing_field_filter("must", "pmsi_priority_")
    #qry.add_missing_field_filter("must", "pmsi_social_rank_")
    #qry.add_term_query('must', 'is_deleted', 'false')
    #qry.add_term_query('must', 'country_combined_', 'Portugal')
    qry.add_term_query('must', 'version', '1')
    qry.add_term_query('must', 'is_deleted', 'false')
    qry.add_term_query('must', 'outlet_state_indicator', 'closed')
    #qry.add_missing_field_filter("must", "outlet_state_indicator")
    #qry.add_missing_field_filter("must_not", "pmsi_social_rank")
    qry.add_sort_order([{ "field_name": "place_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    #qry.add_size(2) # restrict size for testing
    results = es_client.get_data_for_qry(index, mapping, qry.es_query, total = 1, page_size = 10000)
    data = results['hits']
    oo=0
    for aa in data:
        idh=aa['_id']
        aadata=aa['_source']

        if idh[-1]=='0':
           print(idh)
           print(aadata['pmsi_id_version'])
        #if aadata['pmsi_id_version'] != idh:
           oo+=1
           aadata['is_deleted']='true'
           es_client.index_doc_with_id(aadata, index,mapping, idh)
    print(oo)
    break
    #[x.update({"pmsi_id_version" : x['base_id']+"_1"}) for x in data]
    #print("ee")
    #es_client.bulk_index(es_client.es_client, index, mapping, data, "pmsi_id_version", bulk_size=1000)

