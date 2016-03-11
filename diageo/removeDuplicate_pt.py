__author__ = 'rfernandes'

from pylastic import ESWrapper, ESQueryBuilder
import logging
import arrow
logger = logging.getLogger('Crawler')
es_client = ESWrapper(['84.40.63.146:9200'], logger)
import uuid

obR=open("crmMatchesSpain.csv")
Lin=obR.readlines()
obR.close()
bb=0
listDeleted=[]
for ll in Lin:
    lSide,rSide=ll.strip().split(",")
    #print(lSide,rSide)

    queryi1 = ESQueryBuilder()
    queryi1.add_term_query('must', 'place_id', str(lSide.strip()))
    queryi1.add_term_query('must', 'is_deleted', 'false')
    #sort_order = [{"field_name": "pmsi_id", "order": "asc" }]
    #queryi1.add_sort_order(sort_order)
    doc_response1 = es_client.get_data_for_qry('outlet_matching','matches_test', queryi1.es_query, 1)
    #print(lSide)
    if len(doc_response1['source'])>0:
        pmsi_id1=doc_response1['source'][0]['pmsi_id']

    queryi2 = ESQueryBuilder()
    queryi2.add_term_query('must', 'place_id', str(rSide.strip()))
    queryi2.add_term_query('must', 'is_deleted', 'false')
    #sort_order = [{"field_name": "pmsi_id", "order": "asc" }]
    #queryi2.add_sort_order(sort_order)
    doc_response2 = es_client.get_data_for_qry('outlet_matching','matches_test', queryi2.es_query, 1)
    #print(rSide)
    if len(doc_response2['source'])>0:
        pmsi_id2=doc_response2['source'][0]['pmsi_id']

    if len(doc_response1['source'])>0 and len(doc_response2['source'])>0:
        goog1=False
        goog2=False

        if not('C' in lSide):

            place_idC="fb_place_id"
        else:
            place_idC="google_place_id"
            goog1=True
            #print(lSide)
        query3 = ESQueryBuilder()
        #queryi.add_term_query('must', 'place_id', '119367591524946')
        query3.add_term_query('must', 'pmsi_id', str(pmsi_id1))
        query3.add_term_query('must', 'is_deleted', 'false')
        sort_order = [{"field_name": "place_id", "order": "desc" }]
        query3.add_sort_order(sort_order)
        doc_response3 = es_client.get_data_for_qry('horeca_master','horeca_master', query3.es_query, 10)
        # if doc_response3['source']==0:
        #     print(pmsi_id1)

        if not('C' in rSide):
            place_idC="fb_place_id"
        else:
            goog2=True
            place_idC="google_place_id"
        query4 = ESQueryBuilder()
        #queryi.add_term_query('must', 'place_id', '119367591524946')
        query4.add_term_query('must', 'pmsi_id', str(pmsi_id2))
        query4.add_term_query('must', 'is_deleted', 'false')
        sort_order = [{"field_name": "place_id", "order": "desc" }]
        query4.add_sort_order(sort_order)
        doc_response4 = es_client.get_data_for_qry('horeca_master','horeca_master', query4.es_query, 10)
        # if doc_response4['source']==0:
        #     print(pmsi_id2)
        # if doc_response4['source'][0]['Name_m'].lower()!=doc_response3['source'][0]['Name_m'].lower():
        #     print(doc_response4['source'][0]['Name_m']," -------- ",doc_response3['source'][0]['Name_m'])
        #keepPmsi
        changePId=""
        changePmsi=""
        keepPid=""
        keepPmsi=""
        deleteMaster=""
        if goog1 and not(goog2):
            changePId=rSide
            changePmsi=pmsi_id2
            keepPid=lSide
            keepPmsi=pmsi_id1
            deleteMaster=doc_response4
        elif not(goog1) and goog2:
            changePId=lSide
            changePmsi=pmsi_id1
            keepPid=rSide
            keepPmsi=pmsi_id2
            deleteMaster=doc_response3
        elif doc_response3['source'][0]['completeness'] > doc_response4['source'][0]['completeness']:
            changePId=rSide
            changePmsi=pmsi_id2
            keepPid=lSide
            keepPmsi=pmsi_id1
            deleteMaster=doc_response4
        elif doc_response3['source'][0]['completeness'] < doc_response4['source'][0]['completeness']:
            changePId=lSide
            changePmsi=pmsi_id1
            keepPid=rSide
            keepPmsi=pmsi_id2
            deleteMaster=doc_response3

        # elif lSide == rSide:
        #     print(lSide,rSide)
        #     print(doc_response3['source'][0]['completeness'] , doc_response4['source'][0]['completeness'])
        elif pmsi_id1 < pmsi_id2:
            changePId=rSide
            changePmsi=pmsi_id2
            keepPid=lSide
            keepPmsi=pmsi_id1
            deleteMaster=doc_response4
        elif pmsi_id1 > pmsi_id2:
            changePId=lSide
            changePmsi=pmsi_id1
            keepPid=rSide
            keepPmsi=pmsi_id2
            deleteMaster=doc_response3

        else:
            print(lSide,rSide)
            print(doc_response3['source'][0]['completeness'] , doc_response4['source'][0]['completeness'])
            print(doc_response3['source'][0]['url'] , doc_response4['source'][0]['url'])


        indexMatch='outlet_matching'
        mapMatch='matches_test'
############################################
        if True:
            queryi = ESQueryBuilder()
            queryi.add_term_query('must', 'pmsi_id', str(changePmsi))
            # queryi.add_term_query('must', 'details_fetched', 'true')
            queryi.add_term_query('must', 'is_deleted', 'false')
            sort_order = [{"field_name": "place_id", "order": "desc" }]
            doc_count = es_client.get_doc_count_for_qry(indexMatch,mapMatch, queryi.es_query)
            queryi.add_sort_order(sort_order)
            doc_responseOrigin = es_client.get_data_for_qry(indexMatch,mapMatch, queryi.es_query, total=doc_count)

            for cc in doc_responseOrigin['hits']:

                search_response_delete = {
                    'place_id':cc["_source"]["place_id"],
                    'is_deleted': True,
                    'pmsi_id':changePmsi,#cc["_source"]["pmsi_id"],
                    'changeN':0,
                    'created_on':cc["_source"]["created_on"],
                    'modified_on':arrow.utcnow().format('YYYY-MM-DDTHH:mm:ss'),
                    'reason':'merged'
                }
                es_client.index_doc_with_id(search_response_delete, indexMatch,mapMatch, cc['_id'])
                rpt_doc_id_change=str(uuid.uuid1()).replace('-', '')
                search_response_change = {
                    'place_id':cc["_source"]["place_id"],
                    'is_deleted': False,
                    'pmsi_id':keepPmsi,
                    'changeN':0,
                    'created_on':arrow.utcnow().format('YYYY-MM-DDTHH:mm:ss'),
                    'modified_on':arrow.utcnow().format('YYYY-MM-DDTHH:mm:ss'),
                    'reason':''
                }
                es_client.index_doc_with_id(search_response_change, indexMatch,mapMatch, rpt_doc_id_change)
                #matchDict[cc["_source"]["place_id"]]=keepPmsi
                #making sure that recent changes have been done
                while True:
                    query1 = ESQueryBuilder()
                    query1.add_term_query('must', '_id', cc['_id'])
                    query1.add_term_query('must', 'pmsi_id', str(changePmsi))
                    query1.add_term_query('must', 'place_id', cc["_source"]["place_id"])
                    # queryi.add_term_query('must', 'details_fetched', 'true')
                    query1.add_term_query('must', 'is_deleted', 'true')
                    #sort_order = [{"field_name": "place_id", "order": "desc" }]
                    #query1.add_sort_order(sort_order)
                    doc_responseCheckD = es_client.get_data_for_qry(indexMatch,mapMatch, query1.es_query, 1)
                    if len(doc_responseCheckD['source'])>0:
                        break

                while True:
                    queryi = ESQueryBuilder()
                    queryi.add_term_query('must', '_id', rpt_doc_id_change)
                    queryi.add_term_query('must', 'place_id', cc["_source"]["place_id"])
                    queryi.add_term_query('must', 'pmsi_id', str(keepPmsi))
                    # queryi.add_term_query('must', 'details_fetched', 'true')
                    queryi.add_term_query('must', 'is_deleted', 'false')
                    #sort_order = [{"field_name": "place_id", "order": "desc" }]
                    #queryi.add_sort_order(sort_order)
                    doc_responseCheck = es_client.get_data_for_qry(indexMatch,mapMatch, queryi.es_query, 1)
                    if len(doc_responseCheck['source'])>0:
                        break
            if len(deleteMaster['source'])>0:
                listDeleted.append(changePmsi)
                deleteMaster['source'][0]['is_deleted']='true'
                deleteMaster['source'][0]['outlet_state']='merged_manual'
                es_client.index_doc_with_id(deleteMaster['source'][0], 'horeca_master', 'horeca_master', changePmsi)
            else:
                print("Already deleted - ",changePmsi)
############################################

    # else:
    #     if len(doc_response1['source'])==0:
    #         print(lSide)
    #     if len(doc_response2['source'])==0:
    #         print(rSide)
            bb+=1
            print(bb)
print(listDeleted)

    #print("aa")