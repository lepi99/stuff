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
#qry.add_missing_field_filter("must", "geopoint")
qry.add_term_query('must', 'country_combined_', "Thailand")
#qry.add_term_query('must', 'pmsi_type_simple', "Bar")
qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
# get doc counts for query
count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
qry.add_size(count)
data = es_client.get_data_for_qry(index, mapping, qry.es_query)


dt=pd.read_excel("Thai Address Matching 03.xlsx", sheetname="Sheet1",index_col=None, na_values=['NA'])
dt2=dt[~dt['lat'].isnull()]

from geopy.distance import vincenty
l500=[]
l400=[]
l350=[]
l300=[]
l250=[]
fgg=list(dt2['lat_lng'])
zz=0
outf=0
for nn in fgg:
    # if zz > 20:
    #     break
    zz+=1
    print("scholl count - ",zz)
    bn=nn.split(",")
    #tt=0
    for aa in data['source']:
        #tt+=1
        #print(tt)
        ot=(aa["geopoint"][1],aa["geopoint"][0])
        dc=vincenty(bn, ot).km
        if dc < 0.5:
          l500.append(aa['base_id'])
        if dc < 0.4:
          l400.append(aa['base_id'])
        if dc < 0.35:
          l350.append(aa['base_id'])
        if dc < 0.3:
          l300.append(aa['base_id'])
        if dc < 0.25:
          l250.append(aa['base_id'])
          outf+=1
          print("Found - ",dc," - ",outf)

print("l500 size - ",len(l500))
print("l400 size - ",len(l400))
print("l350 size - ",len(l350))
print("l300 size - ",len(l300))
print("l250 size - ",len(l250))

ff=open("l500.csv","w")
for dd in list(set(l500)):
    ff.write(str(dd)+"\n")
ff.close()

ff=open("l400.csv","w")
for dd in list(set(l400)):
    ff.write(str(dd)+"\n")
ff.close()

ff=open("l350.csv","w")
for dd in list(set(l350)):
    ff.write(str(dd)+"\n")
ff.close()

ff=open("l300.csv","w")
for dd in list(set(l300)):
    ff.write(str(dd)+"\n")
ff.close()

ff=open("l250.csv","w")
for dd in list(set(l250)):
    ff.write(str(dd)+"\n")
ff.close()




