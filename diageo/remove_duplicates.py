__author__ = 'rfernandes'

import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
import numpy as np
import math
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'testVelocity', logger)
es_client = ES(['84.40.63.146:9200'], logger)

index = 'horeca_master'
mapping = 'horeca_master'

#dupl=pd.read_excel("RomaniaMaster_duplications.xlsx",'Sheet2')
#duplList=dupl.values.tolist()
#deletedL=[]

qry = ESQueryBuilder()
#qry.add_missing_field_filter("must", "geopoint")
qry.add_term_query('must', 'is_deleted', 'false')
qry.add_term_query('must', 'country_combined_', 'Finland')
# qry.add_term_query('must', 'base_id', str(delePmsi))
qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
# get doc counts for query
count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
qry.add_size(count)
data = es_client.get_data_for_qry(index, mapping, qry.es_query)
    #print(data)


df=pd.DataFrame(data['source'])
df1=df[['pmsi_id','geopoint','Name_m','street']]

print(df1.geopoint.str.split(',',1).tolist())
df1[['Lat','Long']] = pd.DataFrame(df1.geopoint.tolist(),columns = ['Lat','Long'])

df1=df1.sort(['Name_m'],ascending=[1])

df1['Lat_2'] = df1['Lat'].shift(-1)
df1['Long_2'] = df1['Long'].shift(-1)
df1['Name_m_2'] = df1['Name_m'].shift(-1)
df1['street_2'] = df1['street'].shift(-1)
df1=df1.iloc[:-1]


#
# y = sqrt(
#     (cos(lat2) * sin(dlon)) ** 2
#     + (cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)) ** 2
#     )
# x = sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(dlon)
# c = np.atan(y, x)
# return EARTH_R * c


#
# dlon = df1['Long_2'] - df1['Long']
# dlat = df1['Lat_2'] - df1['Lat']
#
# a = np.sin((df1['Lat_2'] - df1['Lat'])/2.0)**2 + np.cos(df1['Lat']) * np.cos(df1['Lat_2']) * np.sin((df1['Long_2'] - df1['Long'])/2.0)**2
#
# c = 2 * np.arcsin(np.sqrt(np.sin((df1['Lat_2'] - df1['Lat'])/2.0)**2 + np.cos(df1['Lat']) * np.cos(df1['Lat_2']) * np.sin((df1['Long_2'] - df1['Long'])/2.0)**2))
# km = 6367 * 2 * np.arcsin(np.sqrt(np.sin((df1['Lat_2'] - df1['Lat'])/2.0)**2 + np.cos(df1['Lat']) * np.cos(df1['Lat_2']) * np.sin((df1['Long_2'] - df1['Long'])/2.0)**2))

# =ACOS(COS(RADIANS(90-Lat1)) *COS(RADIANS(90-Lat2)) +SIN(RADIANS(90-Lat1)) *SIN(RADIANS(90-Lat2)) *COS(RADIANS(Long1-Long2))) *6371

df1['distance'] = 6367 * 2 * np.arcsin(np.sqrt(np.sin((df1['Lat_2'] - df1['Lat'])/2.0)**2 + np.cos(df1['Lat']) * np.cos(df1['Lat_2']) * np.sin((df1['Long_2'] - df1['Long'])/2.0)**2))
#df1['Lat']=df2['Lat']
#df1['Long']=df2['Long']

from difflib import SequenceMatcher
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

m = SequenceMatcher(None, "NEW YORK METS", "NEW YORK MEATS")
print(m.ratio())
print(fuzz.token_set_ratio( "NEW YORK METS", "NEW YORK MEATS"))
print(fuzz.token_sort_ratio( "NEW YORK METS", "NEW YORK MEATS"))
print(fuzz.partial_ratio( "NEW YORK METS", "NEW YORK MEATS"))
# m.ratio() ⇒ 0.962962962963


f = lambda x: fuzz.partial_ratio(x['Name_m'],x['Name_m_2'])

df1['matchStr']=df1.apply(f, axis=1)

print(df1)
print(df1[['distance','matchStr','Name_m','street','Name_m_2','street_2']][(df1['distance']<100)&(df1['matchStr']>89)])
ss=df1[['distance','matchStr','Name_m','street','Name_m_2','street_2']][(df1['distance']<100)&(df1['matchStr']>89)]
ss.to_csv("dupCases")
print(df1[(df1['distance']<100)&(df1['matchStr']>89)].shape)
