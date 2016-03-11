import logging
import csv
from math import isnan
import pandas as pd
from utility import helper
from pylastic import ESWrapper

logger = logging.getLogger('crm_matching')
helper.setup_logger('c:\\temp\\', 'crm_matching', logger)

es = ESWrapper(['es01.online-pmsi.com'], logger)

country = 'Belgium'

file_path = 'c:\\temp\\OnTradeBELGIEbusinesspartners_ver2.csv'

cols_to_keep = ['Klantnr', 'Klantnm', 'FA_Email', 'FA_GSM', 'Adres', 'Huisnr', 'Land', 'Postcode', 'Stadsnaam',
                'Telefoon', 'SMS', 'GSM_1', 'Email', 'Cont_Gsm', 'longitude', 'latitude']

index_name = 'crm_matching'
type_name = 'belgium_1'
mapping = "{\"mappings\":{\"primary\":{\"dynamic_templates\":[{\"template\":{\"mapping\":{\"index\":\"not_analyzed\"," \
          "\"type\": \"string\"},\"match\": \"*\",\"match_mapping_type\": \"string\"}}],\"properties\":{\"geopoint\":" \
          "{\"type\": \"geo_point\",\"lat_lon\":true,\"geohash\":true}}}}}"

data_json_clean = []

try:
    if file_path.lower().endswith('.csv'):
        df = pd.read_csv(file_path, encoding='iso-8859-1')
    elif file_path.lower().endswith(('.xls', '.xlsx')):
        df = pd.read_excel(file_path, encoding='iso-8859-1')
    else:
        print('Failed to read the file, extension is not xls, xlsx, or csv.')
except Exception as ex:
    logger.error('Error reading the file - %s', ex)

try:
    cols = list(df.columns.values)
    for col in cols:
        if col not in cols_to_keep:
            df.drop(col, axis=1, inplace=True)
except Exception as ex:
    logger.error('Error dropping columns - %s', ex)

try:
    lon = df['longitude'].tolist()
    lat = df['latitude'].tolist()
    df['geopoint'] = [{'lon': lon[i], 'lat': lat[i]} for i in range(len(lon))]
    df.drop(['longitude', 'latitude'], axis=1, inplace=True)
    for doc in df['geopoint']:
        if isnan(doc['lon']):
            del doc['lon']
        if isnan(doc['lat']):
            del doc['lat']
except Exception as ex:
    logger.error('Error creating geopoint column - %s', ex)

try:
    df['unique_id'] = list(range(len(lon)))
    df['country'] = [country] * len(lon)
except Exception as ex:
    logger.error('Error creating unique_id and country columns - %s', ex)

try:
    data_json = df.to_dict('records')
    for i in data_json:
        dict_clean = dict((m, n) for m, n in i.items() if not (type(n) == float and isnan(n)))
        data_json_clean.append(dict_clean)
except Exception as ex:
    logger.error('Error creating json format - %s', ex)

exists = es.es_client.indices.exists_type(index_name, type)
if not exists:
    try:
        es.es_client.indices.create(index=index_name, ignore=400, body=mapping)
        es.bulk_index(es.es_client, index_name, type_name, data_json_clean, 'unique_id')
    except Exception as ex:
        logger.error('Error indexing to elasticsearch - %s', ex)
else:
    try:
        es.bulk_index(es.es_client, index_name, type_name, data_json_clean, 'unique_id')
    except Exception as ex:
        logger.error('Error indexing to elasticsearch - %s', ex)
