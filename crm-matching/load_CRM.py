import logging
import csv
from math import isnan
import pandas as pd
from utility import helper
from pylastic import ESWrapper, ESQueryBuilder
import uuid
import random
import arrow
import json

class CRM_input():
    #{Name:"",Phone:"",Address:"",email:"",post_code:"",lat_lon:[],lon_lat:[],lat:"",lon:""}
    def __init__(self, index=None, mapping=None,country=None,config_file_path=None,map_id=None):
        self.valid_countries=['Portugal','Belgium','Spain','United Kingdom','Germany','Netherlands','Switzerland','Finland','Sweden','Norway','Denmark','Greece','Italy','Austria']
        self.logger = logging.getLogger('crm_matching')
        helper.setup_logger('c:\\temp\\', 'crm_matching', self.logger)
        self.config_file_path='mappings_config.json'
        self.es = ESWrapper(['es01.online-pmsi.com'], self.logger)

        #self.col_mapping = col_mapping

        self.country = self.validate_country(country)
        self.configs = self.load_config(config_file_path,map_id,country)

        self.file_path = self.configs["file_path"]#'c:\\temp\\OnTradeBELGIEbusinesspartners_ver2.csv'
        self.col_mapping = self.configs["mapings"]
        # self.cols_to_keep = ['Klantnr', 'Klantnm', 'FA_Email', 'FA_GSM', 'Adres', 'Huisnr', 'Land', 'Postcode', 'Stadsnaam',
        #                 'Telefoon', 'SMS', 'GSM_1', 'Email', 'Cont_Gsm', 'longitude', 'latitude']
        self.index_name = 'crm_matching'
        self.type_name = 'crm_matching2'
        self.version=None
        # self.mapping = "{\"mappings\":{\"primary\":{\"dynamic_templates\":[{\"template\":{\"mapping\":{\"index\":\"not_analyzed\"," \
        #           "\"type\": \"string\"},\"match\": \"*\",\"match_mapping_type\": \"string\"}}],\"properties\":{\"geopoint\":" \
        #           "{\"type\": \"geo_point\",\"lat_lon\":true,\"geohash\":true}}}}}"

        #data_json_clean = []

    def validate_country(self,country):
        if country in self.valid_countries:
            return country
        else:
            raise "its not a valid country - "+country

    def load_config(self,c_file_path=None,map_id=None,country=None):
        if c_file_path == None:
            c_file_path=self.config_file_path
        if map_id == None:
            raise "no map_id provided"
        try:
            self.logger.info("Loading config...")
            with open(c_file_path, "r") as file:
                data = file.read()
                jsdata= json.loads(data)
            for rec in jsdata:
                if rec['map_id']==map_id and rec["country"]==country:
                    return rec
            raise "invalid map_id provided"
        except Exception as ex:
            self.logger.error("Error while loading config - " + str(ex))


    def uploadFile(self,file_path=None):
        if file_path == None:
            file_path=self.file_path
        try:
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path, encoding='iso-8859-1')
            elif file_path.lower().endswith(('.xls', '.xlsx')): #!!!give tab option
                df = pd.read_excel(file_path, encoding='iso-8859-1')
            else:
                raise 'Failed to read the file, extension is not xls, xlsx, or csv.'
            return df
        except Exception as ex:
            self.logger.error('Error reading the file - %s', ex)


    def map_CRM(self,df):

        #try:
            list_to_keep=[]
            dict2 = self.col_mapping.copy()
            for cc in self.col_mapping:
                print(self.col_mapping[cc])
                print(cc)
                if self.col_mapping[cc] != None and str(self.col_mapping[cc]).strip() != "" and self.col_mapping[cc]!= False and self.col_mapping[cc]!= "false":
                    list_to_keep.append(self.col_mapping[cc])
                else:
                    del dict2[cc]
            cols = list(df.columns.values)
            for col in list_to_keep:
                if col not in cols:
                    print("----")
                    print(col)
                    self.logger.error('invalid column name - %s', col)
                    raise "invalid column name - "+ col
                    #df.drop(col, axis=1, inplace=True)
            new_df=df[list_to_keep]
            reversed_dict={}
            for hh in dict2:
                reversed_dict[dict2[hh]]=hh
            new_df.rename(columns=reversed_dict, inplace=True)
            self.col_mapping=dict2
        #except Exception as ex:
            #logger.error('Error dropping columns - %s', ex)
            return self.clean_geopoint(new_df)

    def clean_geopoint(self,df):
        if "lat" in self.col_mapping or "lon" in self.col_mapping:
            if "lat" in self.col_mapping and "lon" in self.col_mapping:
                df['geopoint']=""
                df['geopoint2']=""
                for index, row in df.copy().iterrows():
                    #print(row)
                    if row["lat"] != None and str(row["lat"]).strip()!="" and row["lon"] != None and str(row["lon"]).strip()!="":
                        df.loc[index,'geopoint']={'lon': row["lon"]}
                        df.loc[index,'geopoint']['lat']= row["lat"]
                        df.loc[index,'geopoint2']=[row["lon"]]
                        df.loc[index,'geopoint2'].append(row["lat"])

            else:
                raise "Only one of lat/long present"

            del df["lon"]
            del df["lat"]
        elif "lat_long" in self.col_mapping:
            df['geopoint']=""
            df['geopoint2']=""
            for index, row in df.copy().iterrows():
                    if row["lat_lon"] != None and str(row["lat_lon"]).strip()!=":":
                        gp=str(row["lat_lon"]).replace("[","").replace("]","").split(",")
                        df.loc[index,'geopoint']={'lon': gp[1]}
                        df.loc[index,'geopoint']['lat']= gp[0]
                        df.loc[index,'geopoint2']=[row["lon"]]
                        df.loc[index,'geopoint2'].append(row["lat"])
                        #df['geopoint'][index] = {'lon': gp[0], 'lat': gp[1]}
            del df["lat_long"]
        elif "lon_lat" in self.col_mapping:
            df['geopoint']=""
            df['geopoint2']=""
            for index, row in df.copy().iterrows():
                 if row["lon_lat"] != None and str(row["lon_lat"]).strip()!=":":
                    gp=str(row["lon_lat"]).replace("[","").replace("]","").split(",")
                    df.loc[index,'geopoint']={'lon': gp[0]}
                    df.loc[index,'geopoint']['lat']= gp[1]
                    df.loc[index,'geopoint2']=[row["lon"]]
                    df.loc[index,'geopoint2'].append(row["lat"])
                    #df['geopoint'][index] = {'lon': gp[0], 'lat': gp[1]}
            del df["long_lat"]
        return df

    def get_country_version_count(self,version):
        qry = ESQueryBuilder()
        qry.add_term_query('must', 'country', self.country)
        qry.add_term_query('must', 'version', str(version))
        count = self.es.get_doc_count_for_qry(self.index_name, self.type_name, qry.es_query)
        return count

    def prepare_for_elastic(self,df):
        #try:
            vv=-1
            ff=1
            while ff!=0:
                vv+=1
                ff=self.get_country_version_count(vv)

            data_json = df.to_dict('records')
            new_data_json=[]
            dateNow=arrow.utcnow().format('YYYY-MM-DDTHH:mm:ss')
            for req in data_json:
                req_copy=req.copy()
                for col in req_copy:

                    if req[col] == None or (type(req[col]) == float and isnan(req[col])):
                        del req[col]
                    if col == "geopoint":
                      if isnan(req['geopoint']["lat"]) or isnan(req['geopoint']["lon"]):
                        del req['geopoint']
                    if col == "geopoint2":
                        if req['geopoint2']==[] or (type(req["geopoint2"][0]) == float and isnan(req['geopoint2'][0])) or len(req['geopoint2'])<2:
                            del req['geopoint2']

                req['unique_id']=str(uuid.uuid1()).replace('-', '')#+str(random.random())
                req['version']=vv
                self.version=req['version']
                req['country']=self.country
                req['country_version']=req['country']+"_"+str(vv)
                req['date_insert']=dateNow##??add date
                req['CRM_file']=self.file_path
                req['col_mapping']=self.configs#self.col_mapping

                #dict_clean = dict((m, n) for m, n in i.items() if not (type(n) == float and isnan(n)))
                #data_json_clean.append(dict_clean)
                new_data_json.append(req)
            return new_data_json
        # except Exception as ex:
        #     self.logger.error('Error creating json format - %s', ex)



    def insert_in_elastic_bulk(self,dict_list):
        pd.DataFrame(dict_list).to_csv("test_Bel2.csv")
        try:
            self.es.bulk_index(self.es.es_client, self.index_name, self.type_name, dict_list, 'unique_id')
        except Exception as ex:
            self.logger.error('Error indexing to elasticsearch - %s', ex)

    def insert_in_elastic(self,dict_list):
        pd.DataFrame(dict_list).to_csv("test_Bel2.csv")
        for rr in dict_list:
            self.es.index_doc_with_id(rr, self.index_name,self.type_name, rr['unique_id'])#(self.es.es_client, self.index_name, self.type_name, dict_list, 'unique_id')
        # try:
        #     self.es.bulk_index(self.es.es_client, self.index_name, self.type_name, dict_list, 'unique_id')
        # except Exception as ex:
        #     self.logger.error('Error indexing to elasticsearch - %s', ex)

    def CRM_to_elastic(self):
        df_upload=self.uploadFile()
        df_mapped=self.map_CRM(df_upload)
        print(df_mapped)
        dict_list=self.prepare_for_elastic(df_mapped)
        print(dict_list)
        # self.insert_in_elastic(dict_list)
        self.insert_in_elastic_bulk(dict_list)

#col_dit={"CRM_id":"Klantnr","Name":"Klantnm","Phone":"FA_GSM","city":"Stadsnaam","Address":"Huisnr_Adres","email":"FA_Email","post_code":"Postcode","lat_lon":[],"lon_lat":[],"lat":"latitude","lon":"longitude"}
#col_dit={"CRM_id":"Klantnr","Name":"Klantnm","Phone":"FA_GSM","city":"Stadsnaam","Address":"Huisnr_Adres","email":"FA_Email","post_code":"Postcode","lat_lon":"","lon_lat":"","lat":"latitude","lon":"longitude"}
crm_i= CRM_input(index="crm_matching",mapping="crm_matching",country="Sweden",map_id="Sweden_crm")
crm_i.CRM_to_elastic()

# try:
#     lon = df['longitude'].tolist()
#     lat = df['latitude'].tolist()
#     df['geopoint'] = [{'lon': lon[i], 'lat': lat[i]} for i in range(len(lon))]
#     df.drop(['longitude', 'latitude'], axis=1, inplace=True)
#     for doc in df['geopoint']:
#         if isnan(doc['lon']):
#             del doc['lon']
#         if isnan(doc['lat']):
#             del doc['lat']
# except Exception as ex:
#     logger.error('Error creating geopoint column - %s', ex)
#
# try:
#     df['unique_id'] = list(range(len(lon)))
#     df['country'] = [country] * len(lon)
# except Exception as ex:
#     logger.error('Error creating unique_id and country columns - %s', ex)
#
# try:
#     data_json = df.to_dict('records')
#     for i in data_json:
#         dict_clean = dict((m, n) for m, n in i.items() if not (type(n) == float and isnan(n)))
#         data_json_clean.append(dict_clean)
# except Exception as ex:
#     logger.error('Error creating json format - %s', ex)
#
# exists = es.es_client.indices.exists_type(index_name, type)
# if not exists:
#     try:
#         es.es_client.indices.create(index=index_name, ignore=400, body=mapping)
#         es.bulk_index(es.es_client, index_name, type_name, data_json_clean, 'unique_id')
#     except Exception as ex:
#         logger.error('Error indexing to elasticsearch - %s', ex)
# else:
#     try:
#         es.bulk_index(es.es_client, index_name, type_name, data_json_clean, 'unique_id')
#     except Exception as ex:
#         logger.error('Error indexing to elasticsearch - %s', ex)
