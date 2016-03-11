__author__ = 'rfernandes'


import logging
from utility import helper as um
from pylastic import ESWrapper as ES, ESQueryBuilder
import pandas as pd
logger = logging.getLogger('ReverseGeocoder')
um.setup_logger('c:\\temp\\', 'testVelocity', logger)
es_client = ES(['84.40.63.146:9200'], logger)
index = 'horeca_master'
mapping = 'horeca_master'
import urllib.request as urllib2
#import urllib
from urllib.parse import urlparse, parse_qs, urlencode
import sys
import urllib.error

def checkUrlRedirection(url):
    print(url)
    return _checkUrlRedirection(url,url)

def _checkUrlRedirection(url,initial):

    master_url=url
    try:
        req = urllib2.Request(master_url)
        res = urllib2.urlopen(req)
        finalurl = res.geturl()
        if finalurl != initial:
            return {'result':'yes','finalUrl':finalurl}
        else:
            return {'result':'no','finalUrl':master_url}

    except UnicodeEncodeError:
        print("unicode case")
        print(master_url)
        nurl=list(req.redirect_dict.keys())[0]
        nurl=nurl#.encode('latin1').decode('utf8')#.encode('ASCII')
        parsed_url = urlparse(nurl)
        urlc = parsed_url._replace(path=urllib2.quote(parsed_url.path.encode('utf8'))).geturl()#.geturl()

        return _checkUrlRedirection(urlc,initial)#{'result':'yes','finalUrl':urlc}

    except urllib.error.HTTPError as e:
        return {'result':e}

def checkUrlCountry(country,version):
    qry = ESQueryBuilder()
    #qry.add_missing_field_filter("must", "geopoint")
    qry.add_term_query('must', 'is_deleted', 'false')
    qry.add_term_query('must', 'version', str(version))
    qry.add_term_query('must', 'country_combined_', country)
    # qry.add_term_query('must', 'base_id', str(delePmsi))
    qry.add_sort_order([{ "field_name": "base_id", "order": "asc" }])
    # get doc counts for query
    count = es_client.get_doc_count_for_qry(index, mapping, qry.es_query)
    qry.add_size(count)
    data = es_client.get_data_for_qry(index, mapping, qry.es_query)
    #from urllib import parse
    changesDict={}
    changesDict['same']=0
    changesDict['change']=0
    changesDict['closed']=0

    for doc in data['source']:
        master_url=doc['url']
        rr=checkUrlRedirection(master_url)
        print(rr)
        #changesDict[rr['result']]+=1
        #if rr['result'] != 'same':
            #actResult(rr,doc)
    #print(changesDict)

def actResult(urlResult,docl):
    #if it is closed, flag it somehow. dont forget to make it not analysed
    #if changed - get somehow the id to where it changed and then see if it is some other pmsi_id
    pass


checkUrlCountry("Portugal",0)
#     print("-------------------master url ------------------------------")
#     print(master_url)
#     uu=''
#     if doc['api_source']=='fb':
#       uu='https://www.facebook.com'
#     else:
#       uu='https://plus.google.com'
#     #print(master_url)
#
#     try:
#         req = urllib2.Request(master_url)
#
#
#         #print(req)
#         res = urllib2.urlopen(req)
#         finalurl = res.geturl()
#         #print(finalurl)
#         if finalurl != master_url:
#             print("---------------------------------------------------------")
#             dd+=1
#             print(master_url)
#             print(finalurl)
#             print(dd)
#             if len(req.redirect_dict) > 0:
#                print("cc")
#                print(req.redirect_dict)
#     except UnicodeEncodeError:
#         print("print unicode error")
#         print(sys.exc_info()[1].reason)
#         if len(req.redirect_dict) > 0:
#                print("ee")
#                print(req.redirect_dict)
#         #url = 'http://sum.in.ua/?swrd=автор'
#         nurl=list(req.redirect_dict.keys())[0]
#         nurl=nurl.encode('latin1').decode('utf8')#.encode('ASCII')
#         parsed_url = urlparse(nurl)
#         print(parsed_url)
#         parameters = parse_qs(parsed_url.path)
#         print(parameters)
#         #urlc = parsed_url._replace(query=urlencode(parameters, doseq=True)).geturl()
#         urlc = parsed_url._replace(path=urllib2.quote(parsed_url.path.encode('utf8'))).geturl()#.geturl()
#         print(urlc)
#         req = urllib2.Request(urlc)
#         print(req)
#         res = urllib2.urlopen(req)
#         finalurl = res.geturl()
#         print(finalurl)
#
#     except urllib.error.HTTPError:
#         print("print http error")
#         print(sys.exc_info()[1].reason)
#         # if len(req.redirect_dict) > 0:
#         #        print("ee")
#         #        print(req.redirect_dict)
#
#     except:
#         #print(str(e))
#         print("---------------------------------------------------------")
#         try:
#           if len(req.redirect_dict) > 0:
#                print("dd")
#                print(req.redirect_dict)
#         except:
#             print(sys.exc_info()[1])
#         nn+=1
#         print("except")
#         print(sys.exc_info()[0])
#         print(sys.exc_info()[1].reason)
#
#         if sys.exc_info()[1].reason =='Not Found':
#             print("aa")
#         else:
#             print("bb")
#             print(sys.exc_info()[1].object)
#             print(urllib2.quote(sys.exc_info()[1].object.encode("utf8")))
#             print(sys.exc_info()[1].object.encode("utf8"))
#         #print(sys.exc_info()[1]['code'])
#         print(master_url)
#         print(nn)
#     #     #url = 'http://sum.in.ua/?swrd=автор'
#     #     parsed_url = urlparse(master_url.encode('utf8'))
#     #     print(parsed_url)
#     #     parameters = parse_qs(parsed_url.query)
#     #     print(parameters)
#     #     urlc = parsed_url._replace(query=urlencode(parameters, doseq=True)).geturl()
#     #     print(urlc)
#     #     req = urllib2.Request(urlc)
#     #     print(req)
#     #     res = urllib2.urlopen(req)
#     #     finalurl = res.geturl()
#     #     print(finalurl)
#
# print(nn)
# print(dd)


# logger = logging.getLogger('Crawler')
# um.setup_logger('c:\\temp\\', 'Crawler2',logger, add_console=False, log_level=logging.INFO)
# logger.setLevel(logging.INFO)  # to over-ride this, needs to be after the setup
# logger.propagate = False
# dd=graph_api(logger)
#
# url='https://www.facebook.com/laboheme.baixa'
# # url='https://www.facebook.com/pages/Restaurante-Sem-Nome/117642388319773'
# token='681143818643788|U_-YXwnMJXBU6Gfy71eLs-Bzk2o'
#
# yy=dd.get_id_from_utl(url,token)
# idi=''
# for yyi in yy:
#     idi=yy[yyi]['id']
#     break
# #print(yy.id)
# yy2=dd.checkUrlRedirection(url)
# print(yy2)
# yy3=dd.is_permnanently_closed(idi, token)
# print(yy3)


