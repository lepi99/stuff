__author__ = 'rfernandes'





import urllib.request
import urllib.parse
import pandas as pd
from pandas import ExcelWriter

def getGPSFomPostCode(postcode):
    ptBrake=str(postcode).split("-")
    # if len(ptBrake)>1:
    try:
        urlStr='http://www.codigo-postal.pt/?cp4='+ptBrake[0]+'&cp3='+ptBrake[1]#346'
    #else:
     #   urlStr='http://www.codigo-postal.pt/?cp4='+ptBrake[0]+'&cp3='#+ptBrake[1]#346'

        with urllib.request.urlopen(urlStr) as url:
         s = url.read()
#I'm guessing this would outputt the html source code?
        print(str(s))

        return str(s).split('<b>GPS:</b>')[1].split('</span>')[0]
    except:
        return None

def getGPSFomAddress(address,Local):
    #f = { 'eventName' : 'myEvent', 'eventDescription' : "cool event"}
    address=urllib.parse.quote(address)
    Local=urllib.parse.quote(Local)
    #ptBrake=postcode.split("-")
    #if len(ptBrake)>1:
    urlStr='http://www.codigo-postal.pt/?rua='+address+'&local='+Local#346'
    #else:
     #   urlStr='http://www.codigo-postal.pt/?cp4='+ptBrake[0]+'&cp3='#+ptBrake[1]#346'

    with urllib.request.urlopen(urlStr) as url:
        s = url.read()
#I'm guessing this would outputt the html source code?
    print(str(s))
    try:
        return str(s).split('<b>GPS:</b>')[1].split('</span>')[0]
    except:
        return None
#print(str(s).split('<b>GPS:</b>')[1].split('</span>')[0])

crmFile='pt_crm_geocoded.xlsx'
df_crm_pt = pd.read_excel(crmFile, 'pt_crm_geocoded', index_col=None, na_values=['NA'])
for rr in range(len(list(df_crm_pt['Postal Code']))):
    postcode=df_crm_pt.ix[rr,'Postal Code']
    #if postcode != None:
    df_crm_pt.ix[rr,'GPS']=getGPSFomPostCode(postcode)
    #if df_crm_pt.ix[rr,'GPS'] == None:
    addr=df_crm_pt.ix[rr,'Street Address 1']
    loc=df_crm_pt.ix[rr,'City']
    df_crm_pt.ix[rr,'GPS2']=getGPSFomAddress(addr,loc)
writer = ExcelWriter('output.xlsx')
df_crm_pt.to_excel(writer,'crm_pstcodes_pt')
writer.save()
print("AA")




