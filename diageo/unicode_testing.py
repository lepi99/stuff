__author__ = 'rfernandes'


import pandas as pd
import unicodedata
def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category( c ) != 'Mn')



df = pd.io.excel.read_excel('CroatiaData_name_encode.xlsx', 'CroatiaData_name_encode')

# Create an iteration counter
i = 0

# For each element in first_name and last_name,
for addr, nam, stre in zip(df['Address'], df['Name_m'], df['street']):
    # Change the value of the i'th row in full_name
    # to the combination of the first and last name
    print(df['Address'][i])
    df['Address'][i] = strip_accents(str(addr))
    print(df['Address'][i])
    print(df['Name_m'][i])
    df['Name_m'][i] = strip_accents(nam)
    print(df['Name_m'][i])
    print(df['street'][i])
    df['street'][i] = strip_accents(str(stre))
    print(df['street'][i])

    # Add one to the iteration counter
    i = i+1

df.to_csv("clean_name_croatia.csv")

#ccf=pd.read_csv("CroatiaData_name_encode.csv",)
print("Maçã")
print(strip_accents('vendéglő'))
