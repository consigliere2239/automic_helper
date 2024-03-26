
import pandas as pd
import os
import glob
import pyodbc
import sqlalchemy as sa

server = 'MASKED'
database = 'MASKED'
driver_src='MASKED'
port = str(1433)
string_src = 'MASKED' + server + ':' + port + '/' + database + '?' + driver_src
engine_src = sa.create_engine(string_src)
conn_src = engine_src.connect()

def search_word_in_dtsx_files(folder_path, search_word):
    dtsx_files = glob.glob(os.path.join(folder_path, "*.dtsx"))

    for dtsx_file in dtsx_files:
        try:
            with open(dtsx_file, 'r', encoding='utf-8') as file:
                dtsx_content = file.read()

                if search_word in dtsx_content:
                    print(f"Kelime '{search_word}' dosyada bulundu: {dtsx_file}")
        except UnicodeDecodeError:
            print(f"{dtsx_file} dosyası Unicode ile kodlanmamış bir dosya.")

import pandas as pd

def excel_to_dataframe(file_path):
    df = pd.read_excel(file_path)
    return df["TABLE_NAME"]



excel_file_path="MASKED PATH"

plm_path="MASKED PATH"




conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
cursor = conn.cursor()

query = "MASKED "
cursor.execute(query)
rows = cursor.fetchall()


excel_file_sps="SP PATH"
excel_file_views="VIEWS TO ITERATE"

rows_df=pd.DataFrame(rows)


def excel_to_dataframe_view_sp(file_path):
    df = pd.read_excel(file_path)
    return df["name"]





def search_string_in_dataframe(dataframe, search_string):

    results = pd.DataFrame()
    for column in dataframe.columns:
        
        if dataframe[column].dtype == "object":
            search_result = dataframe[dataframe[column].str.contains(search_string, case=False, na=False)]
            results = pd.concat([results, search_result], ignore_index=True)
    return results






i=0
for vw_sp_object in excel_to_dataframe_view_sp(excel_file_views):

    for row in rows: 
        i+=1
        print(row)
        print(i)


