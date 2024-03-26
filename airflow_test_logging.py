
import requests
import json
import pyodbc
import sqlalchemy as sa
from datetime import timedelta
import time
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import jira_credentials as  C

'''


###### Connection Variables #####

server = 'MASKED'
database = 'MASKED'
driver_src='MASKED'
port = str(1433)
string_src = 'MASKED' + server + ':' + port + '/' + database + '?' + driver_src
engine_src = sa.create_engine(string_src)
conn_src = engine_src.connect()
baseurl="MASKED"
baseurl_composer="MASKED"
#############
username="MASKED"
password="MASKED"
#################
username2="MASKED"
password2="MASKED"
params = {
    "limit": 100,
    "offset": 0,
}

event_tablo="MASKED TABLE"
event_tablo_2="MASKED TABLE"
task_instances_tablo='MASKED TABLE'

directory = 'BQ JSON KEY PATH'
pk_json_input = directory + "MASKED.json"
auth = service_account.Credentials.from_service_account_file(pk_json_input)

compose_user='MASKED'
compose_pwd='MASKED'



def eventLogs_gqb(x="eventLogs"):

    all_data = []

    #client = bigquery.Client.from_service_account_json(pk_json_input)



    #query_job = client.query(query)



    params["offset"] = 0
    print(params["offset"])

    while True:

        response = requests.get(f"{baseurl_composer}{x}", params=params, auth=(C.email,C.psw))

        data = response.json()

        all_data.extend(data["event_logs"])

        if len(data["event_logs"]) < params["limit"]:
            break

        params["offset"] += params["limit"]
        print(params["offset"])
    df = pd.DataFrame(all_data)
    print(df)

    #df.to_gbq('MASKED_TABLE', credentials=auth, if_exists='append',)

    print("GBQ aktarımı tamamlandı. Toplam kayıt sayısı:", len(all_data))

def taskInstances(x="dags/~/dagRuns/~/taskInstances"):
    all_data = []
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
    cursor = conn.cursor()

    query = "SELECT count(*) FROM " + task_instances_tablo
    cursor.execute(query)
    rows = cursor.fetchall()
    params["offset"] = rows[0][0]
    print(params["offset"])

    while True:

        response = requests.get(f"{baseurl}{x}", params=params, auth=(username2, password2))
        data = response.json()
        all_data.extend(data["task_instances"])

        if len(data["task_instances"]) < params["limit"]:
            break

        params["offset"] += params["limit"]
        print(params["offset"])
    df = pd.DataFrame(all_data)
    selected_columns = ["dag_id","dag_run_id","duration","end_date","execution_date","executor_config","hostname","map_index","max_tries","note","operator","pid","pool","pool_slots","priority_weight","queue","queued_when","sla_miss","start_date","state","task_id","trigger","try_number","unixname"]
    df[selected_columns].to_sql(task_instances_tablo, conn_src, if_exists="append", index=False)

    print("Veri SQL Server'a aktarıldı. Toplam kayıt sayısı:", len(all_data))



def eventLogs(x="eventLogs"):
    all_data = []
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
    cursor = conn.cursor()

    query = "SELECT count(*) FROM " + event_tablo
    cursor.execute(query)
    rows = cursor.fetchall()
    params["offset"]=rows[0][0]
    print(params["offset"])

    while True:

        response = requests.get(f"{baseurl}{x}", params=params, auth=(username2, password2))
        data = response.json()
        all_data.extend(data["event_logs"])

        if len(data["event_logs"]) < params["limit"]:
            break

        params["offset"] += params["limit"]
        print(params["offset"])
    df = pd.DataFrame(all_data)
    df.to_sql(event_tablo, conn_src, if_exists="append", index=False)

    print("Veri SQL Server'a aktarıldı. Toplam kayıt sayısı:", len(all_data))




def main():
    start_tgt = time.time()
    eventLogs()
    end_tgt = time.time()
    time_tgt = timedelta(seconds=end_tgt - start_tgt)
    print(time_tgt)

#main()

start_tgt = time.time()
taskInstances()
end_tgt = time.time()
time_tgt = timedelta(seconds=end_tgt - start_tgt)


def main():
    start_tgt = time.time()
    eventLogs_gqb()
    end_tgt = time.time()
    time_tgt = timedelta(seconds=end_tgt - start_tgt)
    print(time_tgt)

main()
#eventLogs_gqb("dagRuns")

'''

import requests


api_key = 'MASKED API KEY'


api_url = 'MASKED URL'


headers = {
    'Authorization': f'Bearer {api_key}'  
}

try:
    
    response = requests.get(api_url, headers=headers)

    
    if response.status_code == 200:
        data = response.json()
        print("API yanıtı:", data)
    else:
        print("API isteği başarısız oldu. Durum kodu:", response.status_code)

except Exception as e:
    print("Bir hata oluştu:", str(e))

print('--------------')
time.sleep(20)
print('---------------')