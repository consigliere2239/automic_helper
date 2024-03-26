import requests
import pandas as pd
import json
import pyodbc
import sqlalchemy as sa



###### Connection Variables #####

server = 'MASKED'
database = 'MASKED'
driver_src='MASKED'
port = str(1433)
string_src = 'MASKED' + server + ':' + port + '/' + database + '?' + driver_src
engine_src = sa.create_engine(string_src)
conn_src = engine_src.connect()
baseurl="MASKED"
username="MASKED"
password="MASKED"

hedef_table_name='MASKED'
stg_table_name="MASKED" 

###### API Variables #####
#ALL API VARIABLES MASKED 

joblist=[]
joblist.append(datatoplama)
joblist.append(detaylimaliyetfiyat)
joblist.append(dimdoviz)
joblist.append(lfltakvimkatsayi)
joblist.append(lfltakvimtarihsel)
joblist.append(dailymeasuretarihsel)
joblist.append(perakendetumfiyat)

for job in joblist:
    print(job)

###### FUNCTIONS #####

def getDataFromAliveMaster(jobName):
    url=baseurl+"executions?name="+jobName
    response = requests.get(url, auth=(username, password))
    if response.status_code == 200:
        json_data = response.json()

    else:
        print("Hata: İstek başarısız oldu. Main flowun run idsi alınamadı. Hata kodu:", response.status_code)
    df = pd.DataFrame(json_data["data"])
    c_list = df.columns
    if "run_id" in c_list:

        run_id_l = df["run_id"]
        run_id = str(run_id_l[0])

        url = baseurl + "executions?run_id=" + run_id

        response = requests.get(url, auth=(username, password))

        if response.status_code == 200:
            json_data = response.json()

        else:
            print("Hata: İstek başarısız oldu. child joblar çekilirken hata alındı. Hata kodu:", response.status_code)

        df = pd.DataFrame(json_data["data"])
        column_list = df.columns
        if "start_time" not in column_list:
            selected_columns = ["name", "run_id", "status_text", "activation_time"]
        elif "end_time" not in column_list:
            selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time"]
        else:
            selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time", "end_time"]
        print(jobName + " icin getDataFromAliveMaster calisti")


        df[selected_columns].to_sql(stg_table_name, conn_src, if_exists='append', index=False)
    else:
        getDataFromDeadMaster(jobName)
        print(jobName+" icin getDataFromDeadMaster calisti")



def getDataFromAliveSlaves(jobName):
    url=baseurl+"executions?name="+jobName
    response = requests.get(url, auth=(username, password))
    if response.status_code == 200:
        json_data = response.json()

    else:
        print("Hata: İstek başarısız oldu. Main flowun run idsi alınamadı. Hata kodu:", response.status_code)



    df = pd.DataFrame(json_data["data"])
    c_list=df.columns

    if "run_id" in c_list: #eğer df dolu ve run_id getiriyorsa buraya giriyor


        run_id_l=df["run_id"]
        run_id=str(run_id_l[0])
        print(run_id)

        url = baseurl+"executions/"+run_id+"/children"


        response = requests.get(url, auth=(username, password))

        if response.status_code == 200:
            json_data = response.json()

        else:
            print("Hata: İstek başarısız oldu. child joblar çekilirken hata alındı. Hata kodu:", response.status_code)

        df = pd.DataFrame(json_data["data"])
        column_list=df.columns
        if "start_time" not in column_list:
            selected_columns = ["name", "run_id", "status_text", "activation_time"]
        elif "end_time" not in column_list:
            selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time"]
        else:
            selected_columns=["name","run_id","status_text","activation_time","start_time","end_time"]



        df[selected_columns].to_sql(stg_table_name, conn_src, if_exists='append', index=False)
    elif "run_id" not in c_list:
        getDataFromDeadSlaves(jobName)
        print(jobName + " icin getDataFromDeadSlaves calisti")


def getDataFromDeadMaster(jobName):
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
    cursor = conn.cursor()

    query = "SELECT run_id FROM "+hedef_table_name+" where name = '"+jobName+"'"
    cursor.execute(query)
    rows = cursor.fetchall()
    #print(rows[0][0])

    url = baseurl + "executions/" + str(rows[0][0])

    response = requests.get(url, auth=(username, password))

    if response.status_code == 200:
        json_data = response.json()

    else:
        print("Hata: İstek başarısız oldu. child joblar çekilirken hata alındı. Hata kodu:", response.status_code)

    df = pd.DataFrame([json_data])
    column_list = df.columns

    if "start_time" not in column_list:
        selected_columns = ["name", "run_id", "status_text", "activation_time"]
    elif "end_time" not in column_list:
        selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time"]
    else:
        selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time", "end_time"]


    df[selected_columns].to_sql(stg_table_name, conn_src, if_exists='append', index=False)


def getDataFromDeadSlaves(jobName):
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
    cursor = conn.cursor()

    query = "SELECT run_id FROM " + hedef_table_name + " where name = '" + jobName + "'"
    cursor.execute(query)
    rows = cursor.fetchall()
    #print(rows[0][0])

    url = baseurl + "executions/"+str(rows[0][0])+"/children"

    response = requests.get(url, auth=(username, password))

    if response.status_code == 200:
        json_data = response.json()

    else:
        print("Hata: İstek başarısız oldu. child joblar çekilirken hata alındı. Hata kodu:", response.status_code)

    df = pd.DataFrame(json_data["data"])
    column_list = df.columns
    if "start_time" not in column_list:
        selected_columns = ["name", "run_id", "status_text", "activation_time"]
    elif "end_time" not in column_list:
        selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time"]
    else:
        selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time", "end_time"]


    df[selected_columns].to_sql(stg_table_name, conn_src, if_exists='append', index=False)

def IsSlaveCheck(jobName):
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes')
    cursor = conn.cursor()

    query = "SELECT IsSlave FROM " + hedef_table_name + " where name = '" + jobName + "'"
    cursor.execute(query)
    rows = cursor.fetchall()

    return rows[0][0]


def initial_input(x):
    url = baseurl + "executions/" + str(x)

    response = requests.get(url, auth=(username, password))

    if response.status_code == 200:
        json_data = response.json()

    else:
        print("Hata: İstek başarısız oldu. child joblar çekilirken hata alındı. Hata kodu:", response.status_code)

    df = pd.DataFrame([json_data])
    column_list = df.columns
    if "start_time" not in column_list:
        selected_columns = ["name", "run_id", "status_text", "activation_time"]
    elif "end_time" not in column_list:
        selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time"]
    else:
        selected_columns = ["name", "run_id", "status_text", "activation_time", "start_time", "end_time"]
    df[selected_columns].to_sql(stg_table_name, conn_src, if_exists='append', index=False)

for job in joblist:

    if IsSlaveCheck(job)==True:
        getDataFromAliveSlaves(job)
    elif IsSlaveCheck(job)==False:
        getDataFromAliveMaster(job)







conn_src.close()