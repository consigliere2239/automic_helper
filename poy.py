import sqlalchemy as sa
import pandas as pd
import time
import datetime
from datetime import timedelta


def insert_data_into_target():
global df_tgt
global time_tgt
server_tgt, port_tgt, db_tgt, schema_tgt, tbl_tgt, driver_tgt = 'MASKED', '1433', 'MASKED', \
'dbo', 'MASKED', \
'MASKED'
string_tgt = 'MASKED' + server_tgt + ':' + port_tgt + '/' + db_tgt + '?' + driver_tgt
engine_tgt = sa.create_engine(string_tgt)
conn_tgt = engine_tgt.connect()
print("--> SQL Server baglantisi basarili")
time.sleep(1)
start_tgt = time.time() # okuma performansı
print("--> Hedef tabloya veri yaziliyor : " + "'" + db_tgt + "." + schema_tgt + "." + tbl_tgt + "'")
# df = pd.read_sql_query('select top 10000 * from ' + db_tgt + '.' + schema_tgt + '.' + tbl_tgt + '', con=conn_tgt)
df_src.to_sql(name=tbl_tgt, con=conn_tgt, schema=schema_tgt, if_exists='replace', index=False)
end_tgt = time.time()
print('--> Aktarim tamamlandi')
time.sleep(1)
time_tgt = timedelta(seconds=end_tgt - start_tgt)
print('--> ' + 'Yazma Süresi: ' + str(timedelta(seconds=end_tgt - start_tgt)))
time.sleep(1)
print('')
print('[ ' + 'Toplam Süre: ' + str(time_tgt) + ' ]')
# return df_tgt, time_tgt


#------------------------------


def read_data_from_source():
global df_src
global time_src
server_src, port_src, db_src, schema_src, tbl_src, driver_src = 'MASKED', '1433', 'MASKED', \
'dbo', 'MASKED', \
'MASKED'
string_src = 'mssql+pyodbc://@' + server_src + ':' + port_src + '/' + db_src + '?' + driver_src
engine_src = sa.create_engine(string_src)
conn_src = engine_src.connect()
print('Baslangic Saati: ' + str(datetime.datetime.now()))
print('--> SqlServer baglantisi basarili')
time.sleep(1)
start_src = time.time()
print('--> Kaynak tablo okunuyor : ' + "'" + db_src + "." + schema_src + "." + tbl_src + "'")
df_src = pd.read_sql_query('select * from ' + db_src + '.' + schema_src + '.' + tbl_src + '', con=conn_src)
end_src = time.time()
print('--> Okuma basarili')
time.sleep(1)
time_src = timedelta(seconds=end_src - start_src)
print('--> ' + 'Okuma Süresi: ' + str(timedelta(seconds=end_src - start_src)))
print('')
return df_src, time_src