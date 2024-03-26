import time
import string
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import io
import sqlalchemy as sa
import pandas as pd
import datetime
from datetime import timedelta
import re
import pyodbc
import pymsteams
import os
global Summary

def search_large_integers(file_name,flag=1):
    matching_lines = [] 
    run_ids=[]
    with open(file_name, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            try:
                number = int(line.strip())  
                if number > 99999999: 
                    matching_lines.append(line_number)
                    run_ids.append(number)
                    
            except ValueError:
                continue  
    if flag==1:
        return matching_lines 
    if flag==0:
        return run_ids

def search_dates_in_file(file_name):
    matching_lines = []  
    date_pattern = r'\b\d{2}/\d{2}/\d{4}\b'  
    with open(file_name, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            matches = re.findall(date_pattern, line)  
            if matches:
                matching_lines.append(line_number)  
                print(f'Eşleşen tarihler: {matches} - Satır: {line_number}')

    return matching_lines
def tarihleri_bul(dosya_adi, numline=9999999):
    tarihler = []

    with open(dosya_adi, 'r') as dosya:
        satirlar = dosya.readlines()

        for i, satir in enumerate(satirlar[:numline]):
            
            pattern = r"\b\d{2}/\d{2}/\d{4}\b"
            tarihler_satir = re.findall(pattern, satir)

            if tarihler_satir:
                tarihler.append(i + 1)

    return tarihler


def read_data_from_source(jobName):
    global df_src
    global time_src
    server_src, port_src, db_src, schema_src, tbl_src, driver_src = 'MASKED', '1433', 'MASKED', \
    'dbo', 'MASKED', \
    'MASKED'
    string_src = 'MASKED' + server_src + ':' + port_src + '/' + db_src + '?' + driver_src
    engine_src = sa.create_engine(string_src)
    conn_src = engine_src.connect()

   
    time.sleep(1)

    
    df_src = pd.read_sql_query('select * from ' + db_src + '.' + schema_src + '.' + tbl_src + ' where job_name = '+"'" +jobName+"'", con=conn_src)

    
    return df_src.iloc[0][1]

def read_url_from_source(jobName):
    global df_src
    global time_src
    server_src, port_src, db_src, schema_src, tbl_src, driver_src = 'MASKED', '1433', 'MASKED', \
    'dbo', 'MASKED', \
    'MASKED'
    string_src = 'MASKED' + server_src + ':' + port_src + '/' + db_src + '?' + driver_src
    engine_src = sa.create_engine(string_src)
    conn_src = engine_src.connect()

    
    time.sleep(1)

    
    df_src = pd.read_sql_query('select * from ' + db_src + '.' + schema_src + '.' + tbl_src + ' where job_name = '+"'" +jobName+"'", con=conn_src)

    print('')
    return df_src.iloc[0][2]


def get_numline_status(dosya_adi, satir_numarasi):
    try:
        with open(dosya_adi, 'r') as dosya:
            satirlar = dosya.readlines()
            if 1 <= satir_numarasi <= len(satirlar):
                satir = satirlar[satir_numarasi - 1]
                return(satir_numarasi)
            else:
                print("Geçersiz satır numarası.")
    except IOError:
        print("Dosya açılırken bir hata oluştu!")
def satiri_yazdir(dosya_adi, satir_numarasi):
    try:
        with open(dosya_adi, 'r') as dosya:
            satirlar = dosya.readlines()
            if 1 <= satir_numarasi <= len(satirlar):
                satir = satirlar[satir_numarasi - 1]
                return satir.strip()
            else:
                print("Geçersiz satır numarası.")
    except IOError:
        print("Dosya açılırken bir hata oluştu!")

def metni_dosyada_ara(dosya_adi, aranan_metin):
    try:
        bulunan_satirlar = []
        with open(dosya_adi, 'r') as dosya:
            for satir_numarasi, satir in enumerate(dosya, start=1):
                if aranan_metin in satir:
                    bulunan_satirlar.append((satir_numarasi, satir.strip()))
        if bulunan_satirlar:
            return bulunan_satirlar
        else:
            return f"Aranan metin '{aranan_metin}' dosyada bulunamadı."
    except IOError:
        return "Dosya açılırken bir hata oluştu!"


def write_to_txt(icerik, dosya_adi):
    try:
        with io.open(dosya_adi, 'w', encoding='utf-8') as dosya:
            dosya.write(icerik)
        
    except IOError:
        print("Dosya açılırken bir hata oluştu!")
def main (job_name,Summary=0):
    global son
    son=""
    name = "MASKED"
    department = "MASKED"
    password = "MASKED"

    
    driver = webdriver.Chrome()

    
    url=str(read_url_from_source(job_name))
    
    driver.get(url)


    
    wait = WebDriverWait(driver, 10)

    username_field = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id=\"awi-97011\"]/div/div[2]/div/div/div/div[1]/div/div[3]/div/div[7]/div/input')))
    username_field.send_keys(name)

    department_field = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id=\"awi-97011\"]/div/div[2]/div/div/div/div[1]/div/div[3]/div/div[9]/div/input')))
    department_field.send_keys(department)

    password_field = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id=\"awi-97011\"]/div/div[2]/div/div/div/div[1]/div/div[3]/div/div[11]/div/div/div/input')))
    password_field.send_keys(password)

    click_login = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id=\"awi-97011\"]/div/div[2]/div/div/div/div[1]/div/div[6]/div/div[5]/div/div/div/div/span/span')))
    click_login.click()

    
    end_time=wait.until(EC.presence_of_element_located((By.XPATH,"//*[@id=\"awi-97011\"]/div/div[2]/div[3]/div/div[1]/div/div/div/div[2]/div/div/div/div/div/div[2]/div[1]/table/tbody/tr[1]/td[1]")))


   
    page_source = driver.page_source

    
    soup = BeautifulSoup(page_source, 'html.parser')

    


    
    icerik = soup.prettify()
    
    global dosya_adi
    dosya_adi = "gdu_check.txt"

    write_to_txt(icerik, dosya_adi)


    

    y = tarihleri_bul(dosya_adi)
    z = search_large_integers(dosya_adi)
    
    last_update = satiri_yazdir(dosya_adi,y[0])

    y.pop(0)
    y_counter = 0
    z_counter = 0
    ilk_eleman = 0
    smart_merge = []

    while len(y) > 0 and len(z) > 0:
        min_length = min(len(y), len(z))
        while ilk_eleman < min_length and z[ilk_eleman] < y[ilk_eleman]:
            if y_counter % 3 == 0:
                smart_merge.append(z[ilk_eleman])
                z.pop(ilk_eleman)
                z_counter += 1
                min_length -= 1
            else:
                smart_merge.append(0)
                y_counter += 1
        while ilk_eleman < min_length and y[ilk_eleman] < z[ilk_eleman]:
            if z_counter % 2 == 0:
                smart_merge.append(y[ilk_eleman])
                y.pop(ilk_eleman)
                y_counter += 1
                min_length -= 1
            else:
                smart_merge.append(0)
                z_counter += 1

    for i in range(len(y)):
        smart_merge.append(y[i])
    

    while len(smart_merge)%5!=0:
        smart_merge.append(0)

    df_int = pd.DataFrame(columns=['RunId', 'ParentID', 'Activation_Time', 'Start_Time', 'End_Time'])
    for i in range(0, len(smart_merge), 5):
        row = smart_merge[i:i + 5]
        df_int.loc[len(df_int)] = row

    if Summary==0:
        son_x=5
    if Summary==1:
        son_x=1
    son += job_name + f"<br> Jobının son {son_x} çalışması aşağıdaki gibidir. <br> "
    row_num = df_int.shape[0]
    column_num = df_int.shape[1]
    for i in range(son_x): 
        for j in range(2,column_num):
            if j==2:
                son+=("<br>Activation Time : ")
            elif j==3:
                son += ("<br>Start Time : ")
            elif j==4:
                son += ("<br>End Time : ")
            deger=df_int.iloc[i, j]
            if  deger!=0:
                son += satiri_yazdir(dosya_adi, df_int.iloc[i, j])
            else:
                son +="null"
            j+=1

        son+="<br> "
        i+=1
    if Summary==0:
        son +="-----------------------------<br>"+ last_update
        

    return son





