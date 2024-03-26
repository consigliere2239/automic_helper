import pymsteams
import pyodbc
import requests

server = 'MASKED'
database = 'MASKED'



webhook_url='MASKED'
#Automic_Helper
myTeamsMessage = pymsteams.connectorcard(webhook_url)

conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
cursor = conn.cursor()

query2 = "SELECT abrevation,end_time FROM MASKED where messageWorthy = 1 and startFlag = 1 and endFlag = 0 and status_text = 'MASKED'"
cursor.execute(query2)

rows2 = cursor.fetchall()

for row in rows2:
    myTeamsMessage.text(str(row[0])+' jobi basariyla saat '+str(row[1])+' itibariyle bitmistir.')
    myTeamsMessage.send()

query = "SELECT abrevation,start_time FROM MASKED where messageWorthy = 1 and startFlag = 0 and status_text = 'MASKED'"
cursor.execute(query)

rows = cursor.fetchall()

for row in rows:
    myTeamsMessage.text(str(row[0])+' jobi '+str(row[1])+ ' itibariyle baslamistir.')
    myTeamsMessage.send()





