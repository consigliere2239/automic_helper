import sqlite3
conn = sqlite3.connect('veritabani.db')
cursor = conn.cursor()

cursor.execute("SELECT * FROM MASKED")
rows = cursor.fetchall()

for row in rows:
    print("job_name:", row[0])
    print("parentID:", row[1])
    print("url:", row[2])
    print("MainFlow:", row[3])
    print("prevParentID:", row[4])
    print("abbrevation:", row[5])

"""
str_constant="INSERT INTO MASKED ([job_name], [parentID], [url], [MainFlow], [prevParentID], [abbrevation]) VALUES "
MASKED
cursor.execute(MASKED+MASKED)
conn.commit()
cursor.execute(MASKED+MASKED)
conn.commit()
cursor.execute(MASKED+MASKED)
conn.commit()
cursor.execute(MASKED+MASKED)
conn.commit()
cursor.execute(str_constant+MASKED)
conn.commit()
"""


conn.close()