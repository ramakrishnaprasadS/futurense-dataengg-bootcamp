import pandas as pd
import mysql.connector as mycon

hostname='localhost'
db='healthcare'
username='root'
pwd='Srkp@1811'
port_id=3308
cur=None
conn=None

try:
    conn=mycon.connect(
    host=hostname,
    database=db,
    user=username,
    password=pwd,
    port=port_id)

    cur=conn.cursor()

    print("Hello User ...Welcome to Patient enquiry desk\n 1.search patient with patientid\n2.Add new patient\n 3.delete the patient\n4.update patient dob\n");
    
    cur.execute('select * from patient limit 10')                  # fetching records
    for each_record in cur.fetchall():
        print(each_record)
    
    conn.commit()

except Exception as error:
    print(error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        print("Connection is closed...Thank you")
        conn.close()





