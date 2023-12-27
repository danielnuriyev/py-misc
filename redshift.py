import redshift_connector

conn = redshift_connector.connect(
     host='',
     port=5439,
     database='dev',
     user='',
     password=''
  )

cursor = conn.cursor()
cursor.execute("select * from ... limit 10")
result = cursor.fetchall()
print(result)

print("done")
