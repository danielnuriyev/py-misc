import redshift_connector

conn = redshift_connector.connect(
     host='<name>.<account>.us-east-1.redshift-serverless.amazonaws.com',
     port=5439,
     database='<db name>',
     user='<user>',
     password='<password>'
  )

# cursor = conn.cursor()
# cursor.execute("select * from ... limit 10")
# result = cursor.fetchall()
# print(result)

print("done")
