from pyathena import connect
import pandas as pd
conn = connect(
    s3_staging_dir='s3://.../',
    region_name='us-east-1',
    work_group='...'
)
df = pd.read_sql("SELECT * FROM ...", conn)
print(df)