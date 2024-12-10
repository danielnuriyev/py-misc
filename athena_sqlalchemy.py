import pandas as pd
from urllib.parse import quote_plus
from pyathena import connect
from sqlalchemy.engine import create_engine

region = "us-east-1"
schema = "..."
results = "s3://.../"

conn_str = (
    "awsathena+rest://athena.{region}.amazonaws.com:443/"
    "?s3_staging_dir={s3_staging_dir}&work_group=..."
)

engine = create_engine(
    conn_str.format(
        region=region,
        # schema=schema,
        s3_staging_dir=quote_plus(results),
    )
)
conn = engine.connect()

df = pd.read_sql_query("SELECT * FROM ...", conn)
print(df)