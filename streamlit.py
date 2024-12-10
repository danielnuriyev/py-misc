import streamlit as st

from pyathena import connect

import pandas as pd
import numpy as np

conn = connect(
    s3_staging_dir='s3://.../',
    region_name='us-east-1',
    work_group='...'
)
df = pd.read_sql("SELECT * FROM ...", conn)

st.dataframe(df)

chart_data = pd.DataFrame(
     np.random.randn(20, 3),
     columns=['a', 'b', 'c'])

st.line_chart(chart_data)