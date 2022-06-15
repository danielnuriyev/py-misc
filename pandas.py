import pandas as pd

df = pd.read_json('~/Downloads/test.json', lines=True)

print(df['pcrid'].max())