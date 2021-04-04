import pandas as pd
import json
from pprint import pprint

url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
df = pd.read_csv(url)
df = df.to_json(orient='index', indent=4)
# df = json.loads(df)
# df = json.dumps(df)
df = pd.read_json(df)

pprint(type(df))
pprint(df)