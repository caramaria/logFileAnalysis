import pandas as pd
import os

CSV_PATH ='/Users/d060462/Documents/Private/University/final/'
filename= 'resultTest.csv'
nameTag =['sim_time', 'log_id', 'caller_entity', 'caller_id', 'target_entity', 'target_id', 'value']


df = message_log = pd.read_csv(os.path.join(CSV_PATH,filename), sep=',')

df.dropna(subset=["value"],axis=0, inplace=True)
#df['value']=df['value'].astype(string).astype(float)
df[' value'] = pd.to_numeric(df['value'],errors='coerce')
print(df['value'].mean())


