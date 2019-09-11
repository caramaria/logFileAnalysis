import pandas as pd
import os


def calcMean(logs):
    print("i Got called")
    df = logs
    df.dropna(subset=["value"],axis=0, inplace=True)
    #df['value']=df['value'].astype(string).astype(float)
    df[' value'] = pd.to_numeric(df['value'],errors='coerce')
    print(df['value'].mean())
    mean = df['value'].mean()
    Data = {'Average': [mean]}
    df = pd.DataFrame(Data)
    return df



MESSAGE_LOGS_PREFIX = '_log_'
PARAMETER_LOGS_PREFIX = 'iniParameter_'
LOGS_FILEPATH = '/Users/d060462/Documents/GitHub/Python/20s_runs_Result'
FINAL_LOGS_FILEPATH = '/Users/d060462/Documents/GitHub/Python/20s_runs_KPIca'

log_files = [parameter_filename for parameter_filename in os.listdir(LOGS_FILEPATH) if
             MESSAGE_LOGS_PREFIX in parameter_filename]
parameter_files = [parameter_filename for parameter_filename in os.listdir(LOGS_FILEPATH) if
                   PARAMETER_LOGS_PREFIX in parameter_filename]
idx = 0

print(log_files)

for file in log_files:

    filename = os.fsdecode(file)

    message_log = pd.read_csv(os.path.join(LOGS_FILEPATH, filename),
                              sep=',')
    message_log_result = calcMean(message_log)
    message_log_result.to_csv(os.path.join(FINAL_LOGS_FILEPATH, filename), sep=',')
    print("Done with log"+ filename)



print('done')