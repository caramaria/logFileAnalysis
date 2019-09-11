import os
import pandas as pd

def add_latency_logs(logs):
    initate_tasklet_logs = logs[logs['log_id'] == 'taskletInitiateRequest']
    tasklet_executed_logs = logs[logs['log_id'] == 'taskletExecutedMessage']

    to_append = None

    tasklet_ids = initate_tasklet_logs['value'].unique()
    for tasklet_id in tasklet_ids:
        # Get first log of tasklet initialisation
        tasklet_initialized_at = get_first_occurrence(initate_tasklet_logs, tasklet_id)
        if tasklet_initialized_at is None:
            raise Exception('Invalid')

        # Get first log of results received for this tasklet
        tasklet_results_received_at = get_first_occurrence(tasklet_executed_logs, tasklet_id)
        if tasklet_results_received_at is not None:
            latency = tasklet_results_received_at - tasklet_initialized_at

            log = {
                'sim_time': tasklet_results_received_at,
                'log_id': 'taskletLatency',
                'caller_entity': 'Custom',
                'caller_id': '-1',
                'target_entity': 'Tasklet',
                'target_id': tasklet_id,
                'value': latency
            }

            if to_append is None:
                to_append = pd.DataFrame(log, index=[0])
            else:
                to_append = to_append.append(pd.DataFrame(log, index=[0]))

    #    logs = logs.append(to_append)
    #    logs.sort_values(by=['sim_time'], inplace=True)
    #    logs.reset_index(inplace=True)
    return to_append

def get_first_occurrence(df, tasklet_id):
    idx = df[df['value'] == tasklet_id].first_valid_index()
    if idx is not None:
        data_point = df.loc[[idx]]
        return data_point['sim_time'].values[0]
    else:
        return None



MESSAGE_LOGS_PREFIX = '_log_'
PARAMETER_LOGS_PREFIX = 'iniParameter_'
LOGS_FILEPATH = '/Users/d060462/Documents/GitHub/Evaluation/finalFiles/final'
FINAL_LOGS_FILEPATH = '/Users/d060462/Documents/GitHub/Evaluation/finalFiles/final_Results/latency'

# log_files = [parameter_filename for parameter_filename in os.listdir(LOGS_FILEPATH) if
#              MESSAGE_LOGS_PREFIX in parameter_filename]
# parameter_files = [parameter_filename for parameter_filename in os.listdir(LOGS_FILEPATH) if
#                    PARAMETER_LOGS_PREFIX in parameter_filename]
# idx = 0

# print(log_files)

# for file in log_files:

filename = 'single_log_1567682124.csv'

message_log = pd.read_csv(os.path.join(LOGS_FILEPATH, filename),
                            sep=',',
                            header=None,
                            names=['sim_time', 'log_id', 'caller_entity', 'caller_id', 'target_entity', 'target_id',
                                    'value'])
message_log_result = add_latency_logs(message_log)
message_log_result.to_csv(os.path.join(FINAL_LOGS_FILEPATH, filename), sep=',')
print("Done with log"+ filename)
print('done')





