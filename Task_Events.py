# To find the last event timestamp in file named 'part-00499-of-00500.csv.gz' under Task_Events folder.

from pandas import read_csv
from os import path
task_events_csv_colnames = ['time', 'missing', 'job_id', 'task_idx', 'machine_id', 'event_type', 'user', 'sched_cls', 
                            'priority', 'cpu_requested', 'mem_requested', 'disk', 'restriction']         
task_event_df = read_csv(path.join('task_events', 'part-00499-of-00500.csv.gz'), header=None, index_col=False, 
                         compression='gzip', names=task_events_csv_colnames)

x= (task_event_df['time'])
print(x[-1])

# Ouptput :-  2506199602822

# Now, we can generate some 200 random sample of moments from which to collect statistics, primarily requested utilization, 
# as well as a single moment in time for which to capture all of the information available about 
# running tasks at that moment for further analysis.

from random import randint, sample, seed
seed(83)
sample_moments = sorted(sample(range(250619902823), 200))
snapshot_moment = randint(0, 250619902822)
print (snapshot_moment)

Output:- 182602390144

# 

from collections import OrderedDict
import os
from pandas import DataFrame

tasks_dict = {}
samples_dicts = OrderedDict([])
sample_moments_iterator = iter(sample_moments)
current_sample_moment = next(sample_moments_iterator)
tasks_df = None


for fn in sorted(os.listdir('/home/aman/Documents/Aman/Google_Project/clusterdata-2011-2/task_events')):
    
    fp = os.path.join('/home/aman/Documents/Aman/Google_Project/clusterdata-2011-2/task_events', fn)
    task_events_df = pd.read_csv(fp, header=None, index_col=False, compression='gzip', 
                              names=task_events_csv_colnames)
    
    for index, event in task_events_df.iterrows():
        
        if current_sample_moment is not None and event['time'] > current_sample_moment:
            tmp_tasks_df = DataFrame(list(tasks_dict.values()))
            samples_dicts[current_sample_moment] = ({'time' : current_sample_moment, 
                                                     'cpu_requested' : sum(tmp_tasks_df['cpu_requested']), 
                                                     'mem_requested' : sum(tmp_tasks_df['mem_requested'])})
            try:
                current_sample_moment = next(sample_moments_iterator)
            except StopIteration:
                current_sample_moment = None
                
        if tasks_df is None and event['time'] > snapshot_moment:
            tasks_df = DataFrame(list(tasks_dict.values()))
            
        if event['event_type'] in [0, 7, 8]:
            tasks_dict[(event['job_id'], event['task_idx'])] = {'task_id' : (event['job_id'], event['task_idx']),
                                                                'machine_id' : event['machine_id'],
                                                                'cpu_requested' : event['cpu_requested'], 
                                                                'mem_requested' : event['mem_requested']}
        elif event['event_type'] in [2, 3, 4, 5, 6]:
            del tasks_dict[(event['job_id'], event['task_idx'])]
            
    if tasks_df is not None and current_sample_moment is None:
        break
                    
samples_df = DataFrame(list(samples_dicts.values()))

# Plot Graph between Memory Requested and Cpu Requested.

import matplotlib.pyplot as plt
fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(samples_df['time'], samples_df['cpu_requested'], label='cpu requested')
ax.plot(samples_df['time'], samples_df['mem_requested'], label='mem requested')
plt.xlim(min(samples_df['time']), max(samples_df['time']))
plt.legend()
plt.show()
