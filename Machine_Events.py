# To collect information about machine events. By looking in the 'schema.csv' file, we know that the csv files in 
# this directory have five fields: time, machine ID, event type, platform ID, cpu capacity, and memory capacity.
# The event type is an integer, 0, 1, 2, with values meaning that a machine has been ADDED (0), REMOVED (1), or UPDATED (2).


from collections import OrderedDict
import os
from pandas import DataFrame
import pandas as pd

machines_dict = {}
sample_moments_iterator = iter(sample_moments)
current_sample_moment = next(sample_moments_iterator)
machines_df = None

machine_events_csv_colnames=['time', 'machine_id', 'event_type', 'platform_id', 'cpu', 'mem']

for fn in sorted(os.listdir('/home/aman/Documents/Aman/Google_Project/clusterdata-2011-2/machine_events')):
    fp = os.path.join('/home/aman/Documents/Aman/Google_Project/clusterdata-2011-2/machine_events', fn)
    machine_events_df = pd.read_csv(fp, header=None, index_col=False, compression='gzip', names=machine_events_csv_colnames)
    for index, event in machine_events_df.iterrows():
        
        if current_sample_moment is not None and event['time'] > current_sample_moment:
            tmp_machines_df = DataFrame(list(machines_dict.values()))
            samples_dicts[current_sample_moment].update({'cpu_available' : sum(tmp_machines_df['cpu']), 
                                                         'mem_available' : sum(tmp_machines_df['mem'])})
            try:
                current_sample_moment = next(sample_moments_iterator)
            except StopIteration:
                current_sample_moment = None
                
        if machines_df is None and event['time'] > snapshot_moment: 
            machines_df = DataFrame(list(machines_dict.values()))
            
        if event['event_type'] in [0, 2]:
            machines_dict[event['machine_id']] = {'machine_id' : event['machine_id'], 
                                                  'cpu' : event['cpu'], 'mem' : event['mem']}
        elif event['event_type'] in [1]:
            del machines_dict[event['machine_id']]

    if machines_df is not None and current_sample_moment is None:
        break
                    
machines_df = DataFrame(list(machines_dict.values()))
samples_df = DataFrame(list(samples_dicts.values()))


# To plot the graph Between Cpu Available, Cpu Requested, Memory Available, Memory Requested. 

fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(samples_df['time'], samples_df['cpu_requested'], label='cpu requested')
ax.plot(samples_df['time'], samples_df['cpu_available'], label='cpu available')
ax.plot(samples_df['time'], samples_df['mem_requested'], label='mem requested')
ax.plot(samples_df['time'], samples_df['mem_available'], label='mem available')
plt.xlim(min(samples_df['time']), max(samples_df['time']))
plt.legend()
plt.show()

# So we can analyze that, Cpu and Memory Requested varies up and down among tasks that
# have been submitted to the system, availablity remains fairly constant. 
# Demand for memory and cpu stay above availability, with some notable dips for memory.
