# To collect the Statistics regarding Total 'Platform_Id' used, How many machines use which Platform_id.
# Regarding Normalized 'Cpu and Memory Capacity Available', And How many machines have how much Cpu and Memory available.

import pandas as pd

df = pd.read_csv('/home/aman/Documents/Aman/Google_Project/clusterdata-2011-2/machine_events/part-00000-of-00001.csv')
(df.ix[:,2].value_counts().rename('Total_GeneralID')).to_csv('/home/aman/Downloads/kk12.csv')


df = pd.read_csv('/home/aman/Documents/Aman/Google_Project/clusterdata-2011-2/machine_events/part-00000-of-00001.csv')
(df.ix[:,3].value_counts().rename('Total_GeneralID')).to_csv('/home/aman/Downloads/kk12.csv')


df = pd.read_csv('/home/aman/Documents/Aman/Google_Project/clusterdata-2011-2/machine_events/part-00000-of-00001.csv')
(df.ix[:,4].value_counts().rename('Total_GeneralID')).to_csv('/home/aman/Downloads/kk12.csv')


df = pd.read_csv('/home/aman/Documents/Aman/Google_Project/clusterdata-2011-2/machine_events/part-00000-of-00001.csv')
(df.ix[:,5].value_counts().rename('Total_GeneralID')).to_csv('/home/aman/Downloads/kk12.csv')

