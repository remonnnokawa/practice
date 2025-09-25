import os 
from pathlib import Path
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from tqdm import tqdm
from japanize_matplotlib import japanize

script_dir = Path(__file__).parent
os.chdir(script_dir)

sns.set_style('whitegrid')
japanize()

path_here = Path().cwd()
target_data = path_here/'2025-09-20_13.12.01-GPX2_results3-rst_times.csv'
df = pd.read_csv(target_data)
print(df.head())

cols_ch1 = ['t_stop1_ps' , 't_stop1_ns' , 'ovf_stop1' , 'refno_unwrapped_stop1','STOP1', 'REFNO1']
cols_ch4 = ['t_stop4_ps', 't_stop4_ns', 'ovf_stop4', 'refno_unwrapped_stop4', 'STOP4', 'REFNO4']

df_ch1 = df[cols_ch1].copy()
df_ch4 = df[cols_ch4].copy()

max_diff_psec = 3000000
time_diffs = list()
for idx1 , record_ch1 in df_ch1.iterrows():
    for idx4 , record_ch4 in df_ch4.iterrows():
        time_diff = record_ch4['t_stop4_ps'] - record_ch1['t_stop1_ps']
        time_diffs.append(
            {
                'time_diff' : time_diff ,
                't_stop4_ps' : record_ch4['t_stop4_ps'] ,
                't_stop1_ps' : record_ch1['t_stop1_ps']
            }
        )
df_all_time_diffs = pd.DataFrame(time_diffs)
print(df_all_time_diffs)