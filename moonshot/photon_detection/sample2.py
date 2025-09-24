from pathlib import Path

from japanize_matplotlib import japanize
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import os
from tqdm import tqdm
#スクリプトファイルがある場所をカレントディレクトリにする
script_dir = Path(__file__).parent
os.chdir(script_dir)


sns.set_style('whitegrid')
japanize()

path_here = Path().cwd()
print(path_here)

#ファイル参照
target_file = path_here / '2025-09-20_13.12.01-GPX2_results3-rst_times.csv'

#csvファイル確認
df = pd.read_csv(target_file)
print(df.head())

cols_ch1 = ['t_stop1_ps', 't_stop1_ns', 'ovf_stop1', 'refno_unwrapped_stop1', 'STOP1', 'REFNO1']
cols_ch4 = ['t_stop4_ps', 't_stop4_ns', 'ovf_stop4', 'refno_unwrapped_stop4', 'STOP4', 'REFNO4']

#データフレームを分ける
df_ch1 = df[cols_ch1].copy()
df_ch4 = df[cols_ch4].copy()

print(df_ch1.head())
print(df_ch4.head())

max_time_diff_psec = 3000000

all_time_diffs = list()
for _idx1, record_ch1 in df_ch1.iterrows():
    time_diffs = list()
    #行ごとの処理だからiterrows()
    for _idx4, record_ch4 in df_ch4.iterrows():
        time_diff = record_ch4['t_stop4_ps'] - record_ch1['t_stop1_ps']
        if time_diff < 0 or time_diff > max_time_diff_psec:
            continue #スキップ
        time_diffs.append(
            {
                'time_diff': time_diff,
                't_stop4_ps': record_ch4['t_stop4_ps'],
                't_stop1_ps': record_ch1['t_stop1_ps'],
            }
        )
        #データフレームに変更
    df_time_diff = pd.DataFrame(time_diffs)
    all_time_diffs.append(df_time_diff)
df_all_time_diff = pd.concat(all_time_diffs,ignore_index=True)
print(df_all_time_diff)

time_bin_psec = 10000

min_time_diff = df_all_time_diff['time_diff'].min()
max_time_diff = df_all_time_diff['time_diff'].max()

dts = np.arange(min_time_diff, max_time_diff, time_bin_psec)
print(f'最小: {min_time_diff}, 最大: {max_time_diff}, 幅: {time_bin_psec}')
print(dts)

counter = {int(dt): 0 for dt in dts}

for _idx, record in tqdm(df_all_time_diff.iterrows(), total=len(df_all_time_diff), leave=True):
    for dt in reversed(counter.keys()):
        if record['time_diff'] >= dt:
            counter[dt] += 1
            break
sr_coincedence = pd.Series(counter)
print(counter)

sr_coincedence = pd.Series(counter)

# ヒストグラム描画
plt.figure(figsize=(8,5))
plt.bar(sr_coincedence.index, sr_coincedence.values, width=time_bin_psec*0.9, color='skyblue', edgecolor='black')
plt.xlabel('ch1 → ch4 時間差 [psec]')
plt.ylabel('検出回数')
plt.title('ch1からch4までの時間差ヒストグラム')
plt.grid(True, axis='y', linestyle='--', alpha=0.5)
plt.show()