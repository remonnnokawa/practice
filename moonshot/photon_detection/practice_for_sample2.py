import os 
from pathlib import Path
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from tqdm import tqdm
from japanize_matplotlib import japanize
#__file__ は現在実行中のスクリプトファイルのパス。
#.parent でそのファイルがあるディレクトリを取得。
script_dir = Path(__file__).parent
#カレントワーキングディレクトリを script_dir に変更する。
os.chdir(script_dir)

#seaborn の描画スタイルを指定。
#'whitegrid' は背景が白でグリッド線が入る設定。他にも "darkgrid", "white", "ticks" などがある。
sns.set_style('whitegrid')
#matplotlib で日本語フォントを使えるようにする。
japanize()
#カレントディレクトリの取得
path_here = Path().cwd()
print(path_here)

#csvファイル参照
target_file = path_here / '2025-09-20_13.12.01-GPX2_results3-rst_times.csv'

df = pd.read_csv(target_file)
print(df.head())

#ファイルを分割
cols_ch1 = ['t_stop1_ps' , 't_stop1_ns' , 'ovf_stop1' , 'refno_unwrapped_stop1','STOP1', 'REFNO1']
cols_ch4 = ['t_stop4_ps', 't_stop4_ns', 'ovf_stop4', 'refno_unwrapped_stop4', 'STOP4', 'REFNO4']
df_ch1 = df[cols_ch1].copy()
df_ch4 = df[cols_ch4].copy()

print(df_ch1.head())
print(df_ch4.head())

#検出時間差の最大値指定
max_time_diffs = 3000000
#データ入力用にリストを作っておく
all_time_diffs = list()
time_diffs = list()
#iterrows() → DataFrame を1行ずつ (index, Series) の形で返す
for _idx1 , record_ch1 in df_ch1.iterrows():
    for _idx4 , record_ch4 in df_ch4.iterrows():
        time_diff = record_ch4['t_stop4_ps'] - record_ch1['t_stop1_ps']
        if time_diff < 0 or time_diff > max_time_diffs:
            continue
        time_diffs.append(
            {
                'time_diff': time_diff,
                't_stop4_ps': record_ch4['t_stop4_ps'],
                't_stop1_ps': record_ch1['t_stop1_ps']
            }
        )
    df_time_diffs = pd.DataFrame(time_diffs)
    all_time_diffs.append(df_time_diffs)
df_all_time_diffs = pd.concat(all_time_diffs,ignore_index= True)
print(df_all_time_diffs.head())
#time_diffs … dict のリスト

#df_time_diffs … その dict リストを変換した DataFrame

#all_time_diffs … 複数の DataFrame を一時的に溜めるリスト

#pd.concat(all_time_diffs) … DataFrame のリストをまとめて 1 つの DataFrame に

time_bin_psec = 10000

min_time_diff = df_all_time_diffs['time_diff'].min()
max_time_diff = df_all_time_diffs['time_diff'].max()
#range()だと扱えるのは整数のみ、返し値はrangeオブジェクト
#np.arrange()なら浮動小数点も使える、返し値がNumpy配列でヒストグラムにつかいやすい
dts = np.arrange(min_time_diff, max_time_diff, time_bin_psec)
print(f'最小: {min_time_diff}, 最大: {max_time_diff}, 幅: {time_bin_psec}')

counter = {int(dt): 0 for dt in dts}

for _idx , record in tqdm(df_all_time_diffs.iterrows(),total=len(df_all_time_diffs),leave = True):
    for dt in reversed(counter.key()):
        if record['time_diff'] >= dt:
            #dict[key]はそのキーに対応する値を意味する
            counter[dt] += 1
            break
sr_coincidence = pd.Series(counter)
