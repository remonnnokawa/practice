import pandas as pd
import matplotlib.pyplot as plt

# データ読み込み
df = pd.read_csv("C:/Users/mikis/OneDrive/デスクトップ/python3.13/a/moonshot/2025-09-20_13.12.01-GPX2_results3-rst_times.csv")

# チャンネルごとに抽出
cols_ch1 = ['t_stop1_ps', 't_stop1_ns', 'ovf_stop1', 'refno_unwrapped_stop1', 'STOP1', 'REFNO1']
cols_ch4 = ['t_stop4_ps', 't_stop4_ns', 'ovf_stop4', 'refno_unwrapped_stop4', 'STOP4', 'REFNO4']

df_ch1 = df[cols_ch1].copy()
df_ch4 = df[cols_ch4].copy()

# パラメータ
max_time_diff_psec = 2_000_000   # 2 μs
time_bin_psec = 10_000           # 10 ns bin

# 時間差を全部計算（全探索）
time_diffs = []
for _, record_ch1 in df_ch1.iterrows():
    for _, record_ch4 in df_ch4.iterrows():
        time_diff = record_ch4['t_stop4_ps'] - record_ch1['t_stop1_ps']
        if 0 <= time_diff <= max_time_diff_psec:
            time_diffs.append(time_diff)

# ヒストグラム表示
plt.hist(time_diffs, bins=range(0, max_time_diff_psec, time_bin_psec))
plt.xlabel("Time difference (ps)")
plt.ylabel("Counts")
plt.title("Histogram of CH4 detection after CH1")
plt.show()