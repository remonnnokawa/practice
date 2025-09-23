#map関数
sample_list = list(range(5))
print(list(map(lambda y : y*2,sample_list)))
#for構文
sample2_list = []
for i in sample_list:
    sample2_list.append(i*2)

print(sample2_list)
#リスト包括表式
sample2_list = [i*2 for i in sample_list]
print(sample2_list)