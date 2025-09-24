#split()
a = "10,20,30".split(",")
print(a)
x = "10 20 30".split()
print(x)
#数値として変換したいとき
b = list(map(int,"10,20,30".split(",")))
print(b)
#test