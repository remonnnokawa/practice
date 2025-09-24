a= list(map(int,input()))
b = 0
def determination(x):
    global b
    if x == 1:
        b += 1
#mapの処理を実行する子はlist()を使わないといけない
list(map(determination,a))
print(b)


#別解
s = input()
print(s.count("1"))