#関数のデフォルト引数に関して
#この場合1つだけ引数を渡せばOK。もちろん複数の引数も可能。
def ask_ok(prompt, retries=4, reminder='再試行してください!'):
    while True:
        reply = input(prompt)
        if reply in {'y', 'ye', 'yes'}:
            return True
        if reply in {'n', 'no', 'nop', 'nope'}:
            return False
        retries = retries - 1
        if retries < 0:
            raise ValueError('無効なユーザーの入力')
        print(reminder)

#デフォルト引数は「関数を呼び出したとき」ではなく、「関数が定義された瞬間」に一度だけ評価される
i = 5 
def f(arg=i): #この時点で i は 5 だから、arg のデフォルト値は 5 に固定される。
    print(arg) 
i = 6 
f()

#関数の呼び出しについて
def f(a, L=[]):
    L.append(a)
    return L

print(f(1))
print(f(2))
print(f(3))
#Lのデフォルト引数は関数が定義されたときに一度だけ評価される。
#上の関数は、後に続く関数呼び出しで関数に渡されている引数を累積します

#もし蓄積させたくなければ以下のようにする
def f(a, L=None):
    if L is None:
        L = []
    L.append(a)
    return L
