#matchの使い方
def http_error(status):
    match status:
        case 400:
            return "Bad request"
        case 404:
            return "Not found"
        case 418:
            return "I'm a teapot"
        #| は or 
        case 401 | 403:
            return "Not allowed"
        #matchにおいてワイルドカード_は最後に置く
        case _:
            return "Something's wrong with the internet"
a = http_error(int(input()))
print(a)

class Point:
    __match_args__ = ('x', 'y')
    def __init__(self, x, y):
        self.x = x
        self.y = y

#パターンに if 節を追加できます。これは "ガード" と呼ばれます。
# ガードがfalseの場合、match は次のcaseブロックの処理に移動します。ガードを評価する前に値が取り出されることに注意してください:


match Point:
    case Point(x, y) if x == y:
        print(f"Y=X at {x}")
    case Point(x, y):
        print(f"対角線上ではない")

#match 文でタプルやリストの形をそのままパターンに書くと、アンパック代入 と同じ動きをする。
#*rest を使えば「残りの要素をまとめる」ことができる。
def check(data):
    match data:
        case [x, y, *rest]:
            return f"x={x}, y={y}, 残り={rest}"
        case (x, y, *rest):
            return f"(tuple) x={x}, y={y}, 残り={rest}"
        case _:
            return "その他"

print(check([1, 2, 3, 4, 5]))   # x=1, y=2, 残り=[3, 4, 5]
print(check((10, 20, 30)))      # (tuple) x=10, y=20, 残り=[30]

def fib(n):    # nまでのフィボナッチ数を出力する
    """nまでのフィボナッチ数をprintする。"""
    a, b = 0, 1
    while a < n:
        print(a, end=' ')
        a, b = b, a+b
    print() #改行処理