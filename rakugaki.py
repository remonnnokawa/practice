birth_date = list(input().split())
print(birth_date)
match birth_date:
    case ['daisuke_takeuchi', '2_27', *rest]:
        print(f"あっているよ {rest}")
    case [nobady , 6_30 ,*rest]:
        print(f"それは捨てたろ")
    case _:
        print("invalid value")


def checkok(attempts = 4 , warning ="try again"):
    while True:
        reply = input()
        if reply == "yes":
            return True
        if reply == "no":
            return False
        if attempts < 0:
            raise ValueError("no more attempts")
        else:
            print(warning)
            attempts = attempts - 1
checkok()

def f(a,L=None):
    if L is None:
        L = []
    L.append(a)
    return a

f(input())