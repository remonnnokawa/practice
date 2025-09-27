point = int(input())
match point:
    case 227:
        print(f"{point} is my birthday")
    case _:
        print("""fxxkin no way 
I know that date""")

points = [30,40,50,60,70]
match points:
    case [x,y,*rest]:
        print(f"x = {x}, y = {y}, rest = {rest}")
    case _:
        pass
