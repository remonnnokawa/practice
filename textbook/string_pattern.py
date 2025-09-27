#文字列リテラルは複数行にまたがって書けます。1 つの方法は三連引用符 ("""...""" や '''...''') を使うことです。
# 改行文字は自動的に文字列に含まれますが、行末に \ を付けることで含めないようにすることもできます。
# 次の例では最初の改行は含まれません:
print("""\
Usage: thingy [OPTIONS]
-h                        Display this usage message
-H hostname               Hostname to connect to
        """)
#表示する回数を指定
print(2 *"ai")