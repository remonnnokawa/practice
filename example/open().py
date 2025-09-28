#'rb'は読み込みをバイナリデータのまま行う、'wb'は読み込みを(ry
fr = open('circle_o.png', 'rb')
fw = open('circle_c.png', 'wb')

while True:
#read(size)で引数でした分のバイト数を読み込む。今回はfrから１バイトずつ読み込む
    data = fr.read(1)
    if len(data) == 0:
        break
    #write()で入力
    fw.write(data)

fw.close()
fr.close()

#PyArrowはファイルシステムを抽象化（ローカル・クラウド関係なく同じAPIで使える）できるので
#open()ではなくそっちをつかって有用になる場合がある
