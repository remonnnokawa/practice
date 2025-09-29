import asyncio
from collections import deque
import signal
from datetime import datetime
from pathlib import Path
import argparse

import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
from schema import TimetagRecordSchema

MAX_LEN = 1024 * 32
#「deque（キュー）にためる最大要素数」を表す定数
#maxlen を使うと「古い要素が自動で捨てられる」振る舞いになる
#channels = deque(maxlen=MAX_LEN)
#Deque はどちらの側からも append と pop が可能で、スレッドセーフでメモリ効率がよく、
# どちらの方向からもおよそ O(1) のパフォーマンスで実行できます。 
#O(1) は、アルゴリズムの実行時間が入力のサイズ（データ量）に関わらず一定であることを示します。
local_fs = fs.LocalFileSystem()
#PyArrow には「ファイルシステム」を抽象化した仕組みがあり,
#fs.LocalFileSystem() はその一つで、ローカル PC のファイルにアクセスするためのもの
#同じ書き方で色々なストレージにアクセスできる
TMP_FILE_PATH = "/tmp/timetag_record.dat"
#一時的な IPC ストリーム（RecordBatch の連続ストリーム）を書き込むためのパスを指定しています。
#IPCとはPyArrow では「Arrow の標準バイナリフォーマット」を指し、
# RecordBatchは Arrow で「表データのひとかたまり」を表す単位
#IPC ストリームはその RecordBatch を複数まとめて、連続的にバイナリ形式で保存したもの。
#local_fs.open_output_stream(TMP_FILE_PATH)
#Arrow IPC ストリーム（pa.ipc.new_stream）へバイナリで書き、
# それを最後に読み取って Parquet に変換しています。直接 Parquet に逐次書き出すのと比べてワークフローや
# 実装上の理由で中間ファイルを置くことがあります。

#Apache Parquet は「列指向のデータフォーマット」です。大規模データ解析（Spark, Hive, Pandas, etc.）で事実上の標準。
#"/tmp/..." は UNIX 系で一般的な一時ディレクトリ。Windows や環境によっては存在しない


class RelayServerProtocol(asyncio.DatagramProtocol):
#asyncio.DatagramProtocol は UDP 通信(接続なし、速いけど信頼性低い通信方式)を処理するための基底クラス
#UDP（Datagram）用の通信時の動作を定義するクラス(プロトコルクラス)
#UDP通信で発生する「イベント」に応じて呼ばれる関数➡コールバック（connection_made, datagram_received, error_received, connection_lost）
#をつかってUDPはイベント駆動を処理するので、DatagramProtocol を使う
    transport: asyncio.DatagramTransport | None #初期値は None（接続前）
    #Transport (asyncio) とは,asyncio がソケットの上にかぶせた 抽象レイヤ。
    # ソケットを直接操作せずに、非同期IO・イベント駆動で使えるようにしたもの。
    #Transport（トランスポート）とは、asyncio では 通信の実際の入出力を担当するオブジェクト。
    channels: deque[int] #deque[int] = 「整数を入れる deque」
    timestamps: deque[int]
    #UDP パケットを受け取ると、データを 32bit 整数ごとに解釈して
    #ch = 上位 5bit（チャンネル番号）,timestamp = 下位 27bit（タイムスタンプ）を取り出す
    #: … 「この変数はこういう型ですよ」と宣言する記号
    num_received_bytes: int
    #ネットワークから受信したデータの「合計バイト数」。
    num_packets: int
    #受信した「パケットの数」。
    #これらは通信効率やエラー率を分析するための基礎データになる。
    writer: pa.RecordBatchStreamWriter
    #writer という変数には PyArrow の RecordBatchStreamWriter クラスのインスタンスが入ることを示しています。
    #RecordBatchStreamWriter とはRecordBatch をバイナリストリームに連続的に書き出すためのクラス。
    #pa.ipc.new_stream(sink, schema) で作る。
            #ink = 書き込み先（ファイル、メモリバッファ、ソケットなど）
            # schema = データの構造（列名と型の定義、例: ch: uint8, timestamp: uint32）
    def __init__(self):
        super().__init__() #継承元のクラス（ここでは asyncio.DatagramProtocol）の __init__ を呼び出しています。
        #親クラス側で必要な初期化処理（内部状態のセットアップや属性の初期化）がある場合にそれを実行させるための呼び出しです。
        self.transport = None#UDP の送受信用トランスポートを格納するための属性を初期化しています。
        #connection_made が呼ばれたときに実際の asyncio.DatagramTransport オブジェクトが代入される想定
        self.channels = deque(maxlen=MAX_LEN)#受信した「チャネル番号」を一時的にためておく collections.deque を作成しています。
        self.timestamps = deque(maxlen=MAX_LEN)#受信した「タイムスタンプ」をためる deque です。
        #maxlen を指定しているので、この deque は 固定長のリングバッファ のように振る舞い、要素が maxlen を超えて追加されると左端（最も古い要素）が自動的に破棄されます
        self.num_received_bytes = 0#datagram_received 内で受信パケットごとに len(data) を足し合わせて更新されます。
        self.num_packets = 0
        self.sink = local_fs.open_output_stream(TMP_FILE_PATH) #local_fs  =  pyarrow.fs.LocalFileSystem()
        #open_output_stream(path) は指定パス（ここでは "/tmp/timetag_record.dat"）に書き込み用の出力ストリーム（OutputStream 相当）を作ります。
        self.writer = pa.ipc.new_stream(self.sink, schema=TimetagRecordSchema)
        #RecordBatch IPC ストリーム書き込み用オブジェクト（一般に RecordBatchStreamWriter 相当）を作成して self.writer に保持
        #self.sink（先ほど開いた出力ストリーム）に対して Arrow の IPC ストリームフォーマットで RecordBatch を連続して書き込めるようにする。
        #schema=TimetagRecordSchema には pyarrow.Schema（列の名前と型を定義したオブジェクト）が渡されている想定で、これが書き込む RecordBatch のスキーマになる
        #つまりTimetagRecordSchema自体になにか代入されるものに対してデータの種類を宣言しているわけではない
    def connection_made(self, transport):#asyncio.DatagramProtocol のコールバックメソッド
        #ソケットが生成されて実際の「トランスポート（送受信を司るオブジェクト）」が用意されると asyncio がこのメソッドを呼び出します。
        self.transport = transport #transport はasyncioが渡してくるインスタンス
        #インスタンス変数に保存することでのちのち使えるようになる

    def datagram_received(self, data, addr):#UDPデータグラムを受信したときに asyncio が呼ぶコールバックです
        #data は受信ペイロード（bytes) ,addr は送信元アドレス（通常は (ip, port) のタプル）
        num_bytes = len(data)#受信バイト列の長さを取得して num_bytes に格納
        self.num_packets += 1#受信した UDP パケットの総数をカウントアップ
        print("Received:", num_bytes, "bytes from", addr)#受信通知をコンソールに出力
        for i in range(0, num_bytes, 4):#受信データを 4バイト（32ビット）ずつ に分割して処理するためのループ　送信側は1レコード=4バイトで送ってきている前提のコード
            record = int.from_bytes(data[i : i + 4], byteorder="little")#スライス data[i:i+4]（bytes）を整数に変換します。リトルエンディアン（最下位バイトが先）で解釈することを意味
            ch = record >> 27#record（最大32ビット想定）を右に27ビットシフトすることで、上位5ビット（32 − 27 = 5 ビット）を取り出します
            timestamp = record & 0x7FFFFFF #&はビット単位の AND を取る演算子です。各ビットを比較して 両方が1のときだけ1 になります。
            #record の 下位27ビットだけ残して、上位ビットは全部0にする ことができる
            print(
                f"ch: {ch:2d}, timestamp: {timestamp:9d} | data: {record:32b} | {record:8x}"
            )
            self.channels.append(ch)#チャンネル値を追加
            self.timestamps.append(timestamp)#タイムスタンプ値を timestamps バッファへ追加
            if self.channels.maxlen == len(self.channels):
                self.write_stream() #len(self.channels) == maxlen）になったら write_stream() を呼んでバッファをフラッシュ（RecordBatch にしてファイルへ書き出す）

        self.num_received_bytes += num_bytes

    def write_stream(self):
        if len(self.channels) != 0:
            batch = pa.RecordBatch.from_arrays(
                [pa.array(self.channels), pa.array(self.timestamps)],
                schema=TimetagRecordSchema,
            )
            self.writer.write_batch(batch)
        self.channels.clear()
        self.timestamps.clear()

    def write_parquet(self, file_path: str = "/tmp/timetag_record.parquet"):
        self.write_stream()
        self.writer.close()
        self.writer = None
        self.sink.close()
        table = pa.ipc.open_stream(TMP_FILE_PATH).read_all()
        pq.write_table(table, file_path)

    def close(self):
        self.write_stream()
        if self.writer is not None:
            self.writer.close()
        if not self.sink.closed:
            self.sink.close()
        if self.transport is not None:
            self.transport.close()


def handle_shutdown():
    loop = asyncio.get_running_loop()
    loop.stop()


async def start_server(
    host: str, port: int, result_dir: Path, enable_save_parquet: bool
):
    print("start server on udp://{}:{}".format(host, port))
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_shutdown)
    _transport, protocol = await loop.create_datagram_endpoint(
        RelayServerProtocol, local_addr=(host, port)
    )

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        if enable_save_parquet:
            datetime_str = datetime.now().strftime("%Y-%m-%d-%H%M-%S")
            parquet_file_path = result_dir / f"timetag_record_{datetime_str}.parquet"
            protocol.write_parquet(str(parquet_file_path))
        print("num packets received:", protocol.num_packets)
        print("num bytes received:", protocol.num_received_bytes)
        protocol.close()
        print("server closed")


def main():
    parser = argparse.ArgumentParser(description="Start the relay server.")
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="The host to bind the server to. default: 0.0.0.0",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8089,
        help="The port to bind the server to. default: 8089",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path.cwd() / "results",
        help="The directory to save the parquet files. default: ./results",
    )
    parser.add_argument(
        "--save-log",
        type=bool,
        default=True,
        help="Whether to save the log as parquet format. default: True",
    )
    args = parser.parse_args()
    asyncio.run(start_server(args.host, args.port, args.output, args.save_log))


if __name__ == "__main__":
    main()
