import asyncio
from collections import deque
import signal
from datetime import datetime
from pathlib import Path
import argparse

import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
from timetagger_proxy.schema import TimetagRecordSchema

MAX_LEN = 1024 * 32
local_fs = fs.LocalFileSystem()
TMP_FILE_PATH = "/tmp/timetag_record.dat"


class RelayServerProtocol(asyncio.DatagramProtocol):
    transport: asyncio.DatagramTransport | None
    channels: deque[int]
    timestamps: deque[int]
    num_received_bytes: int
    num_packets: int
    writer: pa.RecordBatchStreamWriter

    def __init__(self):
        super().__init__()
        self.transport = None
        self.channels = deque(maxlen=MAX_LEN)
        self.timestamps = deque(maxlen=MAX_LEN)
        self.num_received_bytes = 0
        self.num_packets = 0
        self.sink = local_fs.open_output_stream(TMP_FILE_PATH)
        self.writer = pa.ipc.new_stream(self.sink, schema=TimetagRecordSchema)

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        num_bytes = len(data)
        self.num_packets += 1
        print("Received:", num_bytes, "bytes from", addr)
        for i in range(0, num_bytes, 4):
            record = int.from_bytes(data[i : i + 4], byteorder="little")
            ch = record >> 27
            timestamp = record & 0x7FFFFFF
            print(
                f"ch: {ch:2d}, timestamp: {timestamp:9d} | data: {record:32b} | {record:8x}"
            )
            self.channels.append(ch)
            self.timestamps.append(timestamp)
            if self.channels.maxlen == len(self.channels):
                self.write_stream()

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
