from typing import Generator
import pyarrow as pa

TimetagRecordSchema = pa.schema([("ch", pa.uint8()), ("timestamp", pa.uint32())])
"""The schema for the timetag records. Usually the channel has 5 bits and the timestamp has 27 bits."""


def iterate_records(table) -> Generator[tuple[int, int], None, None]:
    """Iterate over the records in the table and yield the channel and timestamp.

    @param table: the table to iterate over. The schema must be TimetagRecordSchema."""
    assert table.schema == TimetagRecordSchema
    for batch in table.to_batches():
        d = batch.to_pydict()
        for i in range(batch.num_rows):
            yield d["ch"][i], d["timestamp"][i]  # type: ignore
