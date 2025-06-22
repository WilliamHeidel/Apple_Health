from typing import Iterator
import dlt
from dlt.common import json
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem, read_jsonl
import gzip

# Define a standalone transformer to read data from a JSON file.
@dlt.transformer
def read_json(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        with file_obj.open() as f:
            yield json.load(f)

@dlt.transformer
def read_json_gz(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        with file_obj.open("rb") as raw_file:
            with gzip.GzipFile(fileobj=raw_file) as decompressed:
                yield json.load(decompressed)


files = filesystem(file_glob="**/*.json.gz") | read_json() #| reformat

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="apple_health",
        destination="filesystem",
        dataset_name="apple_health",
    )
    info = pipeline.run(files, refresh='drop_sources')
    print(info)