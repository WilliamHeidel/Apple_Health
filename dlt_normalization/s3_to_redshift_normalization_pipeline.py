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
            data = json.load(f)
            # Single object (dict)
            if isinstance(data, dict):
                data["source_file"] = file_obj['file_name']
                yield data
            # List of records (array of dicts)
            elif isinstance(data, list):
                for row in data:
                    row["source_file"] = file_obj['file_name']
                    yield row


files = filesystem(file_glob="**/*.json.gz") | read_json()

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="apple_health",
        staging="filesystem",
        destination="redshift",
        dataset_name="apple_health",
        progress='enlighten',
    )
    info = pipeline.run(files, refresh='drop_sources')
    print(info)