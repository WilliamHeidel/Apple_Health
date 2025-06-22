from typing import Iterator
import dlt
from dlt.common import json
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem, read_jsonl
import gzip
    
@dlt.transformer()
def reformat(items: Iterator[FileItemDict]):
   for item in items:
      yield dlt.mark.with_file_import(
          item["file_url"],
          format="jsonl"
      )

# Define a standalone transformer to read data from a JSON file.
@dlt.transformer
def read_json(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        print(file_obj)
        with file_obj.open() as f:
            yield json.load(f)


files = filesystem(file_glob="**/*.jsonl") | read_jsonl() #| reformat

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="apple_health",
        destination="filesystem",
        dataset_name="apple_health",
    )
    info = pipeline.run(files)
    print(info)