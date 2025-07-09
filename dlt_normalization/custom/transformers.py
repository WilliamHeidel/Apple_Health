
from typing import Iterator
import dlt
from dlt.common import json
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem, read_jsonl
import gzip
import ijson

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

@dlt.transformer
def read_top_level_arrays(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        with file_obj.open("rb") as f:
            parser = ijson.parse(f)

            current_key = None
            for prefix, event, value in parser:
                # Start of a new top-level array
                if event == "start_array" and prefix and "." not in prefix:
                    current_key = prefix

                # Items within the array
                elif current_key and prefix.startswith(f"{current_key}.item") and event == "start_map":
                    item = {}
                    for item_prefix, item_event, item_value in parser:
                        if item_event == "end_map":
                            break
                        if item_event in ("string", "number", "boolean", "null"):
                            relative_key = item_prefix.replace(f"{current_key}.item.", "")
                            item[relative_key] = item_value

                    item["source_file"] = file_obj["file_name"]
                    item["source_key"] = current_key
                    yield item

@dlt.transformer(name="foundation_foods_jsonl")
def read_foundation_foods(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        with file_obj.open("rb") as f:
            parser = ijson.items(f, "FoundationFoods.item")
            for food in parser:
                food["source_file"] = file_obj["file_name"]
                yield food

@dlt.transformer(name="survey_foods_jsonl")
def read_survey_foods(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        with file_obj.open("rb") as f:
            parser = ijson.items(f, "SurveyFoods.item")
            for food in parser:
                food["source_file"] = file_obj["file_name"]
                yield food

@dlt.transformer(name="sr_legacy_foods_jsonl")
def read_legacy_foods(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        with file_obj.open("rb") as f:
            parser = ijson.items(f, "SRLegacyFoods.item")
            for food in parser:
                food["source_file"] = file_obj["file_name"]
                yield food

@dlt.transformer(name="branded_foods_jsonl")
def read_branded_foods(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
    for file_obj in items:
        with file_obj.open("rb") as f:
            parser = ijson.items(f, "BrandedFoods.item")
            for food in parser:
                food["source_file"] = file_obj["file_name"]
                yield food
