import dlt
import ijson
from dlt.sources.filesystem import filesystem
from dlt.common.typing import TDataItems
from typing import Iterator, Set

@dlt.resource(name="foundation_foods")
def foundation_foods_custom_resource() -> Iterator[TDataItems]:
    seen_keys: Set[str] = set()

    for file_obj in filesystem(file_glob="**/foundation_foods/*.json"):
        with file_obj.open("rb") as f:
            parser = ijson.parse(f)
            top_level_array_key = None

            for prefix, event, value in parser:
                if event == "start_array" and "." not in prefix:
                    top_level_array_key = prefix
                    continue

                if top_level_array_key and prefix.startswith(f"{top_level_array_key}.item") and event == "start_map":
                    item = {}
                    for item_prefix, item_event, item_value in parser:
                        if item_event == "end_map":
                            break
                        if item_event in ("string", "number", "boolean", "null"):
                            relative_key = item_prefix.replace(f"{top_level_array_key}.item.", "")
                            item[relative_key] = item_value

                    item["source_file"] = file_obj["file_name"]
                    item["source_key"] = top_level_array_key

                    # Dynamically align schema with all seen keys
                    missing_keys = seen_keys - item.keys()
                    for key in missing_keys:
                        item[key] = None

                    # Yield the item before updating seen_keys
                    yield item

                    # Update seen_keys for next items
                    seen_keys.update(item.keys())

@dlt.resource(name="FNDDS_survey_foods")
def foundation_foods_custom_resource() -> Iterator[TDataItems]:
    seen_keys: Set[str] = set()

    for file_obj in filesystem(file_glob="**/FNDDS_survey_foods/*.json"):
        with file_obj.open("rb") as f:
            parser = ijson.parse(f)
            top_level_array_key = None

            for prefix, event, value in parser:
                if event == "start_array" and "." not in prefix:
                    top_level_array_key = prefix
                    continue

                if top_level_array_key and prefix.startswith(f"{top_level_array_key}.item") and event == "start_map":
                    item = {}
                    for item_prefix, item_event, item_value in parser:
                        if item_event == "end_map":
                            break
                        if item_event in ("string", "number", "boolean", "null"):
                            relative_key = item_prefix.replace(f"{top_level_array_key}.item.", "")
                            item[relative_key] = item_value

                    item["source_file"] = file_obj["file_name"]
                    item["source_key"] = top_level_array_key

                    # Dynamically align schema with all seen keys
                    missing_keys = seen_keys - item.keys()
                    for key in missing_keys:
                        item[key] = None

                    # Yield the item before updating seen_keys
                    yield item

                    # Update seen_keys for next items
                    seen_keys.update(item.keys())
