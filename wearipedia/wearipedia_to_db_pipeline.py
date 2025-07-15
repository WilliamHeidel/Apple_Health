from typing import Iterator
import os
import dlt
from dlt.common import json
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem, read_jsonl


dailySummary_files = filesystem(file_glob="**/cronometer/**/*dailySummary*.jsonl*")
dailySummary_resource = dlt.resource(dailySummary_files | read_jsonl(), parallelized=True).with_name("dailySummary")

servings_files = filesystem(file_glob="**/cronometer/**/*servings*.jsonl*")
servings_resource = dlt.resource(servings_files | read_jsonl(), parallelized=True).with_name("servings")

exercises_files = filesystem(file_glob="**/cronometer/**/*exercises*.jsonl*")
exercises_resource = dlt.resource(exercises_files | read_jsonl(), parallelized=True).with_name("exercises")

biometrics_files = filesystem(file_glob="**/cronometer/**/*biometrics*.jsonl*")
biometrics_resource = dlt.resource(biometrics_files | read_jsonl(), parallelized=True).with_name("biometrics")


pipeline_to_redshift = dlt.pipeline(
    pipeline_name="cronometer_data_redshift",
    staging="filesystem",
    destination="redshift",
    dataset_name="cronometer_raw_data",
    progress='enlighten',
)


if __name__ == "__main__":

    pipeline = pipeline_to_redshift
    resources = [dailySummary_resource, servings_resource, exercises_resource, biometrics_resource]
    loader_file_format = 'parquet'

    info = pipeline.run(resources
        ,refresh='drop_sources'
        ,loader_file_format=loader_file_format
    )
    print(info)
