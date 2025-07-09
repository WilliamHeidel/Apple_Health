from typing import Iterator
import os
import dlt
from dlt.common import json
from dlt.common.storages.fsspec_filesystem import FileItemDict
from dlt.common.typing import TDataItems
from dlt.sources.filesystem import filesystem, read_jsonl
import gzip
import ijson
import custom.transformers as transformers
from custom.resources import foundation_foods_custom_resource
import custom.functions as cf

PIPELINE = 'redshift'

foundation_files = filesystem(file_glob="**/foundation_foods/*.json")
foundation_foods_resource = dlt.resource(foundation_files | transformers.read_foundation_foods(), parallelized=True)

survey_foods = filesystem(file_glob="**/FNDDS_survey_foods/*.json")
survey_foods_resource = dlt.resource(survey_foods | transformers.read_survey_foods(), parallelized=True)

sr_legacy_foods = filesystem(file_glob="**/sr_legacy_food/*.json")
sr_legacy_foods_resource = dlt.resource(sr_legacy_foods | transformers.read_legacy_foods(), parallelized=True)

branded_foods = filesystem(file_glob="**/branded_foods/*.json")
branded_foods_resource = dlt.resource(branded_foods | transformers.read_branded_foods(), parallelized=True)
branded_foods_resource = dlt.resource(branded_foods | transformers.read_json(), parallelized=True).with_name("branded_foods_json")

pipeline_to_s3 = dlt.pipeline(
    pipeline_name="usda_raw_data",
    destination="filesystem",
    dataset_name="usda_raw_data",
    progress='enlighten',
)

pipeline_to_redshift = dlt.pipeline(
    pipeline_name="usda_raw_data_redshift",
    staging="filesystem",
    destination="redshift",
    dataset_name="usda_raw_data_rs",
    progress='enlighten',
)

pipeline_to_s3_large = dlt.pipeline(
    pipeline_name="usda_raw_data_large",
    destination="filesystem",
    dataset_name="usda_raw_data_large",
    progress='enlighten',
)

if __name__ == "__main__":

    if PIPELINE == 'redshift':
        pipeline = pipeline_to_redshift
        resources = [foundation_foods_resource, survey_foods_resource, sr_legacy_foods_resource]
        #resources = survey_foods_resource
        loader_file_format = 'jsonl'
    else:
        pipeline = pipeline_to_s3
        resources = [foundation_foods_resource, survey_foods_resource, sr_legacy_foods_resource] #+ [branded_foods_resource]
        #resources += [branded_foods_resource]
        loader_file_format = 'jsonl'

        #os.environ["NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION"] = "false"
        info_large = pipeline_to_s3_large.run(branded_foods_resource
            ,refresh='drop_sources'
            ,loader_file_format='parquet'
        )
        #os.environ["NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION"] = "true"


    info = pipeline.run(resources
        ,refresh='drop_sources'
        ,loader_file_format=loader_file_format
    )
    print(info)

    if PIPELINE != 'redshift':
        # Rename files in S3 bucket
        cf.rename_jsonl_to_gz()
