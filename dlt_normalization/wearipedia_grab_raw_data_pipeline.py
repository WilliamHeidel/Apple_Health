
import wearipedia
import pandas as pd
from dotenv import load_dotenv
import datetime
import boto3
import io
import gzip
import os
import json
import re

load_dotenv('.env/.env')
BUCKET_NAME = os.environ.get("WEARIPEDIA_S3_BUCKET_NAME")
PREFIX = "wearipedia/cronometer/"
email_address = os.getenv(f'CRONOMETER_EMAIL')
password = os.getenv(f'CRONOMETER_PASSWORD')

bucket = BUCKET_NAME
prefix = PREFIX

start_date='2025-07-13' #'2025-04-23' is first start date for logging. #@param {type:"string"}
end_date=(datetime.datetime.today()- datetime.timedelta(days=1)).strftime('%Y-%m-%d') #@param {type:"string"}
synthetic = False #@param {type:"boolean"}

# Get Device
device = wearipedia.get_device("cronometer/cronometer")

if not synthetic:
    device.authenticate({"username": email_address, "password": password})

def list_s3_filenames(bucket_name: str, folder_prefix: str, region="us-east-2"):
    s3 = boto3.client("s3", region_name=region)
    
    paginator = s3.get_paginator("list_objects_v2")
    result = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix):
        for obj in page.get("Contents", []):
            result.append(obj["Key"])

    return result

def extract_unique_dates_from_keys(keys: list[str]) -> set[str]:
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    dates = set()

    for key in keys:
        match = date_pattern.search(key)
        if match:
            dates.add(match.group(0))

    return dates

def get_missing_dates(start_date: str, end_date: str, existing_dates: list[str]) -> list[str]:
    # Convert to datetime
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    existing = set(existing_dates)

    # Generate full date range
    all_dates = {
        (start + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end - start).days + 1)
    }

    # Find dates in range not in existing list
    missing = sorted(all_dates - existing)
    return missing

def upload_dicts_as_gzipped_jsonl_to_s3(records: list[dict], data_type: str, end_date: str, bucket: str, prefix: str, region: str = "us-east-2"):
    buffer = io.BytesIO()

    # Write DataFrame to GZIP CSV in memory
    with gzip.GzipFile(fileobj=buffer, mode="w") as gz:
        for item in records:
            line = json.dumps(item) + "\n"
            gz.write(line.encode('utf-8'))

    buffer.seek(0)

    # Create timestamped filename
    filename = f"{data_type}_{end_date}.jsonl.gz"
    key = os.path.join(f"{prefix}{end_date.replace('-','/')}", filename)

    # Upload to S3
    s3 = boto3.client("s3", region_name=region)
    s3.upload_fileobj(buffer, bucket, key)

    print(f"Uploaded {filename} to S3 bucket.")

if __name__ == "__main__":
    files = list_s3_filenames(bucket, prefix)
    existing_dates = extract_unique_dates_from_keys(files)

    missing_dates = get_missing_dates(start_date, end_date, existing_dates)

    for date in missing_dates:
        params = {"start_date": date, "end_date": date}
        datasets = ['dailySummary', 'servings', 'exercises', 'biometrics']
        for dataset in datasets:
            data = device.get_data("dailySummary", params=params)
            upload_dicts_as_gzipped_jsonl_to_s3(
                data,
                data_type=dataset,
                end_date=date,
                bucket=BUCKET_NAME,
                prefix=PREFIX
            )
    print(f"Missing dates processed: {missing_dates}")
