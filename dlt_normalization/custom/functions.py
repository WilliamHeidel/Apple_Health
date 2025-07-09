
def rename_jsonl_to_gz():
    """
    Rename all .jsonl files in the specified S3 bucket and prefix to .jsonl.gz.
    """

    BUCKET = "billy-heidel-test-bucket"
    PREFIXES = [
        "USDA_FoodCentral/Normalized/usda_raw_data/",
        "USDA_FoodCentral/Normalized/usda_raw_data_large/"
    ]
    NEW_PREFIX = "USDA_FoodCentral/Normalized/usda_data_cleaned/"
    
    import boto3
    s3 = boto3.client("s3")

    for PREFIX in PREFIXES:
        # List objects with .jsonl
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
        objects = response.get("Contents", [])

        for obj in objects:
            key = obj["Key"]
            print(f"Processing key: {key}")
            if ("_dlt" not in key) and (key.endswith(".jsonl") or key.endswith(".parquet") or key.endswith(".json") or key.endswith(".csv")):
                new_key = key.replace(PREFIX,NEW_PREFIX) + ".gz"
                print(f"Renaming: {key} -> {new_key}")
                
                # Copy to new key
                s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=new_key)
                
                # Delete old key
                #s3.delete_object(Bucket=BUCKET, Key=key)


if __name__ == "__main__":
    rename_jsonl_to_gz()
    print("Renaming completed.")
