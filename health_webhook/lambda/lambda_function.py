import json
import os
import boto3
from datetime import datetime
import uuid
import gzip
import io

s3 = boto3.client("s3")
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

def lambda_handler(event, context):
    headers = event.get("headers", {})
    if headers.get("x-api-key") != os.environ.get("EXPECTED_API_KEY"):
        return {
            "statusCode": 401,
            "body": json.dumps({"error": "Unauthorized"})
        }

    try:
        body = json.loads(event["body"])
        now = datetime.utcnow()
        unique_id = str(uuid.uuid4())
        filename = f"health_data/raw/{now.year}/{now.month:02}/{now.day:02}/{now.isoformat()}_{unique_id}.json"

        json_data = json.dumps(body).encode("utf-8")
        buffer = io.BytesIO()

        with gzip.GzipFile(fileobj=buffer, mode="w") as gz:
            gz.write(json_data)

        buffer.seek(0)

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename + ".gz",
            Body=buffer,
            ContentType="application/json",
            ContentEncoding="gzip"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Saved to {filename}.gz"})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
