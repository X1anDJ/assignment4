# lambda-code/cleaner_lambda.py
import os
import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET_NAME = os.environ["BUCKET_NAME"]

s3 = boto3.client("s3", region_name=REGION)


def lambda_handler(event, context):
    # List objects and delete the largest one.
    resp = s3.list_objects_v2(Bucket=BUCKET_NAME)
    contents = resp.get("Contents", [])

    if not contents:
        return {"status": "empty"}
 
    candidates = [
    obj for obj in contents
    if not obj["Key"].startswith("plot")
    ]
    if not candidates:
        candidates = contents

    largest = max(candidates, key=lambda o: o["Size"])

    key = largest["Key"]
    size = largest["Size"]

    s3.delete_object(Bucket=BUCKET_NAME, Key=key)

    return {
        "status": "deleted",
        "deleted_key": key,
        "deleted_size": size,
    }
