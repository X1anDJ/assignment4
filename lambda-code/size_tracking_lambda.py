# lambda-code/size_tracking_lambda.py
import os
import time
import json
import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET_NAME = os.environ["BUCKET_NAME"]
TABLE_NAME = os.environ["TABLE_NAME"]

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)


def _compute_bucket_totals(bucket: str):
    """List all objects and compute total size + count."""
    total_size = 0
    count = 0
    token = None

    while True:
        kwargs = {"Bucket": bucket}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            # optional: ignore the plot object if you don't want it to affect totals
            # if key == "plot":
            #     continue
            total_size += obj["Size"]
            count += 1

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return total_size, count


def lambda_handler(event, context):
    # event from SQS → body contains SNS → Message is S3 event
    buckets = set()

    for record in event.get("Records", []):
        body = json.loads(record["body"])
        sns_msg = json.loads(body["Message"])
        for s3rec in sns_msg.get("Records", []):
            bkt = s3rec["s3"]["bucket"]["name"]
            buckets.add(bkt)

    results = []
    for bucket in buckets:
        total_size, count = _compute_bucket_totals(bucket)
        now_ms = int(time.time() * 1000)

        ddb.put_item(
            TableName=TABLE_NAME,
            Item={
                "bucket": {"S": bucket},
                "timestamp": {"N": str(now_ms)},
                "size_bytes": {"N": str(total_size)},
                "object_count": {"N": str(count)},
            },
        )
        results.append(
            {"bucket": bucket, "size_bytes": total_size, "object_count": count}
        )

    return {"status": "ok", "results": results}
