import os
import time
import json
import boto3

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET_NAME = os.environ["BUCKET_NAME"]      # optional
TABLE_NAME = os.environ["TABLE_NAME"]

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(TABLE_NAME)


def _compute_bucket_totals(bucket: str):
    total = 0
    count = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            # skip plot objects so they don't affect totals
            if key in ("plot", "plot.png", "plot.jpeg"):
                continue
            total += obj["Size"]
            count += 1

    return total, count


def lambda_handler(event, context):
    results = []

    # Process *all* SQS records in the batch
    for sqs_rec in event.get("Records", []):
        # SQS body -> SNS message
        body_str = sqs_rec.get("body", "{}")
        try:
            body = json.loads(body_str)
        except json.JSONDecodeError:
            # Not JSON, skip
            continue

        msg_str = body.get("Message", "{}")
        try:
            sns_msg = json.loads(msg_str)
        except json.JSONDecodeError:
            # Not an S3 event, skip
            continue

        records = sns_msg.get("Records")
        if not records:
            continue

        # There can be multiple S3 records in one SNS message
        for s3_rec in records:
            bucket = s3_rec["s3"]["bucket"]["name"]

            total, count = _compute_bucket_totals(bucket)
            now_ms = int(time.time() * 1000)

            item = {
                "bucket": bucket,
                "timestamp": now_ms,
                "size_bytes": total,
                "object_count": count,
            }

            table.put_item(Item=item)
            results.append(item)

    return {
        "status": "ok",
        "results": results,
    }
