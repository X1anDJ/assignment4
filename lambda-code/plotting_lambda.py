# lambda-code/plotting_lambda.py
import io
import time
import json
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer

os.environ.setdefault("MPLCONFIGDIR", "/tmp/mplconfig")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Read environment variables injected by CDK
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ["BUCKET_NAME"]
TABLE = os.environ["TABLE_NAME"]
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "20"))

s3 = boto3.client("s3", region_name=REGION)
db = boto3.client("dynamodb", region_name=REGION)
_deser = TypeDeserializer()


def _ddb_to_py(item):
    return {k: _deser.deserialize(v) for k, v in item.items()}


def _query_last_seconds(bucket: str, seconds: int):
    now_ms = int(time.time() * 1000)
    start = now_ms - seconds * 1000
    resp = db.query(
        TableName=TABLE,
        ExpressionAttributeNames={"#ts": "timestamp", "#b": "bucket"},
        ExpressionAttributeValues={
            ":b": {"S": bucket},
            ":from": {"N": str(start)},
            ":to": {"N": str(now_ms)},
        },
        KeyConditionExpression="#b = :b AND #ts BETWEEN :from AND :to",
        ProjectionExpression="#b, #ts, size_bytes, object_count",
        ScanIndexForward=True,
    )
    return [_ddb_to_py(i) for i in resp.get("Items", [])]


def _query_all_for_bucket(bucket: str):
    items, eks = [], None
    while True:
        kwargs = {
            "TableName": TABLE,
            "ExpressionAttributeNames": {"#ts": "timestamp", "#b": "bucket"},
            "ExpressionAttributeValues": {":b": {"S": bucket}},
            "KeyConditionExpression": "#b = :b",
            "ProjectionExpression": "#b, #ts, size_bytes",
            "ScanIndexForward": True,
        }
        if eks:
            kwargs["ExclusiveStartKey"] = eks
        resp = db.query(**kwargs)
        items.extend(resp.get("Items", []))
        eks = resp.get("LastEvaluatedKey")
        if not eks:
            break
    return [_ddb_to_py(i) for i in items]


def lambda_handler(event, context):
    bucket = BUCKET
    last = _query_last_seconds(bucket, seconds=WINDOW_SECONDS)
    history = _query_all_for_bucket(bucket)
    if not last:
        return {"statusCode": 200, "body": json.dumps({"msg": "No data yet"})}

    raw_seconds = [int(i["timestamp"]) / 1000.0 for i in last]
    t0 = raw_seconds[0]
    xs = [t - t0 for t in raw_seconds]
    ys = [int(i["size_bytes"]) for i in last]
    max_ever = max((int(i["size_bytes"]) for i in history), default=0)

    fig, ax = plt.subplots()
    ax.plot(xs, ys, marker="o")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("Bucket size (bytes)")
    ax.set_title(bucket)
    ax.axhline(ymax := max(ys), linestyle="dashed", label="Historical high")
    ax.legend()

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    plt.close(fig)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key="plot", Body=buf.getvalue(), ContentType="image/png")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "bucket": bucket,
                "plot_key": "plot",
                "max_ever": max_ever,
                "points_last_window": len(xs),
                "window_seconds": WINDOW_SECONDS,
            }
        ),
    }
