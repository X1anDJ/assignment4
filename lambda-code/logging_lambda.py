# lambda-code/logging_lambda.py
import json
import os
from urllib.parse import unquote_plus

import boto3

logs_client = boto3.client("logs")

LOG_GROUP_NAME = os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME")


def _find_last_size_from_logs(object_name: str) -> int:
    """
    For delete events, S3 does not give us size.
    We search this log group for the last log for this object_name
    and use its (absolute) size_delta as the size.
    """
    if not LOG_GROUP_NAME:
        return 0

    # Simple JSON filter on object_name
    pattern = f'{{ $.object_name = "{object_name}" }}'

    resp = logs_client.filter_log_events(
        logGroupName=LOG_GROUP_NAME,
        filterPattern=pattern,
        limit=5,  # small for this assignment
    )

    events = resp.get("events", [])
    if not events:
        return 0

    # Take the last event (latest)
    for ev in reversed(events):
        try:
            msg = json.loads(ev["message"])
            delta = int(msg.get("size_delta", 0))
            return abs(delta)
        except Exception:
            continue

    return 0


def lambda_handler(event, context):
    # event from SQS → SNS → S3
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        sns_msg = json.loads(body["Message"])
        for s3rec in sns_msg.get("Records", []):
            event_name = s3rec["eventName"]  # e.g. ObjectCreated:Put
            bucket_name = s3rec["s3"]["bucket"]["name"]
            key_enc = s3rec["s3"]["object"]["key"]
            key = unquote_plus(key_enc)

            if event_name.startswith("ObjectCreated"):
                size = int(s3rec["s3"]["object"].get("size", 0))
                size_delta = size
            elif event_name.startswith("ObjectRemoved"):
                size = _find_last_size_from_logs(key)
                size_delta = -size
            else:
                # ignore other events
                continue

            log_entry = {
                "bucket": bucket_name,
                "object_name": key,
                "size_delta": size_delta,
            }

            # JSON log line – metric filter will read $.size_delta
            print(json.dumps(log_entry))

    return {"status": "ok"}
