# lambda-code/driver_lambda.py
import os
import time
import urllib.request
import boto3

REGION = "us-east-1"
BUCKET = os.environ["BUCKET_NAME"]
PLOTTING_API_URL = os.environ["PLOTTING_API_URL"]

s3 = boto3.client("s3", region_name=REGION)


def _sleep():
    time.sleep(2)


def lambda_handler(event, context):
    s3.put_object(
        Bucket=BUCKET,
        Key="assignment1.txt",
        Body=b"Empty Assignment 1",
    )
    _sleep()

    # 28 bytes
    s3.put_object(
        Bucket=BUCKET,
        Key="assignment2.txt",
        Body=b"Empty Assignment 2222222222",
    )
    _sleep()

    s3.put_object(
        Bucket=BUCKET,
        Key="assignment3.txt",
        Body=b"33",
    )
    time.sleep(5)
    # 4) Call plotting API
    with urllib.request.urlopen(PLOTTING_API_URL) as resp:
        body = resp.read().decode("utf-8")

    return {"status": "ok", "plotting_response": body}
