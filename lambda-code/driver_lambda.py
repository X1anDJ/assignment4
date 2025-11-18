import os
import time
import urllib.request
import boto3

REGION = "us-east-1"
BUCKET = os.environ["BUCKET_NAME"]          
# TABLE  = os.environ["TABLE_NAME"]
PLOTTING_API_URL = os.environ["PLOTTING_API_URL"]

s3 = boto3.client("s3", region_name=REGION)

def _sleep():
    time.sleep(1.5)

def lambda_handler(event, context):
    s3.put_object(Bucket=BUCKET, Key="assignment1.txt", Body=b"Empty Assignment 1")
    _sleep()

    s3.put_object(Bucket=BUCKET, Key="assignment1.txt", Body=b"Empty Assignment 2222222222")
    _sleep()

    s3.delete_object(Bucket=BUCKET, Key="assignment1.txt")
    _sleep()
    
    s3.put_object(Bucket=BUCKET, Key="assignment2.txt", Body=b"33")
    _sleep()

    with urllib.request.urlopen(PLOTTING_API_URL) as resp:
        body = resp.read().decode("utf-8")

    return {"status": "ok", "plotting_response": body}
