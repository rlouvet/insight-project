import os
import boto3
import botocore

bucket_name = os.environ['BUCKET_NAME']

def list_files():

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    exists = True
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            exists = False

    bucket_objects_list = bucket.objects.all() if exists else None
    return bucket_objects_list

def download_file(file_name):

    s3 = boto3.resource('s3')
    try:
        s3.Bucket(bucket_name).download_file(file_name, '/tmp/data.json')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
