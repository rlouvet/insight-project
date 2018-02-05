from flask import render_template, jsonify
from app import app
import random
import os, json
import boto3
import botocore

env_mode = os.getenv('ENV_MODE', 'aws')

@app.route('/')
@app.route('/index')
def index():

    if env_mode == 'aws':

        bucket_name = os.environ['BUCKET_NAME']
        file_name = os.environ['FILE_NAME']

        s3 = boto3.resource('s3')
        try:
            s3.Bucket(bucket_name).download_file(file_name, '/tmp/data.json')
            data = json.load('/tmp/data.json')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
    else:
        file_path = os.environ['FILE_PATH']
        data = json.load(open(file_path))


    return render_template('index.html', title='Home', results=data)
