from flask import render_template, jsonify, request
from app import app
import random
import os, json
import boto3
import botocore
import awshelper

@app.route('/')
def index():
    bucket_objects_list = awshelper.list_files()
    processed_object_list = [
    element.key.split("/")[0] for element in bucket_objects_list if '.json' in element.key
    ]
    return render_template('index.html', batch_runs=processed_object_list)


@app.route('/run/<string:run_id>')
def run(run_id):
    bucket_objects_list = awshelper.list_files()
    filtered_list = [
        element.key for element in bucket_objects_list
        if run_id == element.key.split("/")[0]
        and '.json' in element.key.split("/")[1]
    ]
    awshelper.download_file(filtered_list[0])

    data = []
    for line in open('/tmp/data.json', 'r'):
        data.append(json.loads(line))

    return render_template('results.html', run_id=run_id, results=data)
