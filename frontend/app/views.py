from flask import render_template, jsonify, request
from app import app
from app.forms import target_page_list
import random
import os, json
import boto3
import botocore
import awshelper

@app.route('/')
def index():
    bucket_objects_list = awshelper.list_files()
    processed_object_list = [
    element.key.rsplit("/", 1)[0] for element in bucket_objects_list if '.json' in element.key
    ]
    return render_template('index.html', batch_runs=processed_object_list)


@app.route('/run/', methods=['GET'])
def run():
    run_id = request.args.get('run_id')
    target_page = int(request.args.get('target_page'))
    form = target_page_list.TargetPageListForm()
    if target_page:
        form.target_page.default = target_page
        form.process()

    bucket_objects_list = awshelper.list_files()
    filtered_list = [
        element.key for element in bucket_objects_list
        if run_id == element.key.rsplit("/", 1)[0]
        and '.json' in element.key.rsplit("/", 1)[1]
    ]
    awshelper.download_file(filtered_list[0])


    data = []
    for line in open('/tmp/data.json', 'r'):
        data.append(json.loads(line))

    sankey_data = get_sankey(data, target_page)

    return render_template('results.html', run_id=run_id, results=data, sankey_data=sankey_data, form=form)

def get_sankey(data, target_page):
    output_data = []
    for record in data:
        path = record['value']
        if len(path) > 0:
            sink = path[-1]
            if sink == target_page:
                if len(path) == 1:
                    output_record = ['self', str(sink), record['count']]
                else:
                    output_record = [str(path[:-1]), str(sink), record['count']]

                output_data.append(output_record)

    return output_data
