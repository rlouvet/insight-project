#!/bin/bash
sudo apt-get update
sudo apt-get upgrade
sudo apt-get install python-pip python-dev build-essential
sudo pip install flask tornado boto3 flask-wtf
