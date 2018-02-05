#!/bin/bash
make install && make dev
source myenv
python manage.py initdb
python manage.py runserver
