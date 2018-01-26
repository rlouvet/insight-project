from flask import render_template, jsonify
from app import app
import random


@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html', title='Home')
