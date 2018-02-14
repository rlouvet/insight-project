from flask import Flask
app = Flask(__name__)
app.secret_key = 'development key'
from app import views
