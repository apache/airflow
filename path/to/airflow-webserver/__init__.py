from flask import Flask
from .views import init_app

app = Flask(__name__)
init_app(app)