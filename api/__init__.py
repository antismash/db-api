import os
import time
from flask import Flask, g, request
from flask_cors import CORS

SQLALCHEMY_DATABASE_URI = os.getenv('AS_DB_URI', 'postgres://postgres:secret@localhost:5432/antismash')
SQLALCHEMY_TRACK_MODIFICATIONS = False

app = Flask(__name__)
app.config.from_object(__name__)
CORS(app)

from .models import db

db.init_app(app)


@app.before_request
def start_timer():
    g.start = time.time()

@app.after_request
def log_request(response):
    now = time.time()
    duration = round(now - g.start, 2)

    log_params = [
        ('method', request.method),
        ('path', request.path),
        ('status', response.status_code),
        ('duration', duration),
    ]

    line = " ".join(["{}={}".format(name, value) for name, value in log_params])
    app.logger.info(line)

    return response


from . import api
from . import error_handlers
