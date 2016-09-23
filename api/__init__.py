from flask import Flask

DB_CONNECTION = "host='localhost' port=5432 user='postgres' password='secret' dbname='antismash'"
SQLALCHEMY_DATABASE_URI = 'postgres://postgres:secret@localhost:5432/antismash'

app = Flask(__name__)
app.config.from_object(__name__)

from .models import db

db.init_app(app)

from . import api
from . import error_handlers
