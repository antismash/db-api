import os
from flask import Flask

SQLALCHEMY_DATABASE_URI = os.getenv('AS_DB_URI', 'postgres://postgres:secret@localhost:5432/antismash')

app = Flask(__name__)
app.config.from_object(__name__)

from .models import db

db.init_app(app)

from . import api
from . import error_handlers
