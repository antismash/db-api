from flask import Flask

DB_CONNECTION = "host='localhost' port=15432 user='postgres' password='secret' dbname='antismash'"

app = Flask(__name__)
app.config.from_object(__name__)

from . import api
from . import error_handlers
