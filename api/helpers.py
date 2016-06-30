import psycopg2
import psycopg2.extras

from flask import g
from api import app


###################
# Database access #
###################

def connect_db():
    '''Connect to the database'''
    connection = psycopg2.connect(app.config['DB_CONNECTION'], cursor_factory=psycopg2.extras.NamedTupleCursor)
    return connection


def get_db():
    '''Open new DB connection if current context doesn't have one already'''
    if not hasattr(g, 'db_conn'):
        g.db_conn = connect_db()
    return g.db_conn


@app.teardown_appcontext
def close_db(error):
    '''Close the database'''
    if hasattr(g, 'db_conn'):
        g.db_conn.close()
