import pytest
from api import app as flask_app
from api.models import db as _db


@pytest.fixture(scope='session')
def app(request):
    '''Flask application for test'''
    flask_app.config['TESTING'] = True
    ctx = flask_app.app_context()
    ctx.push()

    def teardown():
        ctx.pop()

    request.addfinalizer(teardown)

    return flask_app


@pytest.fixture(scope='session')
def db(app, request):
    '''Database for tests'''
    _db.app = app
    return _db


@pytest.fixture(scope='function')
def session(db, request):
    '''Per-test database session'''
    connection = db.engine.connect()
    transaction = connection.begin()

    options = dict(bind=connection, binds={})
    session = db.create_scoped_session(options=options)

    db.session = session

    def teardown():
        transaction.rollback()
        connection.close()
        session.remove()

    request.addfinalizer(teardown)
    return session
