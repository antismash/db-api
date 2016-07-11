import pytest
from api import helpers


@pytest.fixture
def app(monkeypatch):
    from api import app as flask_app
    import psycopg2

    def fake_connect(connection_string, cursor_factory):
        '''fake the connect function'''
        return (connection_string, cursor_factory)

    monkeypatch.setattr(psycopg2, 'connect', fake_connect)
    return flask_app


def test_connect_db(app):
    '''Test connect_db()'''
    import psycopg2.extras
    fake_connection = helpers.connect_db()
    assert fake_connection == (app.config['DB_CONNECTION'], psycopg2.extras.NamedTupleCursor)
