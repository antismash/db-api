import pytest


#######################
# Test infrastructure #
#######################
def fake_connection():
    '''Fake psycopg2 connection for unit testing'''
    from collections import namedtuple

    class FakeConnection(object):
        '''Fake psycopg2 connection object'''
        def __init__(self):
            '''Set up a persisten cursor object'''
            self._cursor = FakeCursor()

        def cursor(self):
            '''Return a fake cursor'''
            return self._cursor

        def close(self):
            '''noop'''
            pass

    class FakeCursor(object):
        '''Fake NamedTupleCursor-like object'''
        def __init__(self):
            '''Set up empty values'''
            # expected queries should contain a tuple of NamedTuple fields and the query string
            self.expected_queries = []
            self.canned_replies = []
            self.current_names = []

        def execute(self, sql_query, params=None):
            '''Fake execution of the SQL query'''
            assert len(self.expected_queries) > 0
            self.current_names, expected_query = self.expected_queries.pop(0)
            assert sql_query == expected_query

        def fetchone(self):
            '''Fake returning one result'''
            assert len(self.canned_replies) > 0
            reply = self.canned_replies.pop(0)
            return self.wrap_reply(reply)

        def fetchall(self):
            '''Fake returning a result list'''
            assert len(self.canned_replies) > 0
            replies = self.canned_replies.pop(0)
            return [self.wrap_reply(reply) for reply in replies]

        def wrap_reply(self, reply):
            '''Create a namedtuple from the reply'''
            if reply is None:
                return reply

            FakeReply = namedtuple('FakeReply', self.current_names)
            return FakeReply(*reply)

    return FakeConnection()


@pytest.fixture
def app(monkeypatch):
    from api import app as flask_app
    import api.helpers
    monkeypatch.setattr(api.helpers, 'connect_db', fake_connection)
    return flask_app
