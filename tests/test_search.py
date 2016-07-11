import pytest
from testutils import app  # noqa: F401
from api import search, sql
from api.helpers import get_db


def test_get_sql_by_category():
    '''Test getting the category filter SQL from the sql module'''
    result = search.get_sql_by_category('type')
    assert result == sql.CLUSTER_BY_TYPE

    with pytest.raises(AttributeError):
        search.get_sql_by_category('invalid')


def test_clusters_by_category(app):  # noqa: F811
    '''Test getting a cluster by category'''
    expected = [1, 2, 3, 4]
    cur = get_db().cursor()

    cur.expected_queries.append((
        ('bgc_id'), sql.CLUSTER_BY_TYPE
    ))
    cur.canned_replies.append(((i,) for i in expected))

    results = search.clusters_by_category('type', 'foo')
    assert results == set(expected)

    results = search.clusters_by_category('invalid', 'nope')
    assert results == set()
