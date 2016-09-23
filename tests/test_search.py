import pytest
from api import search


def test_none_query():
    query = search.NoneQuery()
    assert query.all() == []
