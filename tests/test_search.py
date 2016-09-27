import pytest
from api import search
from api.search_parser import QueryTerm


def test_none_query():
    query = search.NoneQuery()
    assert query.all() == []


def test_break_lines():
    string = "MAGICMAGICMAGIC"
    expected = "MAGIC\nMAGIC\nMAGIC"
    result = search.helpers.break_lines(string, width=5)
    assert result == expected


def test_cluster_query_from_term_expression():
    term = QueryTerm('expression', category='type', term='lantipeptide')
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 4

    term.category = 'unknown'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 4

    term.category = 'type'
    term.term = 'bogus'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0

    term.category = 'bogus'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0


def test_cluster_query_from_term_operation():
    left = QueryTerm('expression', category='type', term='lantipeptide')
    right = QueryTerm('expression', category='genus', term='Lactococcus')
    term = QueryTerm('operation', operation='and', left=left, right=right)

    ret = search.cluster_query_from_term(term)
    assert ret.count() == 1

    term.operation = 'or'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 4

    term.operation = 'except'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 3

    left.category = 'bogus'
    term.operation = 'and'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0

    term.operation = 'or'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 1

    term.operation = 'except'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0

    left.category = 'type'
    right.category = 'bogus'
    term.operation = 'and'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0

    term.operation = 'or'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 4

    term.operation = 'except'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 4


def test_cluster_query_from_term_invalid():
    term = QueryTerm('expression', category='type', term='lantipeptide')
    term.kind = 'bogus'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0
