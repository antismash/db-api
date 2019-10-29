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
    term = QueryTerm('expression', category='type', term='lanthipeptide')
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 5

    term.category = 'unknown'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 5

    term.category = 'type'
    term.term = 'bogus'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0

    term.category = 'bogus'
    try:
        search.cluster_query_from_term(term)
        assert False, "missing exception"
    except search.helpers.UnknownQueryError:
        pass


def test_cluster_query_from_term_operation():
    left = QueryTerm('expression', category='type', term='lanthipeptide')
    right = QueryTerm('expression', category='genus', term='Streptomyces')
    term = QueryTerm('operation', operation='and', left=left, right=right)

    ret = search.cluster_query_from_term(term)
    assert ret.count() == 5

    term.operation = 'or'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 122

    term.operation = 'except'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0

    left.term = 'bogus'
    term.operation = 'and'
    assert search.cluster_query_from_term(term).count() == 0

    term.operation = 'or'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 122

    term.operation = 'except'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0

    left.term = 'lanthipeptide'
    right.term = 'bogus'
    term.operation = 'and'
    assert search.cluster_query_from_term(term).count() == 0

    term.operation = 'or'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 5

    term.operation = 'except'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 5


def test_cluster_query_from_term_invalid():
    term = QueryTerm('expression', category='type', term='lanthipeptide')
    term.kind = 'bogus'
    try:
        search.cluster_query_from_term(term)
        assert False, "missing exception"
    except search.helpers.UnknownQueryError:
        pass
