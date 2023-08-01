import pytest
from api import search
from api.search_parser import QueryOperand, QueryOperation

from .test_clusters import TOTAL_REGION_COUNT


def test_none_query():
    query = search.NoneQuery()
    assert query.all() == []


def test_break_lines():
    string = "MAGICMAGICMAGIC"
    expected = "MAGIC\nMAGIC\nMAGIC"
    result = search.helpers.break_lines(string, width=5)
    assert result == expected


def test_cluster_query_from_term_expression():
    term = QueryOperand(category='type', value='lanthipeptide')
    expected_count = 5

    ret = search.cluster_query_from_term(term)
    assert ret.count() == expected_count

    term.category = 'unknown'
    with pytest.raises(ValueError):
        search.cluster_query_from_term(term)

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
    left = QueryOperand(category='type', value='lanthipeptide')
    right = QueryOperand(category='genus', value='Streptomyces')
    term = QueryOperation(operator='and', left=left, right=right)

    ret = search.cluster_query_from_term(term)
    assert ret.count() == 5

    term.operation = 'or'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == TOTAL_REGION_COUNT

    term.operation = 'except'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == 0

    left.term = 'bogus'
    term.operation = 'and'
    assert search.cluster_query_from_term(term).count() == 0

    term.operation = 'or'
    ret = search.cluster_query_from_term(term)
    assert ret.count() == TOTAL_REGION_COUNT

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
    term = QueryOperand(category='type', value='lanthipeptide')
    term.kind = 'bogus'
    try:
        search.cluster_query_from_term(term)
        assert False, "missing exception"
    except search.helpers.UnknownQueryError:
        pass
