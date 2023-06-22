import pytest
from api.search import filters
from api.search_parser import (
    Query,
    QueryTerm,
)


def test_query_init():
    query = Query(None)
    assert query.terms is None
    assert query.search_type == 'cluster'
    assert query.return_type == 'json'
    assert repr(query) == "Query(search_type: 'cluster', return_type: 'json', terms: 'None')"


def test_query_from_json():
    minimal = dict(terms={'term_type': 'expr', 'category': 'test_category', 'term': 'test_term'})
    query = Query.from_json(minimal)
    assert query.search_type == 'cluster'
    assert query.return_type == 'json'
    assert isinstance(query.terms, QueryTerm)

    extended = dict(minimal.items())
    extended['search'] = 'gene'
    extended['return_type'] = 'csv'
    query = Query.from_json(extended)
    assert query.search_type == 'gene'
    assert query.return_type == 'csv'
    assert isinstance(query.terms, QueryTerm)

    with pytest.raises(ValueError):
        Query.from_json({})


def test_query_from_string():
    string = "[type]ripp"
    query = Query.from_string(string)
    assert query.search_type == 'cluster'
    assert query.return_type == 'json'
    assert isinstance(query.terms, QueryTerm)

    string = "[type]ripp"
    query = Query.from_string(string, return_type='csv')
    assert query.search_type == 'cluster'
    assert query.return_type == 'csv'
    assert isinstance(query.terms, QueryTerm)


def test_query_term_init_expression():
    with pytest.raises(ValueError):
        term = QueryTerm('expression')
    with pytest.raises(ValueError):
        term = QueryTerm('expression', category='type')
    with pytest.raises(ValueError):
        term = QueryTerm('expression', term='nrps')

    term = QueryTerm('expression', category='type', term='nrps')
    assert term.kind == 'expression'
    assert term.category == 'type'
    assert term.term == 'nrps'
    assert repr(term) == "QueryTerm(category: 'type', term: 'nrps')"
    assert str(term) == "[type]nrps"


def test_query_term_init_operation():
    with pytest.raises(ValueError):
        term = QueryTerm('operation')

    left = QueryTerm('expression', category='type', term='nrps')
    right = QueryTerm('expression', category='type', term='nrps')
    term = QueryTerm('operation', operation='or', left=left, right=right)
    assert term.kind == 'operation'
    assert term.operation == 'or'
    assert term.left == left
    assert term.right == right

    expected = "QueryTerm(operation: {!r},\n\tleft: {!r}\n\tright: {!r}\n)".format('or', left, right)

    assert repr(term) == expected
    assert str(term) == "( [type]nrps OR [type]nrps )"


def test_query_term_init_invalid():
    with pytest.raises(ValueError):
        QueryTerm('foo')


def test_query_term_from_json():
    invalid = {}
    with pytest.raises(ValueError):
        QueryTerm.from_json(invalid)

    invalid['term_type'] = 'invalid'
    with pytest.raises(ValueError):
        QueryTerm.from_json(invalid)

    expr = {}
    expr['term_type'] = 'expr'
    with pytest.raises(ValueError):
        QueryTerm.from_json(expr)

    expr['category'] = 'type'
    expr['term'] = 'nrps'
    term = QueryTerm.from_json(expr)
    assert term.kind == 'expression'
    assert term.category == expr['category']
    assert term.term == expr['term']

    op = {}
    op['term_type'] = 'op'
    with pytest.raises(ValueError):
        QueryTerm.from_json(op)

    op['operation'] = 'oR'
    op['left'] = expr
    op['right'] = expr
    term = QueryTerm.from_json(op)
    assert term.kind == 'operation'
    assert term.operation == 'or'
    assert isinstance(term.left, QueryTerm)
    assert isinstance(term.right, QueryTerm)


def test_query_term_from_string():
    string = ""
    with pytest.raises(ValueError):
        QueryTerm.from_string(string)

    string = "nrps"
    term = QueryTerm.from_string(string)
    assert term.kind == 'expression'
    assert term.category == 'unknown'
    assert term.term == 'nrps'

    string = "[type]nrps"
    term = QueryTerm.from_string(string)
    assert term.kind == 'expression'
    assert term.category == 'type'
    assert term.term == 'nrps'

    string = "[contigedge]true"
    term = QueryTerm.from_string(string)
    assert term.kind == 'expression'
    assert term.category == 'contigedge'
    assert term.term is True

    string = "nrps AND 1234"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.operation == 'and'
    assert term.left.term == 'nrps'
    assert term.right.term == '1234'

    string = "nrps OR 1234"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.operation == 'or'
    assert term.left.term == 'nrps'
    assert term.right.term == '1234'

    string = "nrps EXCEPT 1234"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.operation == 'except'
    assert term.left.term == 'nrps'
    assert term.right.term == '1234'

    string = "nrps 1234"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.operation == 'and'
    assert term.left.term == 'nrps'
    assert term.right.term == '1234'

    string = "nrps OR 1234"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.operation == 'or'
    assert term.left.term == 'nrps'
    assert term.right.term == '1234'

    string = "nrps or 1234"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.operation == 'or'
    assert term.left.term == 'nrps'
    assert term.right.term == '1234'

    string = "ripp AND ( streptomyces OR lactococcus )"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.left.term == 'ripp'
    assert term.right.kind == 'operation'
    assert term.right.left.term == 'streptomyces'
    assert term.right.right.term == 'lactococcus'

    string = "lanthipeptide ((Streptomyces coelicolor) OR (Lactococcus lactis))"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.left.term == 'lanthipeptide'
    assert term.right.kind == 'operation'
    assert term.right.left.kind == 'operation'
    assert term.right.left.left.term == 'Streptomyces'
    assert term.right.right.kind == 'operation'
    assert term.right.right.right.term == 'lactis'

    with pytest.raises(ValueError):
        string = "AND ripp"
        term = QueryTerm.from_string(string)

    with pytest.raises(ValueError):
        string = "END"
        term = QueryTerm.from_string(string)

    with pytest.raises(ValueError):
        string = "( ripp"
        term = QueryTerm.from_string(string)


def test_query_text_filters():
    string = "[candidatekind]neighbouring WITH [bgctype](hgle-ks) WITH [bgctype](spaced term)"
    term = QueryTerm.from_string(string)
    assert term.kind == "expression"
    assert term.category == "candidatekind"
    assert term.term == "neighbouring"
    expected_options = [
        {"name": "bgctype", "value": "hgle-ks"},
        {"name": "bgctype", "value": "spaced term"},
    ]
    all_options = []
    for filter_instance, options in term.filters:
        assert isinstance(filter_instance, filters.TextFilter)
        all_options.append(options)
    assert all_options == expected_options
    # and then the whole thing needs to rebuild
    assert str(term) == string


def test_query_numeric_filters():
    string = "[tfbs]reg WITH [quality](>= 20)"
    term = QueryTerm.from_string(string)
    assert term.kind == "expression"
    assert term.category == "tfbs"
    assert term.term == "reg"
    assert len(term.filters) == 1
    filter_instance, options = term.filters[0]
    assert isinstance(filter_instance, filters.QualitativeFilter)
    assert options == {"name": "quality", "operator": ">=", "value": 20}
    # and then the whole thing needs to rebuild
    assert str(term) == string


def test_invalid_query_filters():
    # don't allow filters to work when the main term is missing the category
    with pytest.raises(ValueError):
        QueryTerm.from_string("nrps WITH [bgctype](hgle-ks)")
    # missing a filter category
    with pytest.raises(ValueError):
        QueryTerm.from_string("[candidatekind]neighbouring WITH [](hgle-ks)")
    # missing any value
    with pytest.raises(ValueError):
        QueryTerm.from_string("[candidatekind]neighbouring WITH [bgctype]()")
    # missing a numeric value
    with pytest.raises(ValueError):
        QueryTerm.from_string("[tfbs]reg WITH [quality](>=)")
    # missing an operator
    with pytest.raises(ValueError):
        QueryTerm.from_string("[tfbs]reg WITH [quality](20)")


def test_query_term__generate_tokens():
    string = "lanthipeptide"
    tokens = QueryTerm._generate_tokens(string)
    assert tokens == ['lanthipeptide', 'END']

    string = "(foo)"
    tokens = QueryTerm._generate_tokens(string)
    assert tokens == ['(', 'foo', ')', 'END']

    string = "foo (foo) foo"
    tokens = QueryTerm._generate_tokens(string)
    assert tokens == ['foo', '(', 'foo', ')', 'foo', 'END']
