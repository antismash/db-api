import pytest
from api.search import filters
from api.search_parser import (
    Query,
    QueryItem,
    QueryTerm,
    QueryOperand,
    QueryOperation,
)


def test_query_init():
    query = Query(None)
    assert query.terms is None
    assert query.search_type == 'cluster'
    assert query.return_type == 'json'
    assert repr(query) == "Query(search_type: 'cluster', return_type: 'json', terms: 'None')"


def test_query_from_json():
    minimal = dict(terms={'term_type': 'expr', 'category': 'test_category', 'value': 'test_term'})
    query = Query.from_json(minimal)
    assert query.search_type == 'cluster'
    assert query.return_type == 'json'
    assert isinstance(query.terms, QueryItem)

    extended = dict(minimal.items())
    extended['search'] = 'gene'
    extended['return_type'] = 'csv'
    query = Query.from_json(extended)
    assert query.search_type == 'gene'
    assert query.return_type == 'csv'
    assert isinstance(query.terms, QueryItem)

    with pytest.raises(ValueError):
        Query.from_json({})


def test_query_from_string():
    string = "{[type|ripp]}"
    query = Query.from_string(string)
    assert query.search_type == 'cluster'
    assert query.return_type == 'json'
    assert isinstance(query.terms, QueryItem)
    assert query.terms.count == -1

    string = "{[type|ripp]}"
    query = Query.from_string(string, return_type='csv')
    assert query.search_type == 'cluster'
    assert query.return_type == 'csv'
    assert isinstance(query.terms, QueryItem)

    string = "3 * {[type|ripp]}"
    query = Query.from_string(string)
    assert query.terms.count == 3
    assert query.terms.category == "type"
    assert query.terms.term == "ripp"


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
    expr['value'] = 'nrps'
    term = QueryTerm.from_json(expr)
    assert term.kind == 'expression'
    assert term.category == expr['category']
    assert term.term == expr['value']

    op = {}
    op['term_type'] = 'op'
    with pytest.raises(ValueError):
        QueryTerm.from_json(op)

    op['operation'] = 'oR'
    op['left'] = expr
    op['right'] = expr
    term = QueryTerm.from_json(op)
    assert term.kind == 'operation'
    assert term.operator == 'or'
    assert isinstance(term.left, QueryOperand)
    assert isinstance(term.right, QueryOperand)


def test_query_term_from_string():
    string = ""
    with pytest.raises(ValueError):
        QueryTerm.from_string(string)

    string = "nrps"
    with pytest.raises(ValueError):
        QueryTerm.from_string(string)

    string = "{[type|nrps]}"
    term = QueryTerm.from_string(string)
    assert term.kind == 'expression'
    assert term.category == 'type'
    assert term.term == 'nrps'

    string = "{[contigedge]}"
    term = QueryTerm.from_string(string)
    assert term.kind == 'expression'
    assert term.category == 'contigedge'

    base = "({[type|nrps]} %s {[type|1234]})"
    for operator in QueryOperation.OPERATORS:
        for case in [operator.lower(), operator.upper()]:
            string = base % (case,)
            term = QueryTerm.from_string(string)
            assert term.kind == 'operation'
            assert term.operator == operator.lower()
            assert term.left.term == 'nrps'
            assert term.right.term == '1234'

    string = "nrps 1234"
    with pytest.raises(ValueError):
        QueryTerm.from_string(string)

    string = "{[type|ripp]} AND ( {[genus|streptomyces]} OR {[genus|lactococcus]} )"
    with pytest.raises(ValueError):
        # missing out parens shouldn't make this come back as an operand
        QueryTerm.from_string(string)

    string = "({[type|ripp]} AND ( {[genus|streptomyces]} OR {[genus|lactococcus]} ))"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.left.term == 'ripp'
    assert term.right.kind == 'operation'
    assert term.right.left.term == 'streptomyces'
    assert term.right.right.term == 'lactococcus'


    string = "({[type|lanthipeptide]} AND ( {[strain|Streptomyces coelicolor]} OR {[strain|Lactococcus lactis]} ))"
    term = QueryTerm.from_string(string)
    assert term.kind == 'operation'
    assert term.left.term == 'lanthipeptide'
    assert term.right.kind == 'operation'
    assert term.right.left.kind == 'expression'
    assert term.right.left.term == 'Streptomyces coelicolor'
    assert term.right.right.kind == 'expression'
    assert term.right.right.term == 'Lactococcus lactis'

    with pytest.raises(ValueError):
        string = "AND ripp"
        term = QueryTerm.from_string(string)

    with pytest.raises(ValueError):
        string = "END"
        term = QueryTerm.from_string(string)

    with pytest.raises(ValueError):
        string = "( ripp"
        term = QueryTerm.from_string(string)


def test_query_real():
    string = "3*{[knowncluster|BGC00] WITH [similarity|>=:36]}"
    term = QueryTerm.from_string(string)
    assert term.kind == "expression"
    assert term.category == "knowncluster"
    assert term.term == "BGC00"
    assert len(term.filters) == 1
    filt = term.filters[0]
    assert filt.runner is filters.available_filters_by_category(term.category, filt.name, as_json=False)
    assert filt.value == 36
    assert filt.operator == ">="


def test_round_trip_json_conversion():
    def check(obj):
        assert isinstance(obj, QueryOperation)
        assert obj.operator == "and"
        assert isinstance(obj.left, QueryOperand)
        assert obj.left.category == "type"
        assert obj.left.term == "nrps"
        assert isinstance(obj.right, QueryOperation)
        assert obj.right.operator == "and"
        left = obj.right.left
        right = obj.right.right
        assert isinstance(left, QueryOperand)
        assert isinstance(right, QueryOperand)
        assert left.category == "asdomain"
        assert left.term == "Epimerization"
        assert right.category == "asdomain"
        assert right.term == "Thioesterase"

    term = QueryTerm.from_string("( {[type|nrps]} AND ( {[asdomain|Epimerization]} AND {[asdomain|Thioesterase]} ) )")
    check(term)
    result = QueryTerm.from_json(term.to_json())
    check(result)


def test_query_text_filters():
    string = "{[candidatekind|neighbouring] WITH [bgctype|hgle-ks] WITH [bgctype|spaced term]}"
    term = QueryTerm.from_string(string)
    assert term.kind == "expression"
    assert term.category == "candidatekind"
    assert term.term == "neighbouring"
    expected_options = [
        {"name": "bgctype", "value": "hgle-ks"},
        {"name": "bgctype", "value": "spaced term"},
    ]
    all_options = []
    for f in term.filters:
        assert isinstance(f.runner, filters.TextFilter)
        all_options.append(f.get_options())
    assert all_options == expected_options
    # and then the whole thing needs to rebuild
    assert str(term) == string


def test_query_numeric_filters():
    string = "{[tfbs|reg] WITH [quality|>=:20]}"
    term = QueryTerm.from_string(string)
    assert term.kind == "expression"
    assert term.category == "tfbs"
    assert term.term == "reg"
    assert len(term.filters) == 1
    filter_instance = term.filters[0]
    assert isinstance(filter_instance.runner, filters.QualitativeFilter)
    assert filter_instance.operator == ">="
    assert filter_instance.value == 20
    assert filter_instance.name == "quality"
    # and then the whole thing needs to rebuild
    assert str(term) == string


def test_invalid_query_filters():
    # don't allow filters to work when the main term is missing the category
    with pytest.raises(ValueError):
        QueryTerm.from_string("nrps WITH [bgctype|hgle-ks]")
    # missing a filter category
    with pytest.raises(ValueError):
        QueryTerm.from_string("{[candidatekind|neighbouring] WITH [|hgle-ks]")
    # missing any value
    with pytest.raises(ValueError):
        QueryTerm.from_string("{[candidatekind|neighbouring] WITH [bgctype|]}")
    # missing a numeric value
    with pytest.raises(ValueError):
        QueryTerm.from_string("[tfbs|reg] WITH [quality|>=:]")
    # missing an operator
    with pytest.raises(ValueError):
        QueryTerm.from_string("{[tfbs|reg] WITH [quality|:20]}")
