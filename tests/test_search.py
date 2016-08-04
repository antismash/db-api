import pytest
from testutils import app  # noqa: F401
from api import search, sql
from api.helpers import get_db


def test_get_sql_by_category_fuzzy():
    '''Test getting the category filter SQL from the sql module'''
    result = search.get_sql_by_category_fuzzy('genus')
    assert result == sql.CLUSTER_BY_GENUS_FUZZY

    with pytest.raises(AttributeError):
        search.get_sql_by_category_fuzzy('invalid')


def test_clusters_by_category(app):  # noqa: F811
    '''Test getting a cluster by category'''
    expected = [1, 2, 3, 4]
    cur = get_db().cursor()

    cur.expected_queries.append((
        ('bgc_id'), sql.CLUSTER_BY_TYPE_OR_DESCRIPTION
    ))
    cur.canned_replies.append(((i,) for i in expected))

    results = search.clusters_by_category('type', 'foo')
    assert results == set(expected)

    results = search.clusters_by_category('invalid', 'nope')
    assert results == set()


def test_parse_simple_search_type(app):  # noqa: F811
    '''Test parsing the simple search string giving a type'''
    cur = get_db().cursor()

    cur.expected_queries.append((
        ('term'), sql.SEARCH_IS_TYPE
    ))

    cur.canned_replies.append(('lantipeptide',))

    result = search.parse_simple_search('LanTiPePtide')
    assert result == [{'category': 'type', 'term': 'lantipeptide'}]


def test_parse_simple_search_acc(app):  # noqa: F811
    '''Test parsing the simple search string giving an accession'''
    cur = get_db().cursor()

    cur.expected_queries.extend([
        (('term'), sql.SEARCH_IS_TYPE),
        (('acc'), sql.SEARCH_IS_ACC),
    ])

    cur.canned_replies.extend([
        None,
        ('AB12345',),
    ])

    result = search.parse_simple_search('aB12345')
    assert result == [{'category': 'acc', 'term': 'AB12345'}]


def test_parse_simple_search_genus(app):  # noqa: F811
    '''Test parsing the simple search string giving a genus'''
    cur = get_db().cursor()

    cur.expected_queries.extend([
        (('term'), sql.SEARCH_IS_TYPE),
        (('acc'), sql.SEARCH_IS_ACC),
        (('genus'), sql.SEARCH_IS_GENUS),
    ])

    cur.canned_replies.extend([
        None,
        None,
        ('Streptomyces',),
    ])

    result = search.parse_simple_search('STREPTomyces')
    assert result == [{'category': 'genus', 'term': 'Streptomyces'}]


def test_parse_simple_search_species(app):  # noqa: F811
    '''Test parsing the simple search string giving a genus'''
    cur = get_db().cursor()

    cur.expected_queries.extend([
        (('term'), sql.SEARCH_IS_TYPE),
        (('acc'), sql.SEARCH_IS_ACC),
        (('genus'), sql.SEARCH_IS_GENUS),
        (('species'), sql.SEARCH_IS_SPECIES),
    ])

    cur.canned_replies.extend([
        None,
        None,
        None,
        ('Streptomyces coelicolor',),
    ])

    result = search.parse_simple_search('coelicolor')
    assert result == [{'category': 'species', 'term': 'Streptomyces coelicolor'}]


def test_parse_simple_search_monomer(app):  # noqa: F811
    '''Test parsing the simple search string giving a monomer'''
    cur = get_db().cursor()

    cur.expected_queries.extend([
        (('term'), sql.SEARCH_IS_TYPE),
        (('acc'), sql.SEARCH_IS_ACC),
        (('genus'), sql.SEARCH_IS_GENUS),
        (('species'), sql.SEARCH_IS_SPECIES),
        (('name'), sql.SEARCH_IS_MONOMER),
    ])

    cur.canned_replies.extend([
        None,
        None,
        None,
        None,
        ('ccmal',),
    ])

    result = search.parse_simple_search('ccmal')
    assert result == [{'category': 'monomer', 'term': 'ccmal'}]


def test_parse_simple_search_compound(app):  # noqa: F811
    '''Test parsing the simple search string giving a compound'''
    cur = get_db().cursor()

    cur.expected_queries.extend([
        (('term'), sql.SEARCH_IS_TYPE),
        (('acc'), sql.SEARCH_IS_ACC),
        (('genus'), sql.SEARCH_IS_GENUS),
        (('species'), sql.SEARCH_IS_SPECIES),
        (('name'), sql.SEARCH_IS_MONOMER),
    ])

    cur.canned_replies.extend([
        None,
        None,
        None,
        None,
        None,
    ])

    result = search.parse_simple_search('FAKE')
    assert result == [{'category': 'compound_seq', 'term': 'FAKE'}]


def test_create_cluster_json_single(app):  # noqa: F811
    '''Test create_cluster_json'''
    cur = get_db().cursor()

    cur.expected_queries.append((
        ('term', 'description'), sql.CLUSTER_INFO
    ))

    cur.canned_replies.append([
        ('lassopeptide', 'Lasso peptide')
    ])

    expected = {
        'term': 'lassopeptide',
        'description': 'Lasso peptide',
    }

    results = search.create_cluster_json(23)
    assert results == expected


def test_create_cluster_json_hybrid(app):  # noqa: F811
    '''Test create_cluster_json for hybrid clusters'''
    cur = get_db().cursor()

    cur.expected_queries.append((
        ('term', 'description'), sql.CLUSTER_INFO
    ))

    cur.canned_replies.append([
        ('lassopeptide', 'Lasso peptide'),
        ('terpene', 'Terpene'),
    ])

    expected = {
        'term': 'lassopeptide-terpene hybrid',
        'description': 'Hybrid cluster: lassopeptide-terpene',
    }

    results = search.create_cluster_json(23)
    assert results == expected


def test_create_cluster_csv_single(app):  # noqa: F811
    '''Test create_cluster_csv'''
    cur = get_db().cursor()

    cur.expected_queries.append((
        ('bgc_id',
         'start_pos',
         'end_pos',
         'cluster_number',
         'acc',
         'version',
         'term',
         'description',
         'species',
         'cbh_acc',
         'cbh_description',
         'similarity'), sql.CLUSTER_INFO
    ))

    cur.canned_replies.append([
        (23, 17, 42, 23, 'ABC12345', 1, 'lassopeptide', 'Lasso peptide', 'E. xample', 'BGC12345', 'Examplin biosynthetic gene cluster', 71)
    ])

    expected = 'E. xample\tABC12345.1\t23\tlassopeptide\t17\t42\tExamplin biosynthetic gene cluster\t71\tBGC12345\thttp://antismash-db.secondarymetabolites.org/output/ABC12345/index.html#cluster-23'

    results = search.create_cluster_csv(23)
    assert results == expected


def test_create_cluster_csv_hybrid(app):  # noqa: F811
    '''Test create_cluster_csv for a hybrid cluster'''
    cur = get_db().cursor()

    cur.expected_queries.append((
        ('bgc_id',
         'start_pos',
         'end_pos',
         'cluster_number',
         'acc',
         'version',
         'term',
         'description',
         'species',
         'cbh_acc',
         'cbh_description',
         'similarity'), sql.CLUSTER_INFO
    ))

    cur.canned_replies.append([
        (23, 17, 42, 23, 'ABC12345', 1, 'lassopeptide', 'Lasso peptide', 'E. xample', 'BGC12345', 'Examplin biosynthetic gene cluster', 71),
        (23, 17, 42, 23, 'ABC12345', 1, 'terpene', 'Terpene', 'E. xample', 'BGC12345', 'Examplin biosynthetic gene cluster', 71),
    ])

    expected = 'E. xample\tABC12345.1\t23\tlassopeptide-terpene hybrid\t17\t42\tExamplin biosynthetic gene cluster\t71\tBGC12345\thttp://antismash-db.secondarymetabolites.org/output/ABC12345/index.html#cluster-23'

    results = search.create_cluster_csv(23)
    assert results == expected


def test_search_bgcs(monkeypatch):
    '''Test the main search function with a '''

    # We individually test all the other functions, so mock the heck out of them
    def fake_parse_simple_search(search_string):
        return [{'category': 'type', 'term': search_string}]
    monkeypatch.setattr(search, 'parse_simple_search', fake_parse_simple_search)

    def fake_clusters_by_category(category, term):
        return set((23, 42, len(category), len(term)))
    monkeypatch.setattr(search, 'clusters_by_category', fake_clusters_by_category)

    def fake_create_cluster_json(bgc_id):
        return {'bgc_id': bgc_id, 'term': 'fake', 'description': 'Fake cluster'}

    expected = [
        fake_create_cluster_json(4),
        fake_create_cluster_json(23),
        fake_create_cluster_json(42),
    ]
    count, results = search.search_bgcs('[type]fake', mapfunc=fake_create_cluster_json)
    assert count == 3
    assert results == expected

    count, results = search.search_bgcs('fake', mapfunc=fake_create_cluster_json)
    assert count == 3
    assert results == expected

    count, results = search.search_bgcs('fake', offset=1, mapfunc=fake_create_cluster_json)
    assert count == 3
    assert results == expected[1:]

    count, results = search.search_bgcs('fake', paginate=2, mapfunc=fake_create_cluster_json)
    assert count == 3
    assert results == expected[:2]
