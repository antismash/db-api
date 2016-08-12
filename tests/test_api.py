from flask import url_for
from api import sql
from api.helpers import get_db
# The noqa tells flake8 to ignore the unused import
from testutils import app  # noqa: F401


def test_version(client):
    '''Test /api/v1.0/version endpoint'''
    from api.version import __version__
    response = client.get(url_for('get_version'))
    assert response.status_code == 200
    assert 'api' in response.json
    assert response.json['api'] == __version__


def test_stats(client):
    '''Test /api/v1.0/stats endpoint'''
    cur = get_db().cursor()

    expected = {
        'num_clusters': 23,
        'num_genomes': 17,
        'num_sequences': 42,
        'top_seq_taxon': 12345,
        'top_seq_taxon_count': 2,
        'top_secmet_taxon': 54321,
        'top_secmet_taxon_count': 3,
        'top_secmet_species': 'E. xample',
        'clusters': [
            {'name': 'foo', 'description': 'fake cluster 1', 'count': 23},
            {'name': 'bar', 'description': 'fake cluster 2', 'count': 42},
        ],
    }

    expected_queries = [
        (('count'), sql.STATS_CLUSTER_COUNT),
        (('count'), sql.STATS_GENOME_COUNT),
        (('count'), sql.STATS_SEQUENCE_COUNT),
        (('term', 'description', 'count'), sql.STATS_COUNTS_BY_TYPE),
        (('tax_id', 'tax_count'), sql.STATS_TAXON_SEQUENCES),
        (('tax_id', 'species', 'clusters_per_seq'), sql.STATS_TAXON_SECMETS),
    ]

    # Replies need to be iterable for the FakeCursor to work
    canned_replies = [
        (expected['num_clusters'],),
        (expected['num_genomes'],),
        (expected['num_sequences'],),
        [(i['name'], i['description'], i['count']) for i in expected['clusters']],
        (expected['top_seq_taxon'], expected['top_seq_taxon_count']),
        (expected['top_secmet_taxon'], expected['top_secmet_species'], expected['top_secmet_taxon_count'])
    ]

    cur.expected_queries.extend(expected_queries)
    cur.canned_replies.extend(canned_replies)


    results = client.get(url_for('get_stats'))
    assert results.status_code == 200
    assert results.json == expected


def test_sec_met_tree(client):
    '''Test /api/v1.0/tree/secmet endpoint'''
    cur = get_db().cursor()

    expected = [
        {
            'id': 'nrps',
            'parent': '#',
            'text': 'Non-ribosomal peptide',
            'state': {
                'disabled': True
            }
        },
        {
            'id': 'lantipeptide',
            'parent': '#',
            'text': 'Lanthipeptide',
            'state': {
                'disabled': True
            }
        },
        {
            'id': 'AB1234_c1_nrps',
            'parent': 'nrps',
            'text': 'E. xample AB1234 Cluster 1',
            'type': 'cluster',
        },
        {
            'id': 'AB1234_c1_lantipeptide',
            'parent': 'lantipeptide',
            'text': 'E. xample AB1234 Cluster 1',
            'type': 'cluster',
        },
    ]

    cur.expected_queries.append((
        ('cluster_number', 'acc', 'term', 'description', 'species'),
        sql.SECMET_TREE
    ))
    cur.canned_replies.append([
        (1, 'AB1234', 'nrps', 'Non-ribosomal peptide', 'E. xample'),
        (1, 'AB1234', 'lantipeptide', 'Lanthipeptide', 'E. xample')
    ])

    results = client.get(url_for('get_sec_met_tree'))
    assert results.status_code == 200
    assert results.json == expected


def test_taxa_superkingdom(client):
    '''Test /api/v1.0/tree/taxa endpoint for superkingdom'''
    cur = get_db().cursor()
    cur.expected_queries.append((
        ('name'), sql.TAXTREE_SUPERKINGOM
    ))
    cur.canned_replies.append(
        [('Bacteria',), ('Fungi',)]
    )

    expected = [
        {
            'id': 'superkingdom_bacteria',
            'parent': '#',
            'text': 'Bacteria',
            'state': {
                'disabled': True
            },
            'children': True
        },
        {
            'id': 'superkingdom_fungi',
            'parent': '#',
            'text': 'Fungi',
            'state': {
                'disabled': True
            },
            'children': True
        },
    ]

    results = client.get(url_for('get_taxon_tree'))
    assert results.status_code == 200
    assert results.json == expected


def test_taxa_phylum(client):
    '''Test /api/v1.0/tree/taxa endpoint for phylum'''
    cur = get_db().cursor()
    cur.expected_queries.append((
        ('name'), sql.TAXTREE_PHYLUM
    ))
    cur.canned_replies.append(
        [('Actinobacteria',)]
    )

    expected = [
        {
            'id': 'phylum_bacteria_actinobacteria',
            'parent': 'superkingdom_bacteria',
            'text': 'Actinobacteria',
            'state': {
                'disabled': True
            },
            'children': True
        },
    ]

    results = client.get(url_for('get_taxon_tree'), query_string="id=superkingdom_bacteria")
    assert results.status_code == 200
    assert results.json == expected


def test_taxa_class(client):
    '''Test /api/v1.0/tree/taxa endpoint for class'''
    cur = get_db().cursor()
    cur.expected_queries.append((
        ('name'), sql.TAXTREE_CLASS
    ))
    cur.canned_replies.append(
        [('Actinobacteria',)]
    )

    expected = [
        {
            'id': 'class_bacteria_actinobacteria_actinobacteria',
            'parent': 'phylum_bacteria_actinobacteria',
            'text': 'Actinobacteria',
            'state': {
                'disabled': True
            },
            'children': True
        },
    ]

    results = client.get(url_for('get_taxon_tree'), query_string="id=phylum_bacteria_actinobacteria")
    assert results.status_code == 200
    assert results.json == expected


def test_taxa_order(client):
    '''Test /api/v1.0/tree/taxa endpoint for order'''
    cur = get_db().cursor()
    cur.expected_queries.append((
        ('name'), sql.TAXTREE_ORDER
    ))
    cur.canned_replies.append(
        [('Streptomycetales',)]
    )

    expected = [
        {
            'id': 'order_bacteria_actinobacteria_actinobacteria_streptomycetales',
            'parent': 'class_bacteria_actinobacteria_actinobacteria',
            'text': 'Streptomycetales',
            'state': {
                'disabled': True
            },
            'children': True
        },
    ]

    results = client.get(url_for('get_taxon_tree'), query_string="id=class_bacteria_actinobacteria_actinobacteria")
    assert results.status_code == 200
    assert results.json == expected


def test_taxa_family(client):
    '''Test /api/v1.0/tree/taxa endpoint for family'''
    cur = get_db().cursor()
    cur.expected_queries.append((
        ('name'), sql.TAXTREE_FAMILY
    ))
    cur.canned_replies.append(
        [('Streptomycetaceae',)]
    )

    expected = [
        {
            'id': 'family_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae',
            'parent': 'order_bacteria_actinobacteria_actinobacteria_streptomycetales',
            'text': 'Streptomycetaceae',
            'state': {
                'disabled': True
            },
            'children': True
        },
    ]

    results = client.get(url_for('get_taxon_tree'), query_string="id=order_bacteria_actinobacteria_actinobacteria_streptomycetales")
    assert results.status_code == 200
    assert results.json == expected


def test_taxa_genus(client):
    '''Test /api/v1.0/tree/taxa endpoint for genus'''
    cur = get_db().cursor()
    cur.expected_queries.append((
        ('name'), sql.TAXTREE_GENUS
    ))
    cur.canned_replies.append(
        [('Streptomyces',)]
    )

    expected = [
        {
            'id': 'genus_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae_streptomyces',
            'parent': 'family_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae',
            'text': 'Streptomyces',
            'state': {
                'disabled': True
            },
            'children': True
        },
    ]

    results = client.get(url_for('get_taxon_tree'), query_string="id=family_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae")
    assert results.status_code == 200
    assert results.json == expected


def test_taxa_species(client):
    '''Test /api/v1.0/tree/taxa endpoint for species'''
    cur = get_db().cursor()
    cur.expected_queries.append((
        ('species', 'acc', 'version'), sql.TAXTREE_SPECIES
    ))
    cur.canned_replies.append(
        [('Streptomyces coelicolor', 'NC_003888', 3)]
    )

    expected = [
        {
            'id': 'nc_003888',
            'parent': 'genus_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae_streptomyces',
            'text': 'Streptomyces coelicolor NC_003888.3',
            'type': 'strain'
        },
    ]

    results = client.get(url_for('get_taxon_tree'), query_string="id=genus_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae_streptomyces")
    assert results.status_code == 200
    assert results.json == expected


def test_search(client, monkeypatch):
    '''Test /api/v1.0/search endpoint'''
    expected = {
        'total': 0,
        'clusters': [],
        'stats': {},
        'offset': 0,
        'paginate': 50
    }

    # We have separate tests for the search code, so just test the REST API part here
    import api.api

    def fake_search(search_string, offset=0, paginate=0):
        '''fake search function'''
        return expected['total'], {}, expected['clusters']
    monkeypatch.setattr(api.api, 'search_bgcs', fake_search)

    results = client.post(url_for('search'), data='{"search_string": "foo"}', content_type="application/json")
    assert results.status_code == 200
    assert results.json == expected

    expected['clusters'] = [{'foo': 'foo'}, {'bar': 'bar'}]
    expected['total'] = 23
    expected['offset'] = 2

    results = client.post(url_for('search'), data='{"search_string": "foo", "offset": 2}', content_type="application/json")
    assert results.status_code == 200
    assert results.json == expected

    expected['paginate'] = 5

    results = client.post(url_for('search'), data='{"search_string": "foo", "offset": 2, "paginate": 5}', content_type="application/json")
    assert results.status_code == 200
    assert results.json == expected


def test_export(client, monkeypatch):
    '''Test /api/v1.0/export endpoint'''
    expected = '''#Species\tNCBI accession\tCluster number\tBGC type\tFrom\tTo\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL
fake\tcsv\tline\n'''

    # We have separate tests for the search code, so just test the REST API part here
    import api.api

    def fake_search(search_string, offset=0, paginate=0, mapfunc=None):
        '''fake search function'''
        return None, {}, ['fake\tcsv\tline']
    monkeypatch.setattr(api.api, 'search_bgcs', fake_search)

    results = client.post(url_for('export'), data='{"search_string": "foo"}', content_type="application/json")
    assert results.status_code == 200
    print dir(results)
    assert results.data == expected


def test_genome(client, monkeypatch):
    '''Test /api/v1.0/genome/<identifier> endpoint'''
    reference = {
        'count': 23,
        'clusters': [{'foo': 'foo'}, {'bar': 'bar'}]
    }

    # We have separate tests for the search code, so just test the REST API part here
    import api.api

    def fake_search(search_string):
        '''fake search function'''
        return reference['count'], {}, reference['clusters']
    monkeypatch.setattr(api.api, 'search_bgcs', fake_search)

    results = client.get(url_for('show_genome', identifier='fake'))
    assert results.status_code == 200
    assert results.json == reference['clusters']
