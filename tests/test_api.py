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
