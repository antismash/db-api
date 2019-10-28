import json
from flask import url_for
from api import taxtree


def test_version(client):
    '''Test /api/v1.0/version endpoint'''
    from api.version import __version__
    response = client.get(url_for('get_version'))
    assert response.status_code == 200
    assert 'api' in response.json
    assert response.json['api'] == __version__


def test_stats(client):
    '''Test /api/v2.0/stats endpoint'''

    expected = {
        'num_sequences': 86,
        'top_seq_taxon_count': 84,
        'top_seq_species': 'Streptomyces Unclassified',
        'num_genomes': 2,
        'top_secmet_assembly_id': 'GCF_000203835.1',
        'num_clusters': 122,
        'top_secmet_species': 'Streptomyces coelicolor A3(2)',
        'top_secmet_taxon_count': 27,
        'top_seq_taxon': 1158056,
        'top_secmet_taxon': 100226,
        'clusters': [
            {'name': 't1pks', 'count': 48, 'description': 'Type I polyketide'},
            {'name': 'nrps', 'count': 24, 'description': 'Nonribosomal peptide'},
            {'name': 'terpene', 'count': 14, 'description': 'Terpene'},
            {'name': 'nrps-like', 'count': 6, 'description': 'Nonribosomal peptide fragment'},
            {'name': 'siderophore', 'count': 6, 'description': 'Siderophore'},
            {'name': 't3pks', 'count': 6, 'description': 'Type III polyketide'},
            {'name': 'butyrolactone', 'count': 5, 'description': 'Butyrolactone'},
            {'name': 'lanthipeptide', 'count': 5, 'description': 'Lanthipeptide'},
            {'name': 'bacteriocin', 'count': 4, 'description': 'Bacteriocin or other unspecified RiPP'},
            {'name': 'other', 'count': 3, 'description': 'Other'},
            {'name': 't2pks', 'count': 3, 'description': 'Type II polyketide'},
            {'name': 'arylpolyene', 'count': 2, 'description': 'Aryl polyene'},
            {'name': 'betalactone', 'count': 2, 'description': 'Beta-lactone containing protease inhibitor'},
            {'name': 'ectoine', 'count': 2, 'description': 'Ectoine'},
            {'name': 'hgle-ks', 'count': 2, 'description': 'hglE-type polyketide'},
            {'name': 'indole', 'count': 2, 'description': 'Indole'},
            {'name': 'ladderane', 'count': 2, 'description': 'Ladderane'},
            {'name': 'lassopeptide', 'count': 2, 'description': 'Lasso peptide'},
            {'name': 'pks-like', 'count': 2, 'description': 'Polyketide fragment'},
            {'name': 'blactam', 'count': 1, 'description': 'Beta-lactam'},
            {'name': 'furan', 'count': 1, 'description': 'Furan'},
            {'name': 'hserlactone', 'count': 1, 'description': 'Homoserine lactone'},
            {'name': 'linaridin', 'count': 1, 'description': 'Linaridin'},
            {'name': 'melanin', 'count': 1, 'description': 'Melanin'}
        ]
    }

    results = client.get(url_for('get_stats_v2'))
    assert results.status_code == 200
    assert results.json == expected


def test_sec_met_tree(client):
    '''Test /api/v1.0/tree/secmet endpoint'''

    expected = [
        {
            "id": "arylpolyene",
            "parent": "#",
            "state": {
                "disabled": True
            },
            "text": "Aryl polyene"
        },
    ]

    results = client.get(url_for('get_sec_met_tree'))
    assert results.status_code == 200
    assert results.json[:1] == expected


def test_taxa_superkingdom(client):
    '''Test /api/v1.0/tree/taxa endpoint for superkingdom'''

    expected = taxtree.get_superkingdom()
    results = client.get(url_for('get_taxon_tree'))
    assert results.status_code == 200
    assert results.json == expected


def test_taxa_phylum(client):
    '''Test /api/v1.0/tree/taxa endpoint for phylum'''

    expected = taxtree.get_phylum(['bacteria'])
    results = client.get(url_for('get_taxon_tree'), query_string="id=superkingdom_bacteria")
    assert results.status_code == 200
    assert results.json == expected


def test_search(client):
    '''Test /api/v1.0/search endpoint'''
    expected = {
        "clusters": [
            {
                "acc": "NC_003903",
                "assembly_id": "GCF_000203835",
                "cbh_acc": "BGC0000914",
                "cbh_description": "Methylenomycin",
                "cbh_rank": 1,
                "region_number": 2,
                "contig_edge": False,
                "description": "Hybrid cluster: Butyrolactone & Furan",
                "start_pos": 226409,
                "end_pos": 255381,
                "genus": "Streptomyces",
                "minimal": False,
                "similarity": 100,
                "species": "coelicolor",
                "strain": "A3(2)",
                "term": "butyrolactone - furan hybrid",
                "version": 1
            }
        ],
        "offset": 0,
        "paginate": 50,
        "stats": {
            "clusters_by_phylum": {
                "data": [
                    1
                ],
                "labels": [
                    "Actinobacteria"
                ]
            },
            "clusters_by_type": {
                "data": [
                    1,
                    1
                ],
                "labels": [
                    "butyrolactone",
                    "furan"
                ]
            }
        },
        "total": 1,
    }

    results = client.post(url_for('search'), data='{"search_string": "[type]furan"}', content_type="application/json")
    assert results.status_code == 200
    assert results.json["clusters"][0].pop("bgc_id")
    assert results.json == expected

    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'type', 'term': 'furan'}}}
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert results.json["clusters"][0].pop("bgc_id")
    assert results.json == expected

    query['query']['return_type'] = 'csv'
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 400

    query['query'] = {}
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 400


def test_search_paginate(client):
    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'genus', 'term': 'Streptomyces'}},
             'paginate': 5}
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert len(results.json['clusters']) == 5
    assert results.json['paginate'] == 5


def test_export(client):
    '''Test /api/v1.0/export endpoint'''

    expected_json = [{
        "acc": "NC_003903",
        "assembly_id": "GCF_000203835",
        "cbh_acc": "BGC0000914",
        "cbh_description": "Methylenomycin",
        "cbh_rank": 1,
        "region_number": 2,
        "contig_edge": False,
        "description": "Hybrid cluster: Butyrolactone & Furan",
        "start_pos": 226409,
        "end_pos": 255381,
        "genus": "Streptomyces",
        "minimal": False,
        "similarity": 100,
        "species": "coelicolor",
        "strain": "A3(2)",
        "term": "butyrolactone - furan hybrid",
        "version": 1
    }]

    expected_csv = '''#Genus\tSpecies\tStrain\tNCBI accession\tFrom\tTo\tBGC type\tOn contig edge\tFast mode only\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL\tDownload URL
{genus}\t{species}\t{strain}\t{acc}.{version}\t{start_pos}\t{end_pos}\t{term}\t{contig_edge}\t{minimal}\t{cbh_description}\t{similarity}\t{cbh_acc}\thttps://antismash-db.secondarymetabolites.org/go/GCF_000203835/{acc}.{version}/{start_pos}.{end_pos}\thttps://antismash-db.secondarymetabolites.org/api/v1.0/download/genbank/GCF_000203835/{acc}.{version}/{start_pos}.{end_pos}
'''.format(**expected_json[0]).encode()

    expected_fasta = '''>{acc}.{version}|{start_pos}-{end_pos}|butyrolactone - furan|{genus} {species} {strain}
ACCGGCCTCGCCGTGACGCGGGTGCTCGGGGTCGAAGATCCCGTCCAGCGGGATCGACTTGCCCATGATGTCCGGCGGCA'''.format(**expected_json[0]).encode()

    results = client.post(url_for('export'), data='{"search_string": "[type]furan"}', content_type="application/json")
    assert results.status_code == 200
    assert results.data == expected_csv

    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'type', 'term': 'furan'}}}
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    results.json[0].pop("bgc_id")  # arbitrary value, not useful for testing
    assert results.json == expected_json

    query['query']['return_type'] = 'csv'
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert results.data == expected_csv

    query['query']['return_type'] = 'fasta'
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert results.data[:len(expected_fasta)] == expected_fasta
    assert results.data.startswith(expected_fasta)

    query['query']['return_type'] = 'bogus'
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 400

    query['query'] = {}
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 400


def test_genome(client):
    '''Test /api/v1.0/genome/<identifier> endpoint'''
    expected = {
        "acc": "NC_003888",
        "assembly_id": "GCF_000203835",
        "cbh_acc": "BGC0001101",
        "cbh_description": "Leinamycin",
        "cbh_rank": 1,
        "region_number": 1,
        "contig_edge": False,
        "description": "Hybrid cluster: hglE-type polyketide & Type I polyketide",
        "end_pos": 139654,
        "genus": "Streptomyces",
        "minimal": False,
        "similarity": 2,
        "species": "coelicolor",
        "start_pos": 86636,
        "strain": "A3(2)",
        "term": "hgle-ks - t1pks hybrid",
        "version": 3
    }

    results = client.get(url_for('show_genome', identifier='nc_003888'))
    assert results.status_code == 200
    results.json[0].pop("bgc_id")  # a generated value dependent on database construction order
    assert results.json[0] == expected


def test_available(client):
    '''Test /api/v1.0/available/<category>/<term> endpoint'''
    expected = [{'val': 'Streptomonospora', 'desc': None}, {'val': 'Streptomyces', 'desc': None}]
    results = client.get(url_for('list_available', category='genus', term='streptom'))
    assert results.status_code == 200
    assert results.json == expected
