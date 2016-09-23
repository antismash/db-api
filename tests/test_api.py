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
    '''Test /api/v1.0/stats endpoint'''

    expected = {
        "clusters": [
            {
                "count": 17,
                "description": "Nonribosomal peptide",
                "name": "nrps"
            },
            {
                "count": 15,
                "description": "Terpene",
                "name": "terpene"
            },
            {
                "count": 9,
                "description": "Type I polyketide",
                "name": "t1pks"
            },
            {
                "count": 6,
                "description": "Siderophore",
                "name": "siderophore"
            },
            {
                "count": 5,
                "description": "Bacteriocin or other unspecified RiPP",
                "name": "bacteriocin"
            },
            {
                "count": 5,
                "description": "Other",
                "name": "other"
            },
            {
                "count": 4,
                "description": "Lanthipeptide",
                "name": "lantipeptide"
            },
            {
                "count": 4,
                "description": "Type II polyketide",
                "name": "t2pks"
            },
            {
                "count": 4,
                "description": "Type III polyketide",
                "name": "t3pks"
            },
            {
                "count": 3,
                "description": "Ectoine",
                "name": "ectoine"
            },
            {
                "count": 3,
                "description": "Other types of polyketides",
                "name": "otherks"
            },
            {
                "count": 2,
                "description": "Butyrolactone",
                "name": "butyrolactone"
            },
            {
                "count": 2,
                "description": "Melanin",
                "name": "melanin"
            },
            {
                "count": 2,
                "description": "Trans-AT polyketide",
                "name": "transatpks"
            },
            {
                "count": 1,
                "description": "Pheganomycin-like ligase",
                "name": "fused"
            },
            {
                "count": 1,
                "description": "Indole",
                "name": "indole"
            },
            {
                "count": 1,
                "description": "Lasso peptide",
                "name": "lassopeptide"
            },
            {
                "count": 1,
                "description": "Polyunsaturated fatty acid",
                "name": "pufa"
            },
        ],
        "num_clusters": 73,
        "num_genomes": 4,
        "num_sequences": 6,
        "top_secmet_species": "cyaneogriseus",
        "top_secmet_taxon": 477245,
        "top_secmet_taxon_count": 32.0,
        "top_seq_taxon": 1435356,
        "top_seq_taxon_count": 3
    }


    results = client.get(url_for('get_stats'))
    assert results.status_code == 200
    assert results.json == expected


def test_sec_met_tree(client):
    '''Test /api/v1.0/tree/secmet endpoint'''

    expected = [
        {
            "id": "nrps",
            "parent": "#",
            "state": {
                "disabled": True
            },
            "text": "Nonribosomal peptide"
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
                "acc": "CP010849",
                "bgc_id": 54,
                "cbh_acc": "BGC0000709_c1",
                "cbh_description": "Neomycin_biosynthetic_gene_cluster",
                "cluster_number": 13,
                "description": "Lasso peptide",
                "end_pos": 4616468,
                "genus": "Streptomyces",
                "similarity": 8,
                "species": "cyaneogriseus",
                "start_pos": 4593898,
                "strain": "NMWT 1",
                "term": "lassopeptide",
                "version": 1
            }
        ],
        "offset": 0,
        "paginate": 50,
        "stats": {
            "clusters_by_phylum": {
                "data": [1],
                "labels": ["Actinobacteria"]
            },
            "clusters_by_type": {
                "data": [1],
                "labels": ["lassopeptide"]
            }
        },
        "total": 1
    }

    results = client.post(url_for('search'), data='{"search_string": "[type]lassopeptide"}', content_type="application/json")
    assert results.status_code == 200
    assert results.json == expected

    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'type', 'term': 'lassopeptide'}}}
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert results.json == expected

    query['query']['return_type'] = 'csv'
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 400

    query['query'] = {}
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 400


def test_export(client):
    '''Test /api/v1.0/export endpoint'''
    expected = '''#Genus\tSpecies\tNCBI accession\tCluster number\tBGC type\tFrom\tTo\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL
Streptomyces\tcyaneogriseus\tCP010849.1\t13\tlassopeptide\t4593898\t4616468\tNeomycin_biosynthetic_gene_cluster\t8\tBGC0000709_c1\thttp://antismash-db.secondarymetabolites.org/output/CP010849/index.html#cluster-13
'''

    results = client.post(url_for('export'), data='{"search_string": "[type]lassopeptide"}', content_type="application/json")
    assert results.status_code == 200
    assert results.data == expected


def test_genome(client):
    '''Test /api/v1.0/genome/<identifier> endpoint'''
    expected = [{
        "acc": "NC_017486",
        "bgc_id": 1,
        "cbh_acc": "BGC0000535_c1",
        "cbh_description": "Nisin_A_biosynthetic_gene_cluster",
        "cluster_number": 1,
        "description": "Lanthipeptide",
        "end_pos": 620999,
        "genus": "Lactococcus",
        "similarity": 100,
        "species": "lactis",
        "start_pos": 594686,
        "strain": "CV56",
        "term": "lantipeptide",
        "version": 1
    }]

    results = client.get(url_for('show_genome', identifier='nc_017486'))
    assert results.status_code == 200
    assert results.json == expected


def test_available(client):
    '''Test /api/v1.0/available/<category>/<term> endpoint'''
    expected = [['Lactococcus']]
    results = client.get(url_for('list_available', category='genus', term='l'))
    assert results.status_code == 200
    assert results.json == expected
