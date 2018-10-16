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
        "clusters": [
            {
                "count": 6,
                "description": "Terpene",
                "name": "terpene"
            },
            {
                "count": 4,
                "description": "Nonribosomal peptide",
                "name": "nrps"
            },
            {
                "count": 4,
                "description": "Type I polyketide",
                "name": "t1pks"
            },
            {
                "count": 3,
                "description": "Lanthipeptide",
                "name": "lantipeptide"
            },
            {
                "count": 3,
                "description": "Siderophore",
                "name": "siderophore"
            },
            {
                "count": 2,
                "description": "Bacteriocin or other unspecified RiPP",
                "name": "bacteriocin"
            },
            {
                "count": 2,
                "description": "Butyrolactone",
                "name": "butyrolactone"
            },
            {
                "count": 2,
                "description": "hglE-type polyketide",
                "name": "otherks"
            },
            {
                "count": 2,
                "description": "Type II polyketide",
                "name": "t2pks"
            },
            {
                "count": 2,
                "description": "Type III polyketide",
                "name": "t3pks"
            },
            {
                "count": 1,
                "description": "Ectoine",
                "name": "ectoine"
            },
            {
                "count": 1,
                "description": "Furan",
                "name": "furan"
            },
            {
                "count": 1,
                "description": "Indole",
                "name": "indole"
            },
            {
                "count": 1,
                "description": "Melanin",
                "name": "melanin"
            },
            {
                "count": 1,
                "description": "Other",
                "name": "other"
            }
        ],
        "num_clusters": 29,
        "num_genomes": 2,
        "num_sequences": 293,
        "top_secmet_assembly_id": "GCF_000203835.1",
        "top_secmet_species": "Streptomyces coelicolor A3(2)",
        "top_secmet_taxon": 100226,
        "top_secmet_taxon_count": 29,
        "top_seq_species": "Streptomyces Unclassified",
        "top_seq_taxon": 1158056,
        "top_seq_taxon_count": 290,

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
		"bgc_id": 29,
		"cbh_acc": "BGC0000914_c1",
		"cbh_description": "Methylenomycin",
		"cbh_rank": 1,
		"cluster_number": 29,
                "contig_edge": False,
		"description": "Hybrid cluster: Butyrolactone-Furan",
		"end_pos": 253262,
		"genus": "Streptomyces",
                "minimal": False,
		"similarity": 61,
		"species": "coelicolor",
		"start_pos": 239259,
		"strain": "A3(2)",
		"term": "butyrolactone-furan hybrid",
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
    assert results.json == expected

    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'type', 'term': 'furan'}}}
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
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
    expected_csv = b'''#Genus\tSpecies\tStrain\tNCBI accession\tCluster number\tBGC type\tFrom\tTo\tOn contig edge\tFast mode only\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL\tDownload URL
Streptomyces\tcoelicolor\tA3(2)\tNC_003903.1\t29\tbutyrolactone-furan hybrid\t239259\t253262\tFalse\tFalse\tMethylenomycin\t61\tBGC0000914_c1\thttps://antismash-db.secondarymetabolites.org/go/GCF_000203835/29\thttps://antismash-db.secondarymetabolites.org/api/v1.0/download/genbank/GCF_000203835/cluster/29
'''

    expected_json = [{
        "acc": "NC_003903",
        "assembly_id": "GCF_000203835",
        "bgc_id": 29,
        "cbh_acc": "BGC0000914_c1",
        "cbh_description": "Methylenomycin",
        "cbh_rank": 1,
        "cluster_number": 29,
        "contig_edge": False,
        "description": "Hybrid cluster: Butyrolactone-Furan",
        "end_pos": 253262,
        "genus": "Streptomyces",
        "minimal": False,
        "similarity": 61,
        "species": "coelicolor",
        "start_pos": 239259,
        "strain": "A3(2)",
        "term": "butyrolactone-furan hybrid",
        "version": 1
    }]

    expected_fasta = b'''>NC_003903.1|Cluster 29|butyrolactone-furan|239259-253262|Streptomyces coelicolor A3(2)
AGCAACGGCCGGATCACTCACCCCCTGATGCGAAGGAGAACGGCATGACCGAGTCCACGGTCGCGCGCATCATCGCGATC'''

    results = client.post(url_for('export'), data='{"search_string": "[type]furan"}', content_type="application/json")
    assert results.status_code == 200
    assert results.data == expected_csv

    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'type', 'term': 'furan'}}}
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert results.json == expected_json

    query['query']['return_type'] = 'csv'
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert results.data == expected_csv

    query['query']['return_type'] = 'fasta'
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
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
        "bgc_id": 1,
        "cbh_acc": "BGC0001101_c1",
        "cbh_description": "Leinamycin",
        "cbh_rank": 1,
        "cluster_number": 1,
        "contig_edge": False,
        "description": "Hybrid cluster: Type I polyketide-hglE-type polyketide",
        "end_pos": 139654,
        "genus": "Streptomyces",
        "minimal": False,
        "similarity": 2,
        "species": "coelicolor",
        "start_pos": 86636,
        "strain": "A3(2)",
        "term": "t1pks-otherks hybrid",
        "version": 3
    }

    results = client.get(url_for('show_genome', identifier='nc_003888'))
    assert results.status_code == 200
    assert results.json[0] == expected


def test_available(client):
    '''Test /api/v1.0/available/<category>/<term> endpoint'''
    expected = [{'val': 'Streptomyces', 'desc': None}]
    results = client.get(url_for('list_available', category='genus', term='streptom'))
    assert results.status_code == 200
    assert results.json == expected
