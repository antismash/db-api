import json
from flask import url_for
from api import api, taxtree

from .test_clusters import SCO_STRAIN


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
        'num_sequences': 96,
        'top_seq_taxon_count': 94,
        'top_seq_species': 'Streptomyces Unclassified',
        'num_genomes': 2,
        'top_secmet_assembly_id': 'GCF_000203835.1',
        'num_clusters': 49,  # TODO *regions, should this be named differently
        'top_secmet_species': f'Streptomyces coelicolor {SCO_STRAIN}',
        'top_secmet_taxon_count': 26,
        'top_seq_taxon': 2,  # arbitrary, values depend on when this taxon was inserted
        'top_secmet_taxon': 1,  # arbitrary, values depend on when this taxon was inserted
        'clusters': [
            {'name': 't1pks', 'count': 59, 'description': 'Type I polyketide'},
            {'name': 'nrps', 'count': 23, 'description': 'Non-ribosomal peptide synthase'},
            {'name': 'terpene', 'count': 14, 'description': 'Terpene'},
            {'name': 'nrps-like', 'count': 8, 'description': 'NRPS-like fragments'},
            {'name': 'ni-siderophore', 'count': 6, 'description': 'NRPS-independent IucA/IucC-like siderophores'},
            {'name': 't3pks', 'count': 6, 'description': 'Type III polyketide'},
            {'name': 'butyrolactone', 'count': 5, 'description': 'Butyrolactone'},
            {'name': 'lanthipeptide-class-i', 'count': 3, 'description': 'Class I Lanthipeptide'},
            {'name': 'other', 'count': 3, 'description': 'Other, non-categorised secondary metabolites'},
            {'name': 'ripp-like', 'count': 3, 'description': 'Fallback rule containing known RiPP-related profiles'},
            {'name': 't2pks', 'count': 3, 'description': 'Type II polyketide'},
            {'name': 'arylpolyene', 'count': 2, 'description': 'Aryl polyene'},
            {'name': 'betalactone', 'count': 2, 'description': 'beta-lactone containing protease inhibitor'},
            {'name': 'ectoine', 'count': 2, 'description': 'Ectoine'},
            {'name': 'hgle-ks', 'count': 2, 'description': 'heterocyst glycolipid synthase like PKS'},
            {'name': 'indole', 'count': 2, 'description': 'Indole'},
            {'name': 'ladderane', 'count': 2, 'description': 'Ladderane'},
            {'name': 'lassopeptide', 'count': 2, 'description': 'Lasso peptide'},
            {'name': 'nrp-metallophore', 'count': 2, 'description': 'Non-ribosomal peptide metallophores'},
            {'name': 'pks-like', 'count': 2, 'description': 'PKS-like fragments'},
            {'name': 'blactam', 'count': 1, 'description': 'beta-lactam'},
            {'name': 'furan', 'count': 1, 'description': 'Furan'},
            {'name': 'hserlactone', 'count': 1, 'description': 'Homoserine lactone'},
            {'name': 'lanthipeptide-class-ii', 'count': 1, 'description': 'Class II Lanthipeptide'},
            {'name': 'lanthipeptide-class-iii', 'count': 1, 'description': 'Class III Lanthipeptide'},
            {'name': 'linaridin', 'count': 1, 'description': 'Linear arid peptides'},
            {'name': 'melanin', 'count': 1, 'description': 'Melanin'},
            {'name': 'prodigiosin', 'count': 1, 'description': 'Serratia-type nontraditional PKS prodigiosin biosynthesis pathway'},
            {'name': 'redox-cofactor', 'count': 1, 'description': 'Redox-cofactors'},
            {'name': 'thioamide-nrp', 'count': 1, 'description': 'thioamide-containing non-ribosomal peptides'},
        ]
    }

    results = client.get(url_for('get_stats_v2'))
    assert results.status_code == 200
    assert results.json == expected


def test_categories_have_filters():
    def check_filter(filt):
        value = filt["value"]
        assert isinstance(value, str)
        assert " " not in value and value.lower() == value
        assert isinstance(filt["label"], str)
        assert isinstance(filt["type"], str)
        for key in filt.get("choices", {}):
            assert isinstance(key, str)

    def check_option(option):
        assert len(set(option).intersection({"label", "value", "type"})) == 3
        for filt in option.get("filters", {}):
            check_filter(filt)

    # categories is special, it doesn't use jsonify, so the payload needs to be
    # manually checked instead of just using .json
    data = json.loads(api.list_available_categories().data.decode())

    for key in ["options", "groups"]:
        assert data[key]
    assert len(data) == 2  # no extras, thank you
    for option in data["options"]:
        check_option(option)

    for group in data["groups"]:
        for key in ["options", "header"]:
            assert group[key], group
        assert isinstance(group["header"], str)
        for option in group["options"]:
            check_option(option)


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
                "assembly_id": "GCF_000203835.1",
                "cbh_acc": "BGC0000914",
                "cbh_description": "methylenomycin A",
                "cbh_rank": 1,
                "record_number": 2,
                "region_number": 2,
                "contig_edge": False,
                "description": "Hybrid region: Butyrolactone & Furan",
                "start_pos": 226409,
                "end_pos": 255381,
                "genus": "Streptomyces",
                "similarity": 100,
                "species": "coelicolor",
                "strain": SCO_STRAIN,
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
                    "Actinomycetota"
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

    url = url_for('search').replace("v2", "v1")
    results = client.post(url, data='{"search_string": "{[type|furan]}"}', content_type="application/json")
    assert results.status_code == 200
    assert results.json["clusters"][0].pop("bgc_id")
    assert results.json == expected

    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'type', 'value': 'furan'}}}
    results = client.post(url, data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert results.json["clusters"][0].pop("bgc_id")
    assert results.json == expected

    query['query']['return_type'] = 'csv'
    results = client.post(url, data=json.dumps(query), content_type="application/json")
    assert results.status_code == 400

    query['query'] = {}
    results = client.post(url, data=json.dumps(query), content_type="application/json")
    assert results.status_code == 400


def test_search_paginate(client):
    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'genus', 'value': 'Streptomyces'}},
             'paginate': 5}
    results = client.post(url_for('search'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    assert len(results.json['clusters']) == 5
    assert results.json['paginate'] == 5


def test_export(client):
    '''Test /api/v1.0/export endpoint'''

    expected_json = [{
        "acc": "NC_003903",
        "assembly_id": "GCF_000203835.1",
        "cbh_acc": "BGC0000914",
        "cbh_description": "methylenomycin A",
        "cbh_rank": 1,
        "record_number": 2,
        "region_number": 2,
        "contig_edge": False,
        "description": "Hybrid region: Butyrolactone & Furan",
        "start_pos": 226409,
        "end_pos": 255381,
        "genus": "Streptomyces",
        "similarity": 100,
        "species": "coelicolor",
        "strain": SCO_STRAIN,
        "term": "butyrolactone - furan hybrid",
        "version": 1
    }]
    json_with_helpers = dict(expected_json[0])
    json_with_helpers["url"] = "https://antismash-db.secondarymetabolites.org"

    expected_csv = (
        "#Genus\tSpecies\tStrain\tNCBI accession\tFrom\tTo\tBGC type\tOn contig edge"
        "\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL\tDownload URL\n"
        "{genus}\t{species}\t{strain}\t{acc}.{version}\t{start_pos}\t{end_pos}\t{term}\t{contig_edge}"
        "\t{cbh_description}\t{similarity}\t{cbh_acc}\t{url}/go/{acc}.{version}/{start_pos}-{end_pos}"
        "\t{url}/api/v1.0/download/genbank/{assembly_id}/{acc}/{version}/{start_pos}/{end_pos}\n"
    ).format(**json_with_helpers).encode()

    expected_fasta = (
        ">{acc}.{version}|{start_pos}-{end_pos}|butyrolactone - furan|{genus} {species} {strain}\n"
        "ACCGGCCTCGCCGTGACGCGGGTGCTCGGGGTCGAAGATCCCGTCCAGCGGGATCGACTTGCCCATGATGTCCGGCGGCA"
    ).format(**json_with_helpers).encode()


    query = {'query': {'search': 'cluster', 'return_type': 'json', 'terms': {'term_type': 'expr', 'category': 'type', 'value': 'furan'}}}
    results = client.post(url_for('export'), data=json.dumps(query), content_type="application/json")
    assert results.status_code == 200
    results.json[0].pop("bgc_id")  # arbitrary value, not useful for testing
    assert results.json == expected_json

    results = client.post(url_for('export'), data='{"search_string": "{[type|furan]}"}', content_type="application/json")
    assert results.status_code == 200
    assert results.data == expected_csv

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
        "assembly_id": "GCF_000203835.1",
        "cbh_acc": "BGC0001101",
        "cbh_description": "leinamycin",
        "cbh_rank": 1,
        "record_number": 1,
        "region_number": 1,
        "contig_edge": False,
        "description": "Hybrid region: heterocyst glycolipid synthase like PKS & Type I polyketide",
        "end_pos": 139654,
        "genus": "Streptomyces",
        "similarity": 2,
        "species": "coelicolor",
        "start_pos": 86636,
        "strain": SCO_STRAIN,
        "term": "hgle-ks - t1pks hybrid",
        "version": 3
    }

    results = client.get(url_for('show_genome', identifier='nc_003888'))
    assert results.status_code == 200
    results.json[0].pop("bgc_id")  # a generated value dependent on database construction order
    assert results.json[0] == expected


def test_available(client):
    '''Test /api/v1.0/available/<category>/<term> endpoint'''
    expected = [{'desc': None, 'val': 'coelicolor'}]
    results = client.get(url_for('list_available', category='species', term="coe"))
    assert results.status_code == 200
    assert results.json == expected


def test_available_filter_values(client):
    """ Test /api/v1.0/available_filter_values/<category>/<filter_name>/<term> """
    expected = [
        {"val": "cdps", "desc": "tRNA-dependent cyclodipeptide synthases"},
        {"val": "transat-pks", "desc": "Trans-AT polyketide"},
        {"val": "transat-pks-like", "desc": "transAT-PKS-like fragments"},
        {"val": "tropodithietic-acid", "desc": "tropodithietic acid like cluster"},
    ]
    results = client.get(url_for("list_available_filter_values", category="candidatekind", filter_name="bgctype", term="tr"))
    assert results.status_code == 200
    assert results.json == expected
