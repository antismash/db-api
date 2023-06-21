'''Test taxtree-related functions'''
from api.taxtree import (
    get_superkingdom,
    get_phylum,
    get_class,
    get_order,
    get_family,
    get_genus,
    get_species,
    get_strains,
    _create_tree_node,
)

from .test_clusters import SCO_STRAIN


def test_get_superkingdom(session):
    expected = [{
        'children': True,
        'id': 'superkingdom_bacteria',
        'parent': '#',
        'state': {'disabled': True},
        'text': 'Bacteria (2)'
    }]
    ret = get_superkingdom()
    assert ret == expected


def test_get_phylum(session):
    expected = [
        {
            "children": True,
            "id": "phylum_bacteria_actinomycetota",
            "parent": "superkingdom_bacteria",
            "state": {"disabled": True},
            "text": "Actinomycetota (2)",
        },
    ]
    ret = get_phylum(['bacteria'])
    assert ret == expected


def test_get_class(session):
    expected = [{
        'children': True,
        'id': 'class_bacteria_actinomycetota_actinomycetes',
        'parent': 'phylum_bacteria_actinomycetota',
        'state': {'disabled': True},
        'text': 'Actinomycetes (2)'
    }]
    ret = get_class(['bacteria', 'actinomycetota'])
    assert ret == expected


def test_get_order(session):
    expected = [{
        'children': True,
        'id': 'order_bacteria_actinomycetota_actinomycetes_kitasatosporales',
        'parent': 'class_bacteria_actinomycetota_actinomycetes',
        'state': {'disabled': True},
        'text': 'Kitasatosporales (2)'
    }]
    ret = get_order(['bacteria', 'actinomycetota', 'actinomycetes'])
    assert ret == expected


def test_get_family(session):
    expected = [{
        'children': True,
        'id': 'family_bacteria_actinomycetota_actinomycetes_kitasatosporales_streptomycetaceae',
        'parent': 'order_bacteria_actinomycetota_actinomycetes_kitasatosporales',
        'state': {'disabled': True},
        'text': 'Streptomycetaceae (2)'
    }]
    ret = get_family(['bacteria', 'actinomycetota', 'actinomycetes', 'kitasatosporales'])
    assert ret == expected


def test_get_genus(session):
    expected = [{
        'children': True,
        'id': 'genus_bacteria_actinomycetota_actinomycetes_kitasatosporales_streptomycetaceae_streptomyces',
        'parent': 'family_bacteria_actinomycetota_actinomycetes_kitasatosporales_streptomycetaceae',
        'state': {'disabled': True},
        'text': 'Streptomyces (2)'
    }]
    ret = get_genus(['bacteria', 'actinomycetota', 'actinomycetes', 'kitasatosporales', 'streptomycetaceae'])
    assert ret == expected


def test_get_species(session):
    expected = [{
        'children': True,
        'id': 'species_bacteria_actinomycetota_actinomycetes_kitasatosporales_streptomycetaceae_streptomyces_coelicolor',
        'parent': 'genus_bacteria_actinomycetota_actinomycetes_kitasatosporales_streptomycetaceae_streptomyces',
        'state': {'disabled': True},
        'text': 'coelicolor (1)'
    },
    {
        'children': True,
        'id': 'species_bacteria_actinomycetota_actinomycetes_kitasatosporales_streptomycetaceae_streptomyces_unclassified',
        'parent': 'genus_bacteria_actinomycetota_actinomycetes_kitasatosporales_streptomycetaceae_streptomyces',
        'state': {'disabled': True},
        'text': 'Unclassified (1)'},
    ]
    ret = get_species(['bacteria', 'actinomycetota', 'actinomycetes', 'kitasatosporales', 'streptomycetaceae', 'streptomyces'])
    assert ret == expected


def test_get_strain(session):
    expected = [{
        'assembly_id': 'GCF_000203835.1',
        'id': 'gcf_000203835.1',
        'li_attr': {'data-assembly': 'GCF_000203835.1'},
        'parent': 'species_bacteria_actinomycetota_actinomycetes_kitasatosporales_streptomycetaceae_streptomyces_coelicolor',
        'text': f'Streptomyces coelicolor {SCO_STRAIN} GCF_000203835.1',
        'type': 'strain'
    }]
    ret = get_strains(['bacteria', 'actinomycetota', 'actinomycetes', 'kitasatosporales', 'streptomycetaceae', 'streptomyces', 'coelicolor'])
    assert ret == expected


def test__create_tree_node(session):
    expected = {
        'id': 'testid',
        'parent': 'parent',
        'text': 'cool text',
        'state': {'disabled': True},
        'children': True
    }
    node = _create_tree_node(expected['id'], expected['parent'], expected['text'])
    assert node == expected

    expected = {
        'assembly_id': 'testid',
        'id': 'testid',
        'li_attr': {'data-assembly': 'testid'},
        'parent': 'parent',
        'text': 'cool text',
        'type': 'strain'
    }
    node = _create_tree_node(expected['id'], expected['parent'], expected['text'], assembly_id=expected['assembly_id'], disabled=False, leaf=True)
    assert node == expected
