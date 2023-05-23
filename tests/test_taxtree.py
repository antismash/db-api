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
            'children': True,
            'id': 'phylum_bacteria_actinobacteria',
            'parent': 'superkingdom_bacteria',
            'state': {'disabled': True},
            'text': 'Actinobacteria (1)'
        },
        {
            "children": True,
            "id": "phylum_bacteria_actinomycetota",
            "parent": "superkingdom_bacteria",
            "state": {"disabled": True},
            "text": "Actinomycetota (1)",
        },
    ]
    ret = get_phylum(['bacteria'])
    assert ret == expected


def test_get_class(session):
    expected = [{
        'children': True,
        'id': 'class_bacteria_actinobacteria_actinobacteria',
        'parent': 'phylum_bacteria_actinobacteria',
        'state': {'disabled': True},
        'text': 'Actinobacteria (1)'
    }]
    ret = get_class(['bacteria', 'actinobacteria'])
    assert ret == expected


def test_get_order(session):
    expected = [{
        'children': True,
        'id': 'order_bacteria_actinobacteria_actinobacteria_streptomycetales',
        'parent': 'class_bacteria_actinobacteria_actinobacteria',
        'state': {'disabled': True},
        'text': 'Streptomycetales (1)'
    }]
    ret = get_order(['bacteria', 'actinobacteria', 'actinobacteria'])
    assert ret == expected


def test_get_family(session):
    expected = [{
        'children': True,
        'id': 'family_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae',
        'parent': 'order_bacteria_actinobacteria_actinobacteria_streptomycetales',
        'state': {'disabled': True},
        'text': 'Streptomycetaceae (1)'
    }]
    ret = get_family(['bacteria', 'actinobacteria', 'actinobacteria', 'streptomycetales'])
    assert ret == expected


def test_get_genus(session):
    expected = [{
        'children': True,
        'id': 'genus_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae_streptomyces',
        'parent': 'family_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae',
        'state': {'disabled': True},
        'text': 'Streptomyces (1)'
    }]
    ret = get_genus(['bacteria', 'actinobacteria', 'actinobacteria', 'streptomycetales', 'streptomycetaceae'])
    assert ret == expected


def test_get_species(session):
    expected = [{
        'children': True,
        'id': 'species_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae_streptomyces_coelicolor',
        'parent': 'genus_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae_streptomyces',
        'state': {'disabled': True},
        'text': 'coelicolor (1)'
    },
    ]
    ret = get_species(['bacteria', 'actinobacteria', 'actinobacteria', 'streptomycetales', 'streptomycetaceae', 'streptomyces'])
    assert ret == expected


def test_get_strain(session):
    expected = [{
        'assembly_id': 'GCF_000203835.1',
        'id': 'gcf_000203835.1',
        'li_attr': {'data-assembly': 'GCF_000203835.1'},
        'parent': 'species_bacteria_actinobacteria_actinobacteria_streptomycetales_streptomycetaceae_streptomyces_coelicolor',
        'text': 'Streptomyces coelicolor CFB_NBC_0001 GCF_000203835.1',
        'type': 'strain'
    }]
    ret = get_strains(['bacteria', 'actinobacteria', 'actinobacteria', 'streptomycetales', 'streptomycetaceae', 'streptomyces', 'coelicolor'])
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
