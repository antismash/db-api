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
        'text': 'Bacteria'
    }]
    ret = get_superkingdom()
    assert ret == expected


def test_get_phylum(session):
    expected = [
        {'children': True,
         'id': 'phylum_bacteria_actinobacteria',
         'parent': 'superkingdom_bacteria',
         'state': {'disabled': True},
         'text': 'Actinobacteria'},
        {'children': True,
         'id': 'phylum_bacteria_firmicutes',
         'parent': 'superkingdom_bacteria',
         'state': {'disabled': True},
         'text': 'Firmicutes'},
    ]
    ret = get_phylum(['bacteria'])
    assert ret == expected


def test_get_class(session):
    expected = [{
        'children': True,
        'id': 'class_bacteria_firmicutes_bacilli',
        'parent': 'phylum_bacteria_firmicutes',
        'state': {'disabled': True},
        'text': 'Bacilli'
    }]
    ret = get_class(['bacteria', 'firmicutes'])
    assert ret == expected


def test_get_order(session):
    expected = [{
        'children': True,
        'id': 'order_bacteria_firmicutes_bacilli_lactobacillales',
        'parent': 'class_bacteria_firmicutes_bacilli',
        'state': {'disabled': True},
        'text': 'Lactobacillales'
    }]
    ret = get_order(['bacteria', 'firmicutes', 'bacilli'])
    assert ret == expected


def test_get_family(session):
    expected = [{
        'children': True,
        'id': 'family_bacteria_firmicutes_bacilli_lactobacillales_streptococcaceae',
        'parent': 'order_bacteria_firmicutes_bacilli_lactobacillales',
        'state': {'disabled': True},
        'text': 'Streptococcaceae'
    }]
    ret = get_family(['bacteria', 'firmicutes', 'bacilli', 'lactobacillales'])
    assert ret == expected


def test_get_genus(session):
    expected = [{
        'children': True,
        'id': 'genus_bacteria_firmicutes_bacilli_lactobacillales_streptococcaceae_lactococcus',
        'parent': 'family_bacteria_firmicutes_bacilli_lactobacillales_streptococcaceae',
        'state': {'disabled': True},
        'text': 'Lactococcus'
    }]
    ret = get_genus(['bacteria', 'firmicutes', 'bacilli', 'lactobacillales', 'streptococcaceae'])
    assert ret == expected


def test_get_species(session):
    expected = [{
        'children': True,
        'id': 'species_bacteria_firmicutes_bacilli_lactobacillales_streptococcaceae_lactococcus_lactis',
        'parent': 'genus_bacteria_firmicutes_bacilli_lactobacillales_streptococcaceae_lactococcus',
        'state': {'disabled': True},
        'text': 'lactis'
    }]
    ret = get_species(['bacteria', 'firmicutes', 'bacilli', 'lactobacillales', 'streptococcaceae', 'lactococcus'])
    assert ret == expected


def test_get_strain(session):
    expected = [{
        'id': 'nc_017486',
        'parent': 'species_bacteria_firmicutes_bacilli_lactobacillales_streptococcaceae_lactococcus_lactis',
        'text': 'Lactococcus lactis CV56 NC_017486.1',
        'type': 'strain'
    }]
    ret = get_strains(['bacteria', 'firmicutes', 'bacilli', 'lactobacillales', 'streptococcaceae', 'lactococcus', 'lactis'])
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
        'id': 'testid',
        'parent': 'parent',
        'text': 'cool text',
        'type': 'strain'
    }
    node = _create_tree_node(expected['id'], expected['parent'], expected['text'], disabled=False, leaf=True)
    assert node == expected
