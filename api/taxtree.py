'''Functions related to building the taxonomic tree data'''
from sqlalchemy import func, or_

from .models import (
    db,
    DnaSequence,
    Genome,
    Taxa,
)


def search(search_term):
    """Get a taxtree path where genus, species or strain matches the search term."""
    kingdoms = set()
    phyla = set()
    classes = set()
    orders = set()
    families = set()
    genera = set()
    species = set()

    filters = [
       Taxa.genus.ilike('%{}%'.format(search_term)),
       Taxa.species.ilike('%{}%'.format(search_term)),
       Taxa.strain.ilike('%{}%'.format(search_term)),
    ]

    hits = db.session.query(Taxa.superkingdom, Taxa.phylum, Taxa._class, Taxa.taxonomic_order,
                            Taxa.family, Taxa.genus, Taxa.species) \
                     .filter(or_(*filters))
    for hit in hits.all():
        kingdoms.add('superkingdom_{}'.format(hit[0]).lower())
        phyla.add('phylum_{}'.format('_'.join(hit[0:2])).lower())
        classes.add('class_{}'.format('_'.join(hit[0:3])).lower())
        orders.add('order_{}'.format('_'.join(hit[0:4])).lower())
        families.add('family_{}'.format('_'.join(hit[0:5])).lower())
        genera.add('genus_{}'.format('_'.join(hit[0:6])).lower())
        species.add('species_{}'.format('_'.join(hit[0:7])).lower())

    tax_path = sorted(list(kingdoms))
    tax_path.extend(sorted(list(phyla)))
    tax_path.extend(sorted(list(classes)))
    tax_path.extend(sorted(list(orders)))
    tax_path.extend(sorted(list(families)))
    tax_path.extend(sorted(list(genera)))
    tax_path.extend(sorted(list(species)))

    return tax_path


def get_superkingdom():
    '''Get list of superkingdoms'''
    tree = []
    kingdoms = db.session.query(Taxa.superkingdom, func.count(Genome.assembly_id)) \
                         .join(Genome) \
                         .group_by(Taxa.superkingdom).order_by(Taxa.superkingdom)
    for kingdom in kingdoms:
        tree.append(_create_tree_node('superkingdom_{}'.format(kingdom[0].lower()),
                                      '#', '{} ({})'.format(kingdom[0], kingdom[1])))

    return tree


def get_phylum(params):
    '''Get list of phyla per kingdom'''
    tree = []
    phyla = db.session.query(Taxa.phylum, func.count(Genome.assembly_id)) \
                      .join(Genome) \
                      .filter(Taxa.superkingdom.ilike(params[0])) \
                      .group_by(Taxa.phylum).order_by(Taxa.phylum)
    for phylum in phyla:
        id_list = params + [phylum[0].lower()]
        tree.append(_create_tree_node('phylum_{}'.format('_'.join(id_list)),
                                      'superkingdom_{}'.format('_'.join(params)),
                                      '{} ({})'.format(phylum[0], phylum[1])))

    return tree


def get_class(params):
    '''Get list of classes per kingdom/phylum'''
    tree = []
    classes = db.session.query(Taxa._class, func.count(Genome.assembly_id)) \
                        .join(Genome) \
                        .filter(Taxa.superkingdom.ilike(params[0])) \
                        .filter(Taxa.phylum.ilike(params[1])) \
                        .group_by(Taxa._class).order_by(Taxa._class)
    for cls in classes:
        id_list = params + [cls[0].lower()]
        tree.append(_create_tree_node('class_{}'.format('_'.join(id_list)),
                                      'phylum_{}'.format('_'.join(params)),
                                      '{} ({})'.format(cls[0], cls[1])))
    return tree


def get_order(params):
    '''Get list of oders per kingdom/phylum/class'''
    tree = []
    orders = db.session.query(Taxa.taxonomic_order, func.count(Genome.assembly_id)) \
                       .join(Genome) \
                       .filter(Taxa.superkingdom.ilike(params[0])) \
                       .filter(Taxa.phylum.ilike(params[1])) \
                       .filter(Taxa._class.ilike(params[2])) \
                       .group_by(Taxa.taxonomic_order).order_by(Taxa.taxonomic_order)
    for order in orders:
        id_list = params + [order[0].lower()]
        tree.append(_create_tree_node('order_{}'.format('_'.join(id_list)),
                                      'class_{}'.format('_'.join(params)),
                                      '{} ({})'.format(order[0], order[1])))
    return tree


def get_family(params):
    '''Get list of families per kingdom/phylum/class/order'''
    tree = []
    families = db.session.query(Taxa.family, func.count(Genome.assembly_id)) \
                         .join(Genome) \
                         .filter(Taxa.superkingdom.ilike(params[0])) \
                         .filter(Taxa.phylum.ilike(params[1])) \
                         .filter(Taxa._class.ilike(params[2])) \
                         .filter(Taxa.taxonomic_order.ilike(params[3])) \
                         .group_by(Taxa.family).order_by(Taxa.family)
    for family in families:
        id_list = params + [family[0].lower()]
        tree.append(_create_tree_node('family_{}'.format('_'.join(id_list)),
                                      'order_{}'.format('_'.join(params)),
                                      '{} ({})'.format(family[0], family[1])))
    return tree


def get_genus(params):
    '''Get list of genera per kingdom/phylum/class/order/family'''
    tree = []
    genera = db.session.query(Taxa.genus, func.count(Genome.assembly_id)) \
                       .join(Genome) \
                       .filter(Taxa.superkingdom.ilike(params[0])) \
                       .filter(Taxa.phylum.ilike(params[1])) \
                       .filter(Taxa._class.ilike(params[2])) \
                       .filter(Taxa.taxonomic_order.ilike(params[3])) \
                       .filter(Taxa.family.ilike(params[4])) \
                       .group_by(Taxa.genus).order_by(Taxa.genus)
    for genus in genera:
        id_list = params + [genus[0].lower()]
        tree.append(_create_tree_node('genus_{}'.format('_'.join(id_list)),
                                      'family_{}'.format('_'.join(params)),
                                      '{} ({})'.format(genus[0], genus[1])))
    return tree


def get_species(params):
    '''Get list of species per kingdom/phylum/class/order/family/genus'''
    tree = []
    species = db.session.query(Taxa.species, func.count(Genome.assembly_id)) \
                        .join(Genome) \
                        .filter(Taxa.superkingdom.ilike(params[0])) \
                        .filter(Taxa.phylum.ilike(params[1])) \
                        .filter(Taxa._class.ilike(params[2])) \
                        .filter(Taxa.taxonomic_order.ilike(params[3])) \
                        .filter(Taxa.family.ilike(params[4])) \
                        .filter(Taxa.genus.ilike(params[5])) \
                        .group_by(Taxa.species).order_by(Taxa.species)
    for sp in species:
        id_list = params + [sp[0].lower()]
        tree.append(_create_tree_node('species_{}'.format('_'.join(id_list)),
                                      'genus_{}'.format('_'.join(params)),
                                      '{} ({})'.format(sp[0], sp[1])))

    return tree


def get_strains(params):
    '''Get list of strains per kingdom/phylum/class/order/family/genus/species'''
    tree = []
    strains = db.session.query(Taxa.tax_id, Taxa.genus, Taxa.species, Taxa.strain,
                               Genome.assembly_id) \
                        .join(Genome) \
                        .filter(Taxa.superkingdom.ilike(params[0])) \
                        .filter(Taxa.phylum.ilike(params[1])) \
                        .filter(Taxa._class.ilike(params[2])) \
                        .filter(Taxa.taxonomic_order.ilike(params[3])) \
                        .filter(Taxa.family.ilike(params[4])) \
                        .filter(Taxa.genus.ilike(params[5])) \
                        .filter(Taxa.species.ilike(params[6])) \
                        .order_by(Taxa.strain)
    for strain in strains:
        tree.append(_create_tree_node('{}'.format(strain.assembly_id.lower()),
                                      'species_{}'.format('_'.join(params)),
                                      '{s.genus} {s.species} {s.strain} {s.assembly_id}'.format(s=strain),
                                      assembly_id=strain.assembly_id,
                                      disabled=False, leaf=True))
    return tree


def _create_tree_node(node_id, parent, text, assembly_id=None, disabled=True, leaf=False):
    '''create a jsTree node structure'''
    ret = {}
    ret['id'] = node_id
    ret['parent'] = parent
    ret['text'] = text
    if disabled:
        ret['state'] = {'disabled': True}
    if leaf:
        ret['type'] = 'strain'
        ret['assembly_id'] = assembly_id
        ret['li_attr'] = {"data-assembly": assembly_id}
    else:
        ret['children'] = True
    return ret
