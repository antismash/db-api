'''Functions related to building the taxonomic tree data'''
from .models import (
    db,
    DnaSequence,
    Genome,
    Taxa,
)


def get_superkingdom():
    '''Get list of superkingdoms'''
    tree = []
    kingdoms = db.session.query(Taxa.superkingdom).group_by(Taxa.superkingdom).order_by(Taxa.superkingdom)
    for kingdom in kingdoms:
        tree.append(_create_tree_node('superkingdom_{}'.format(kingdom[0].lower()),
                                      '#', kingdom[0]))

    return tree


def get_phylum(params):
    '''Get list of phyla per kingdom'''
    tree = []
    phyla = db.session.query(Taxa.phylum).filter(Taxa.superkingdom.ilike(params[0])) \
                      .group_by(Taxa.phylum).order_by(Taxa.phylum)
    for phylum in phyla:
        id_list = params + [phylum[0].lower()]
        tree.append(_create_tree_node('phylum_{}'.format('_'.join(id_list)),
                                      'superkingdom_{}'.format('_'.join(params)),
                                      phylum[0]))

    return tree


def get_class(params):
    '''Get list of classes per kingdom/phylum'''
    tree = []
    classes = db.session.query(Taxa._class) \
                        .filter(Taxa.superkingdom.ilike(params[0])) \
                        .filter(Taxa.phylum.ilike(params[1])) \
                        .group_by(Taxa._class).order_by(Taxa._class)
    for cls in classes:
        id_list = params + [cls[0].lower()]
        tree.append(_create_tree_node('class_{}'.format('_'.join(id_list)),
                                      'phylum_{}'.format('_'.join(params)),
                                      cls[0]))
    return tree


def get_order(params):
    '''Get list of oders per kingdom/phylum/class'''
    tree = []
    orders = db.session.query(Taxa.taxonomic_order) \
                       .filter(Taxa.superkingdom.ilike(params[0])) \
                       .filter(Taxa.phylum.ilike(params[1])) \
                       .filter(Taxa._class.ilike(params[2])) \
                       .group_by(Taxa.taxonomic_order).order_by(Taxa.taxonomic_order)
    for order in orders:
        id_list = params + [order[0].lower()]
        tree.append(_create_tree_node('order_{}'.format('_'.join(id_list)),
                                      'class_{}'.format('_'.join(params)),
                                      order[0]))
    return tree


def get_family(params):
    '''Get list of families per kingdom/phylum/class/order'''
    tree = []
    families = db.session.query(Taxa.family) \
                         .filter(Taxa.superkingdom.ilike(params[0])) \
                         .filter(Taxa.phylum.ilike(params[1])) \
                         .filter(Taxa._class.ilike(params[2])) \
                         .filter(Taxa.taxonomic_order.ilike(params[3])) \
                         .group_by(Taxa.family).order_by(Taxa.family)
    for family in families:
        id_list = params + [family[0].lower()]
        tree.append(_create_tree_node('family_{}'.format('_'.join(id_list)),
                                      'order_{}'.format('_'.join(params)),
                                      family[0]))
    return tree


def get_genus(params):
    '''Get list of genera per kingdom/phylum/class/order/family'''
    tree = []
    genera = db.session.query(Taxa.genus) \
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
                                      genus[0]))
    return tree


def get_species(params):
    '''Get list of species per kingdom/phylum/class/order/family/genus'''
    tree = []
    species = db.session.query(Taxa.species) \
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
                                      sp[0]))

    return tree


def get_strains(params):
    '''Get list of strains per kingdom/phylum/class/order/family/genus/species'''
    tree = []
    strains = db.session.query(Taxa.tax_id, Taxa.genus, Taxa.species, Taxa.strain,
                               DnaSequence.acc, DnaSequence.version) \
                        .join(Genome).join(DnaSequence) \
                        .filter(Taxa.superkingdom.ilike(params[0])) \
                        .filter(Taxa.phylum.ilike(params[1])) \
                        .filter(Taxa._class.ilike(params[2])) \
                        .filter(Taxa.taxonomic_order.ilike(params[3])) \
                        .filter(Taxa.family.ilike(params[4])) \
                        .filter(Taxa.genus.ilike(params[5])) \
                        .filter(Taxa.species.ilike(params[6])) \
                        .order_by(Taxa.strain)
    for strain in strains:
        tree.append(_create_tree_node('{}'.format(strain.acc.lower()),
                                      'species_{}'.format('_'.join(params)),
                                      '{s.genus} {s.species} {s.strain} {s.acc}.{s.version}'.format(s=strain),
                                      disabled=False, leaf=True))
    return tree


def _create_tree_node(node_id, parent, text, disabled=True, leaf=False):
    '''create a jsTree node structure'''
    ret = {}
    ret['id'] = node_id
    ret['parent'] = parent
    ret['text'] = text
    if disabled:
        ret['state'] = {'disabled': True}
    if leaf:
        ret['type'] = 'strain'
    else:
        ret['children'] = True
    return ret
