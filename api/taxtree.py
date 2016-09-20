'''Functions related to building the taxonomic tree data'''
import sql


def get_superkingdom(cur):
    '''Get list of superkingdoms'''
    tree = []
    cur.execute(sql.TAXTREE_SUPERKINGOM)
    kingdoms = cur.fetchall()
    for kingdom in kingdoms:
        tree.append(_create_tree_node('superkingdom_{}'.format(kingdom[0].lower()),
                                      '#', kingdom[0]))

    return tree


def get_phylum(cur, params):
    '''Get list of phyla per kingdom'''
    tree = []
    cur.execute(sql.TAXTREE_PHYLUM, params)
    phyla = cur.fetchall()
    for phylum in phyla:
        id_list = params + [phylum[0].lower()]
        tree.append(_create_tree_node('phylum_{}'.format('_'.join(id_list)),
                                      'superkingdom_{}'.format('_'.join(params)),
                                      phylum[0]))

    return tree


def get_class(cur, params):
    '''Get list of classes per kingdom/phylum'''
    tree = []
    cur.execute(sql.TAXTREE_CLASS, params)
    classes = cur.fetchall()
    for cls in classes:
        id_list = params + [cls[0].lower()]
        tree.append(_create_tree_node('class_{}'.format('_'.join(id_list)),
                                      'phylum_{}'.format('_'.join(params)),
                                      cls[0]))
    return tree


def get_order(cur, params):
    '''Get list of oders per kingdom/phylum/class'''
    tree = []
    cur.execute(sql.TAXTREE_ORDER, params)
    orders = cur.fetchall()
    for order in orders:
        id_list = params + [order[0].lower()]
        tree.append(_create_tree_node('order_{}'.format('_'.join(id_list)),
                                      'class_{}'.format('_'.join(params)),
                                      order[0]))
    return tree


def get_family(cur, params):
    '''Get list of families per kingdom/phylum/class/order'''
    tree = []
    cur.execute(sql.TAXTREE_FAMILY, params)
    families = cur.fetchall()
    for family in families:
        id_list = params + [family[0].lower()]
        tree.append(_create_tree_node('family_{}'.format('_'.join(id_list)),
                                      'order_{}'.format('_'.join(params)),
                                      family[0]))
    return tree


def get_genus(cur, params):
    '''Get list of genera per kingdom/phylum/class/order/family'''
    tree = []
    cur.execute(sql.TAXTREE_GENUS, params)
    genera = cur.fetchall()
    for genus in genera:
        id_list = params + [genus[0].lower()]
        tree.append(_create_tree_node('genus_{}'.format('_'.join(id_list)),
                                      'family_{}'.format('_'.join(params)),
                                      genus[0]))
    return tree


def get_species(cur, params):
    '''Get list of species per kingdom/phylum/class/order/family/genus'''
    tree = []
    cur.execute(sql.TAXTREE_SPECIES, params)
    strains = cur.fetchall()
    for strain in strains:
        tree.append(_create_tree_node('{}'.format(strain.acc.lower()),
                                      'genus_{}'.format('_'.join(params)),
                                      '{} {}.{}'.format(strain.species, strain.acc, strain.version),
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
