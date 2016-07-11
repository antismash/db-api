'''The API calls'''

from flask import (
    jsonify,
    request,
)
from . import app
from .helpers import get_db
from .search import search_bgcs
import sql


@app.route('/api/v1.0/version')
def get_version():
    '''display the API version'''
    from .version import __version__ as api_version
    ret = {
        'api': api_version
    }
    return jsonify(ret)


@app.route('/api/v1.0/stats')
def get_stats():
    '''contents for the stats page'''
    cur = get_db().cursor()
    cur.execute(sql.STATS_CLUSTER_COUNT)
    num_clusters = cur.fetchone()[0]

    cur.execute(sql.STATS_GENOME_COUNT)
    num_genomes = cur.fetchone()[0]

    cur.execute(sql.STATS_SEQUENCE_COUNT)
    num_sequences = cur.fetchone()[0]

    clusters = []

    cur.execute(sql.STATS_COUNTS_BY_TYPE)
    ret = cur.fetchall()
    for cluster in ret:
        clusters.append({'name': cluster.term, 'description': cluster.description, 'count': cluster.count})


    cur.execute(sql.STATS_TAXON_SEQUENCES)
    ret = cur.fetchone()
    top_seq_taxon = ret.tax_id
    top_seq_taxon_count = ret.tax_count


    cur.execute(sql.STATS_TAXON_SECMETS)
    ret = cur.fetchone()
    top_secmet_taxon = ret.tax_id
    top_secmet_species = ret.species
    top_secmet_taxon_count = ret.clusters_per_seq

    stats = {
        'num_clusters': num_clusters,
        'num_genomes': num_genomes,
        'num_sequences': num_sequences,
        'top_seq_taxon': top_seq_taxon,
        'top_seq_taxon_count': top_seq_taxon_count,
        'top_secmet_taxon': top_secmet_taxon,
        'top_secmet_taxon_count': top_secmet_taxon_count,
        'top_secmet_species': top_secmet_species,
        'clusters': clusters,
    }

    return jsonify(stats)


@app.route('/api/v1.0/tree/secmet')
def get_sec_met_tree():
    '''Get the jsTree structure for secondary metabolite clusters'''
    cur = get_db().cursor()
    cur.execute(sql.SECMET_TREE)
    ret = cur.fetchall()

    clusters = []
    types = {}

    for entry in ret:
        types[entry.term] = entry.description
        clusters.append({
            "id": "{}_c{}_{}".format(entry.acc, entry.cluster_number, entry.term),
            "parent": entry.term,
            "text": "{} {} Cluster {}".format(entry.species, entry.acc, entry.cluster_number),
            "type": "cluster",
        })

    for name, desc in types.iteritems():
        clusters.insert(0, {
            "id": name,
            "parent": "#",
            "text": desc,
            "state": {
                "disabled": True
            }
        })

    tree = clusters
    return jsonify(tree)


@app.route('/api/v1.0/tree/taxa')
def get_taxon_tree():
    '''Get the jsTree structure for all taxa'''
    tree_id = request.args.get('id', '1')

    cur = get_db().cursor()

    HANDLERS = {
        'superkingdom': get_phylum,
        'phylum': get_class,
        'class': get_order,
        'order': get_family,
        'family': get_genus,
        'genus': get_species,
    }

    if tree_id == '1':
        tree = get_superkingdom(cur)
    else:
        params = tree_id.split('_')
        taxlevel = params[0]
        params = params[1:]
        handler = HANDLERS.get(taxlevel, lambda x, y: [])
        tree = handler(cur, params)

    return jsonify(tree)


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


@app.route('/api/v1.0/search', methods=['POST'])
def search():
    '''Handle searching the database'''
    search_string = request.json.get('search_string', '')
    try:
        offset = int(request.json.get('offset'))
    except TypeError:
        offset = 0

    try:
        paginate = int(request.json.get('paginate'))
    except TypeError:
        paginate = 50

    total_count, found_bgcs = search_bgcs(search_string, offset=offset, paginate=paginate)

    result = {
        'total': total_count,
        'clusters': found_bgcs,
        'offset': offset,
        'paginate': paginate,
    }

    return jsonify(result)


@app.route('/api/v1.0/genome/<identifier>')
def show_genome(identifier):
    '''show information for a genome by identifier'''
    search_string = '[acc]{}'.format(identifier)
    _, found_bgcs = search_bgcs(search_string)

    return jsonify(found_bgcs)


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
