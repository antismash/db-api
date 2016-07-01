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
    cur.execute("SELECT COUNT(bgc_id) FROM antismash.biosynthetic_gene_clusters")
    num_clusters = cur.fetchone()[0]

    cur.execute("SELECT COUNT(genome_id) FROM antismash.genomes")
    num_genomes = cur.fetchone()[0]

    clusters = []

    cur.execute("SELECT term, description, count FROM antismash.bgc_types JOIN (SELECT bgc_type_id, COUNT(1) FROM antismash.rel_clusters_types GROUP BY bgc_type_id) foo USING (bgc_type_id) ORDER BY count DESC")
    ret = cur.fetchall()
    for cluster in ret:
        clusters.append({'name': cluster.term, 'description': cluster.description, 'count': cluster.count})

    stats = {
        'num_clusters': num_clusters,
        'num_genomes': num_genomes,
        'clusters': clusters,
    }

    return jsonify(stats)


@app.route('/api/v1.0/tree/secmet')
def get_sec_met_tree():
    '''Get the jsTree structure for secondary metabolite clusters'''
    cur = get_db().cursor()
    cur.execute("""
SELECT bgc_id, cluster_number, acc, term, description
    FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.rel_clusters_types USING (bgc_id)
    JOIN antismash.bgc_types USING (bgc_type_id)
""")
    ret = cur.fetchall()

    clusters = []
    types = {}

    for entry in ret:
        types[entry.term] = entry.description
        clusters.append({
            "id": "{}_c{}_{}".format(entry.acc, entry.cluster_number, entry.term),
            "parent": entry.term,
            "text": "{} Cluster {}".format(entry.acc, entry.cluster_number),
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
    tree = []

    cur = get_db().cursor()
    cur.execute(sql.TAXTREE_SUPERKINGOM)
    kingdoms = cur.fetchall()
    for kingdom in kingdoms:
        kingdom_list = [kingdom[0]]
        tree.append(_create_tree_node('superkingdom_{}'.format(kingdom[0].lower()),
                                      '#', kingdom[0]))
        cur.execute(sql.TAXTREE_PHYLUM, kingdom_list)
        phyla = cur.fetchall()
        for phylum in phyla:
            phylum_list = kingdom_list + [phylum[0]]
            tree.append(_create_tree_node('phylum_{}'.format(phylum[0].lower()),
                                          'superkingdom_{}'.format(kingdom[0].lower()),
                                          phylum[0]))
            cur.execute(sql.TAXTREE_CLASS, phylum_list)
            classes = cur.fetchall()
            for cls in classes:
                cls_list = phylum_list + [cls[0]]
                tree.append(_create_tree_node('class_{}'.format(cls[0].lower()), 'phylum_{}'.format(phylum[0].lower()),
                                              cls[0]))
                cur.execute(sql.TAXTREE_ORDER, cls_list)
                orders = cur.fetchall()
                for order in orders:
                    order_list = cls_list + [order[0]]
                    tree.append(_create_tree_node('order_{}'.format(order[0].lower()), 'class_{}'.format(cls[0].lower()),
                                                  order[0]))
                    cur.execute(sql.TAXTREE_FAMILY, order_list)
                    families = cur.fetchall()
                    for family in families:
                        family_list = order_list + [family[0]]
                        tree.append(_create_tree_node('family_{}'.format(family[0].lower()), 'order_{}'.format(order[0].lower()),
                                                      family[0]))
                        cur.execute(sql.TAXTREE_GENUS, family_list)
                        genera = cur.fetchall()
                        for genus in genera:
                            genus_list = family_list + [genus[0]]
                            tree.append(_create_tree_node('genus_{}'.format(genus[0].lower()), 'family_{}'.format(family[0].lower()),
                                                          genus[0]))
                            cur.execute(sql.TAXTREE_SPECIES, genus_list)
                            strains = cur.fetchall()
                            for strain in strains:
                                tree.append(_create_tree_node('{}'.format(strain.acc.lower()),
                                                              'genus_{}'.format(genus[0].lower()),
                                                              '{} {}.{}'.format(strain.species, strain.acc, strain.version),
                                                              disabled=False, leaf=True))

    return jsonify(tree)


@app.route('/api/v1.0/search', methods=['POST'])
def search():
    '''Handle searching the database'''
    search_string = request.json.get('search_string', '')
    found_bgcs = search_bgcs(search_string)

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
    return ret
