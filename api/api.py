'''The API calls'''

from flask import jsonify
from . import app
from .helpers import get_db


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
