'''The API calls'''

import StringIO
import json
from flask import (
    abort,
    jsonify,
    request,
    send_file,
)
import sqlalchemy
from sqlalchemy import (
    cast,
    desc as sql_desc,
    distinct,
    Float,
    func,
)
from . import app, taxtree
from .search import (
    core_search,
    json_stats,
    available_term_by_category,
    CLUSTER_FORMATTERS,
)
from .search_parser import Query
from .models import (
    db,
    BgcType,
    BiosyntheticGeneCluster as Bgc,
    DnaSequence,
    Genome,
    Locus,
    Taxa,
    t_rel_clusters_types,
)


@app.route('/api/v1.0/version')
def get_version():
    '''display the API version'''
    from .version import __version__ as api_version
    ret = {
        'api': api_version,
        'sqlalchemy': sqlalchemy.__version__
    }
    return jsonify(ret)


@app.route('/api/v1.0/stats')
def get_stats():
    '''contents for the stats page'''
    num_clusters = Bgc.query.count()

    num_genomes = Genome.query.count()

    num_sequences = DnaSequence.query.count()

    clusters = []

    sub = db.session.query(t_rel_clusters_types.c.bgc_type_id, func.count(1).label('count')) \
                    .group_by(t_rel_clusters_types.c.bgc_type_id).subquery()
    ret = db.session.query(BgcType.term, BgcType.description, sub.c.count).join(sub) \
                    .order_by(sub.c.count.desc(), BgcType.term)
    for cluster in ret:
        clusters.append({'name': cluster.term, 'description': cluster.description, 'count': cluster.count})

    ret = db.session.query(Taxa.tax_id, Taxa.genus, Taxa.species, func.count(DnaSequence.acc).label('tax_count')) \
                    .join(Genome).join(DnaSequence) \
                    .group_by(Taxa.tax_id).order_by(sql_desc('tax_count')).limit(1).first()
    top_seq_taxon = ret.tax_id
    top_seq_taxon_count = ret.tax_count


    ret = db.session.query(Taxa.tax_id, Taxa.genus, Taxa.species, func.count(distinct(Bgc.bgc_id)).label('bgc_count'),
                           func.count(distinct(DnaSequence.acc)).label('seq_count'),
                           (cast(func.count(distinct(Bgc.bgc_id)), Float) / func.count(distinct(DnaSequence.acc))).label('clusters_per_seq')) \
                    .join(Genome).join(DnaSequence).join(Locus).join(Bgc) \
                    .group_by(Taxa.tax_id).order_by(sql_desc('clusters_per_seq')).limit(1).first()
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
    ret = db.session.query(Bgc.bgc_id, Bgc.cluster_number,
                           DnaSequence.acc,
                           BgcType.term, BgcType.description,
                           Taxa.genus, Taxa.species) \
                    .join(t_rel_clusters_types).join(BgcType).join(Locus) \
                    .join(DnaSequence).join(Genome).join(Taxa) \
                    .order_by(Taxa.genus, Taxa.species, DnaSequence.acc, Bgc.cluster_number) \
                    .all()

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

    HANDLERS = {
        'superkingdom': taxtree.get_phylum,
        'phylum': taxtree.get_class,
        'class': taxtree.get_order,
        'order': taxtree.get_family,
        'family': taxtree.get_genus,
        'genus': taxtree.get_species,
        'species': taxtree.get_strains,
    }

    if tree_id == '1':
        tree = taxtree.get_superkingdom()
    else:
        params = tree_id.split('_')
        taxlevel = params[0]
        params = params[1:]
        handler = HANDLERS.get(taxlevel, lambda x: [])
        tree = handler(params)

    return jsonify(tree)


@app.route('/api/v1.0/search', methods=['POST'])
def search():
    try:
        if 'query' not in request.json:
            query = Query.from_string(request.json.get('search_string', ''))
        else:
            query = Query.from_json(request.json['query'])
    except ValueError:
        abort(400)

    if query.return_type != 'json':
        abort(400)

    clusters = CLUSTER_FORMATTERS['json'](core_search(query))
    stats = json_stats(clusters)

    result = {
        'total': len(clusters),
        'clusters': clusters,
        'offset': 0,
        'paginate': 50,
        'stats': stats,
    }

    return jsonify(result)


@app.route('/api/v1.0/export', methods=['POST'])
def export():
    '''Export the search results as CSV file'''
    try:
        if 'query' not in request.json:
            query = Query.from_string(request.json.get('search_string', ''), return_type='csv')
        else:
            query = Query.from_json(request.json['query'])
    except ValueError:
        abort(400)

    if query.return_type not in ('json', 'csv', 'fasta'):
        abort(400)

    found_bgcs = CLUSTER_FORMATTERS[query.return_type](core_search(query))
    filename = 'asdb_search_results.{}'.format(query.return_type)
    if query.return_type == 'json':
        found_bgcs = [json.dumps(found_bgcs)]


    handle = StringIO.StringIO()
    for line in found_bgcs:
        handle.write('{}\n'.format(line))

    handle.seek(0)


    return send_file(handle, attachment_filename=filename, as_attachment=True)


@app.route('/api/v1.0/genome/<identifier>')
def show_genome(identifier):
    '''show information for a genome by identifier'''
    query = Query.from_string('[acc]{}'.format(identifier))
    found_bgcs = CLUSTER_FORMATTERS['json'](core_search(query))

    return jsonify(found_bgcs)


@app.route('/api/v1.0/available/<category>/<term>')
def list_available(category, term):
    '''list available terms for a given category'''
    return jsonify(available_term_by_category(category, term))
