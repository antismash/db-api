'''The API calls'''

from distutils.util import strtobool
from enum import auto, Enum, unique
from io import BytesIO
import json
import re

from flask import (
    abort,
    g,
    jsonify,
    make_response,
    redirect,
    request,
    Response,
    send_file,
    stream_with_context,
)
import sqlalchemy
from sqlalchemy import (
    between,
    cast,
    desc as sql_desc,
    distinct,
    Float,
    func,
    or_,
)

from . import app, taxtree
from .asdb_jobs import (
    dispatchBlast,
    Job,
    JobType,
)
from .search import (
    core_search,
    format_results,
    region_stats,
    available_term_by_category,
)
from .search.clusters import CLUSTERS as CLUSTER_HANDLERS
from .search.filters import (
    available_filters_by_category,
)
from .search.helpers import (
    InvalidQueryError,
    TextFilter,
    UnknownQueryError,
    sanitise_string,
)
from .search_parser import Query
from .models import (
    db,
    BgcType,
    Region,
    DnaSequence,
    Filename,
    Genome,
    Taxa,
    t_rel_regions_types,
)
from .errors import TooManyResults
from .legacy import dbv1_accessions


MIME_TYPE_MAP = {
    'json': 'application/json',
    'csv': 'text/csv',
    'fasta': 'application/fasta',
    'fastaa': 'application/fasta',
}

FASTA_LIMITS = {
    'cluster': 100,
    'gene': 500,
    'domain': 500
}

SAFE_IDENTIFIER_PATTERN = re.compile('[^A-Za-z0-9_.]+', re.UNICODE)


@app.route('/api/v1.0/version')
def get_version():
    '''display the API version'''
    from .version import __version__ as api_version
    ret = {
        'api': api_version,
        'sqlalchemy': sqlalchemy.__version__
    }
    return jsonify(ret)


def _common_stats():
    """Get the stats shared by the v1 and v2 version of the call"""

    num_clusters = Region.query.filter(Region.contig_edge.is_(False)).count()

    num_genomes = Genome.query.count()

    num_sequences = DnaSequence.query.count()

    clusters = []
    sub = db.session.query(t_rel_regions_types.c.bgc_type_id, func.count(1).label('count')) \
                    .group_by(t_rel_regions_types.c.bgc_type_id).subquery()
    ret = db.session.query(BgcType.term, BgcType.description, BgcType.category, sub.c.count).join(sub) \
                    .order_by(sub.c.count.desc(), BgcType.term, BgcType.category)
    for cluster in ret:
        clusters.append({'name': cluster.term, 'description': cluster.description, 'count': cluster.count, 'category': cluster.category})

    ret = db.session.query(Taxa.tax_id, Taxa.genus, Taxa.species, Taxa.strain, func.count(DnaSequence.accession).label('tax_count')) \
                    .join(Genome, Genome.tax_id == Taxa.tax_id).join(DnaSequence, DnaSequence.genome_id == Genome.genome_id) \
                    .group_by(Taxa.tax_id).order_by(sql_desc('tax_count')).limit(1).first()
    top_seq_taxon = ret.tax_id
    top_seq_species = f'{ret.genus} {ret.species} {ret.strain}'
    top_seq_taxon_count = ret.tax_count

    stats = {
        'num_clusters': num_clusters,
        'num_genomes': num_genomes,
        'num_sequences': num_sequences,
        'top_seq_taxon': top_seq_taxon,
        'top_seq_taxon_count': top_seq_taxon_count,
        'top_seq_species': top_seq_species,
        'clusters': clusters,
    }
    return stats


@app.route('/api/v1.0/stats')
def get_stats_v1():
    '''contents for the stats page'''
    stats = _common_stats()

    ret = db.session.query(Taxa.tax_id, Taxa.genus, Taxa.species, Taxa.strain,
                           DnaSequence.accession,
                           func.count(distinct(Region.region_number)).label('bgc_count'),
                           func.count(distinct(DnaSequence.accession)).label('seq_count'),
                           (cast(func.count(distinct(Region.region_number)), Float) / func.count(distinct(DnaSequence.accession))).label('clusters_per_seq')) \
                    .join(Genome).join(DnaSequence).join(Region) \
                    .group_by(Taxa.tax_id, DnaSequence.accession).order_by(sql_desc('clusters_per_seq')).limit(1).first()
    stats['top_secmet_taxon'] = ret.tax_id
    stats['top_secmet_species'] = '{r.genus} {r.species} {r.strain}'.format(r=ret)
    stats['top_secmet_acc'] = ret.accession
    stats['top_secmet_taxon_count'] = ret.clusters_per_seq

    return jsonify(stats)


@app.route('/api/v2.0/stats')
def get_stats_v2():
    """contents for the stats page"""
    stats = _common_stats()

    ret = db.session.query(Taxa.tax_id, Taxa.genus, Taxa.species, Taxa.strain,
                           Genome.assembly_id,
                           func.count(distinct(Region.region_number)).label('bgc_count'),
                           func.count(distinct(Genome.assembly_id)).label('seq_count'),
                           (cast(func.count(distinct(Region.region_number)), Float) / func.count(distinct(Genome.assembly_id))).label('clusters_per_seq')) \
                    .join(Genome).join(DnaSequence).join(Region) \
                    .filter(Genome.assembly_id is not None) \
                    .group_by(Taxa.tax_id, Genome.assembly_id).order_by(sql_desc('clusters_per_seq')).limit(1).first()
    stats['top_secmet_taxon'] = ret.tax_id
    stats['top_secmet_species'] = '{r.genus} {r.species} {r.strain}'.format(r=ret)
    stats['top_secmet_assembly_id'] = ret.assembly_id
    stats['top_secmet_taxon_count'] = ret.bgc_count

    return jsonify(stats)


@app.route('/api/v1.0/tree/secmet')
def get_sec_met_tree():
    '''Get the jsTree structure for secondary metabolite clusters'''
    ret = db.session.query(Region.region_id, Region.region_number,
                           DnaSequence.accession,
                           BgcType.term, BgcType.description,
                           Taxa.genus, Taxa.species, Taxa.strain, Genome.assembly_id) \
                    .join(DnaSequence, DnaSequence.accession == Region.accession).join(Genome).join(Taxa) \
                    .join(t_rel_regions_types).join(BgcType) \
                    .order_by(BgcType.description, Taxa.genus, Taxa.species, DnaSequence.accession, Region.region_number) \
                    .all()

    clusters = []
    types = {}

    for entry in ret:
        types[entry.term] = entry.description
        species = entry.species if entry.species != 'Unclassified' else 'sp.'
        assembly_id = entry.assembly_id.split('.')[0] if entry.assembly_id else None
        name = '{} {} {}'.format(entry.genus, species, entry.strain)
        clusters.append({
            "id": "{}_c{}_{}".format(entry.accession, entry.region_number, entry.term),
            "parent": entry.term,
            "text": "{} {} Region {}".format(name, entry.accession, entry.region_number),
            "assembly_id": assembly_id,
            "region_number": entry.region_number,
            "type": "cluster",
        })

    for name, desc in sorted(list(types.items()), reverse=True):
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


def _get_taxon_tree_node(tree_id):
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

    return tree


@app.route('/api/v1.0/tree/taxa')
def get_taxon_tree():
    '''Get the jsTree structure for all taxa'''
    tree_id = request.args.get('id', '1')
    tree = _get_taxon_tree_node(tree_id)

    return jsonify(tree)


@app.route('/api/v1.0/tree/taxa/massload')
def get_taxon_tree_massload():
    tree_ids = request.args.get('id', '1')
    id_list = tree_ids.split(',')

    multitree = {}
    for tree_id in id_list:
        multitree[tree_id] = _get_taxon_tree_node(tree_id)

    return jsonify(multitree)


@app.route('/api/v1.0/tree/taxa/search')
def search_taxon_tree():
    search = request.args.get('str', None)
    if not search:
        return jsonify([])

    search_path = taxtree.search(search)

    return jsonify(search_path)


def search_common():
    """Shared logic between the v1 and v2 version of the /search endpoint."""
    try:
        if 'query' not in request.json:
            query = Query.from_string(request.json.get('search_string', ''))
        else:
            query = Query.from_json(request.json['query'])
    except ValueError:
        abort(400)

    if query.return_type != 'json':
        abort(400)

    try:
        offset = int(request.json.get('offset', '0'))
    except ValueError:
        offset = 0

    try:
        paginate = int(request.json.get('paginate', '50'))
    except ValueError:
        paginate = 50

    try:
        results = core_search(query)
    except UnknownQueryError:
        abort(make_response({"message": "Unknown query category"}, 400))
    except InvalidQueryError as err:
        abort(make_response({"message": str(err)}, 400))

    return query, results, offset, paginate


@app.route('/api/v1.0/search', methods=['POST'])
def search_v1():
    query, results, offset, paginate = search_common()
    stats = region_stats(results)
    total = len(results)

    if paginate > 0:
        end = min(offset + paginate, total)
    else:
        end = total

    clusters = format_results(query, results[offset:end])

    result = {
        'total': total,
        'clusters': clusters,
        'offset': offset,
        'paginate': paginate,
        'stats': stats,
    }

    return jsonify(result)


@app.route('/api/v2.0/search', methods=['POST'])
def search():
    query, results, offset, paginate = search_common()
    total = len(results)
    if paginate > 0:
        end = min(offset + paginate, total)
    else:
        end = total
    clusters = format_results(query, results[offset:end])


    result = {
        'total': total,
        'clusters': clusters,
        'offset': offset,
        'paginate': paginate,
    }

    return jsonify(result)


@app.route('/api/v1.0/searchstats', methods=['POST'])
def searchstats():
    query, results, offset, paginate = search_common()
    total = len(results)
    stats = region_stats(results)
    result = {
        'total': total,
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

    try:
        offset = int(request.json.get('offset', '0'))
    except ValueError:
        offset = 0

    try:
        paginate = int(request.json.get('paginate', '0'))
    except ValueError:
        paginate = 0

    return_type = query.return_type
    search_type = query.search_type

    if return_type not in ('json', 'csv', 'fasta', 'fastaa'):
        abort(400)

    try:
        search_results = core_search(query)
    except UnknownQueryError:
        abort(400)
    g.verbose = query.verbose
    if query.verbose:
        g.search_str = str(query)

    total = len(search_results)

    if paginate > 0:
        end = min(offset + paginate, total)
    else:
        end = total

    search_results = search_results[offset:end]

    limit = FASTA_LIMITS.get(search_type, 100)

    if return_type.startswith('fasta') and len(search_results) > limit:
        raise TooManyResults('More than {limit} search results for FASTA {search} download ({number} found), please specify a smaller query.'.format(
            limit=limit, search=search_type, number=len(search_results)))

    found_bgcs = format_results(query, search_results)
    filename = 'asdb_search_results.{}'.format(return_type)
    if query.return_type == 'json':
        found_bgcs = [json.dumps(found_bgcs)]

    handle = BytesIO()
    for line in found_bgcs:
        handle.write('{}\n'.format(line).encode('utf-8'))

    handle.seek(0)

    mime_type = MIME_TYPE_MAP.get(query.return_type, None)

    return send_file(handle, mimetype=mime_type, attachment_filename=filename, as_attachment=True)


@app.route('/api/v1.0/export/<search_type>/<return_type>')
def export_get(search_type, return_type):
    '''Export the search results as a file'''

    search_string = request.args.get('search', '')
    if search_string == '':
        abort(400)

    if return_type not in ('json', 'csv', 'fasta', 'fastaa'):
        abort(400)

    query = Query.from_string(search_string, search_type=search_type, return_type=return_type)

    g.verbose = False
    try:
        search_results = core_search(query)
    except UnknownQueryError:
        abort(400)
    if len(search_results) > 100 and search_type == 'cluster' and return_type == 'fasta':
        raise TooManyResults('More than 100 search results for FASTA cluster download, please specify a smaller query.')
    found_bgcs = format_results(query, search_results)
    if query.return_type == 'json':
        found_bgcs = [json.dumps(found_bgcs)]

    def generate():
        for line in found_bgcs:
            yield line + '\n'

    mime_type = MIME_TYPE_MAP.get(query.return_type, None)

    return Response(stream_with_context(generate()), mimetype=mime_type)


@app.route('/api/genome/<identifier>')
@app.route('/api/v1.0/genome/<identifier>')
def show_genome(identifier):
    '''show information for a genome by identifier'''
    query = Query.from_string(f"{{[acc|{identifier}]}}")
    found_bgcs = format_results(query, core_search(query))

    return jsonify(found_bgcs)


@app.route('/api/assembly/<identifier>')
@app.route('/api/v1.0/assembly/<identifier>')
def show_assembly(identifier):
    """show information for an assembly by identifier"""
    query = Query.from_string(f"{{[assembly|{identifier}]}}")
    found_bgcs = format_results(query, core_search(query))

    return jsonify(found_bgcs)


@app.route('/api/available/term/<category>/<term>')
@app.route('/api/v1.0/available/<category>/<term>')
def list_available(category, term):
    '''list available terms for a given category'''
    return jsonify(available_term_by_category(category, term))


def _canonical_assembly_id(identifier):
    """Turn the identifier into an ID usable for lookups."""
    safe_id = SAFE_IDENTIFIER_PATTERN.sub('', identifier).split('.')[0]
    if safe_id in dbv1_accessions:
        return safe_id, True

    res = db.session.query(Genome.assembly_id).filter(Genome.assembly_id.ilike("{}%".format(safe_id))).first()
    if res:
        return res.assembly_id, False

    res = db.session.query(Genome.assembly_id) \
                    .join(DnaSequence) \
                    .filter(DnaSequence.accession.ilike("{}%".format(safe_id))) \
                    .first()
    if res:
        return res.assembly_id, False

    abort(404)


@app.route('/api/goto/<identifier>')
@app.route('/api/v1.0/goto/<identifier>')
@app.route('/go/<identifier>')
def goto(identifier):
    safe_id, is_v1 = _canonical_assembly_id(identifier)
    if is_v1:
        return redirect("https://antismash-dbv1.secondarymetabolites.org/output/{}/index.html".format(safe_id))

    return redirect("/output/{}/index.html".format(safe_id))


@app.route('/api/v1.0/goto/<identifier>/cluster/<int:number>')
def goto_cluster(identifier, number):
    safe_id, is_v1 = _canonical_assembly_id(identifier)
    if is_v1:
        return redirect("https://antismash-dbv1.secondarymetabolites.org/output/{}/index.html#cluster-{}".format(safe_id, number))

    return redirect("/output/{}/index.html#cluster-{}".format(safe_id, number))


@app.route('/api/goto/<identifier>/<region>')
@app.route('/go/<identifier>/<region>')
def goto_region(identifier, region):
    safe_id, is_v1 = _canonical_assembly_id(identifier)
    if is_v1:
        return redirect("https://antismash-dbv1.secondarymetabolites.org/output/{}/index.html#cluster-{}".format(safe_id, region))

    return redirect(f"/output/{safe_id}/index.html#{region}")


@app.route('/api/area/<record>.<int:version>/<int:start_pos>-<int:end_pos>')
@app.route('/api/v1.0/area/<record>.<int:version>/<int:start_pos>-<int:end_pos>')
def area(record, version, start_pos, end_pos):
    safe_acc = SAFE_IDENTIFIER_PATTERN.sub('', record)

    query = Region.query.join(DnaSequence, Region.dna_sequence).join(Genome, DnaSequence.genome)
    query = query.filter(DnaSequence.accession == safe_acc).filter(DnaSequence.version == version)
    query = query.filter(or_(
        Region.start_pos.between(start_pos, end_pos),
        Region.end_pos.between(start_pos, end_pos),
        between(start_pos, Region.start_pos, Region.end_pos),
        between(end_pos, Region.start_pos, Region.end_pos),
    ))
    res = query.all()

    # Right now format_results needs a Query object
    dummy = Query(None, search_type="cluster", return_type="json")
    # TODO: refactor format_results to optionally take a search_type and return_type
    clusters = format_results(dummy, res)

    result = {
        "clusters": clusters
    }

    return jsonify(result)


@app.route('/api/area/<record>/<int:start_pos>-<int:end_pos>')
@app.route('/api/v1.0/area/<record>/<int:start_pos>-<int:end_pos>')
def area_without_version(record, start_pos, end_pos):
    safe_acc = SAFE_IDENTIFIER_PATTERN.sub('', record)

    query = Region.query.join(DnaSequence, Region.dna_sequence).join(Genome, DnaSequence.genome)
    query = query.filter(DnaSequence.accession == safe_acc)
    query = query.filter(or_(
        Region.start_pos.between(start_pos, end_pos),
        Region.end_pos.between(start_pos, end_pos),
        between(start_pos, Region.start_pos, Region.end_pos),
        between(end_pos, Region.start_pos, Region.end_pos),
    ))
    res = query.all()

    # Right now format_results needs a Query object
    dummy = Query(None, search_type="cluster", return_type="json")
    # TODO: refactor format_results to optionally take a search_type and return_type
    clusters = format_results(dummy, res)

    result = {
        "clusters": clusters
    }

    return jsonify(result)


def _get_base_url(identifier):
    safe_id = SAFE_IDENTIFIER_PATTERN.sub('', identifier).split('.')[0]
    ret = db.session.query(Filename.assembly_id, Filename.base_filename) \
            .filter(Filename.assembly_id == safe_id).first()
    if not ret:
        abort(404)

    return "/output/{r.assembly_id}/{r.base_filename}".format(r=ret)


@app.route('/api/v1.0/download/genbank/<identifier>')
def download_genbank(identifier):
    url = _get_base_url(identifier)
    return redirect("{}.final.gbk".format(url))


@app.route('/api/v1.0/download/table/<identifier>')
def download_table(identifier):
    url = _get_base_url(identifier)
    return redirect("{}.geneclusters.xls".format(url))


@app.route('/api/v1.0/download/genbank/<identifier>/cluster/<int:number>')
def download_cluster(identifier, number):
    url = _get_base_url(identifier)
    return redirect("{}.cluster{:03d}.gbk".format(url, number))


def returnJobInfo(job: Job):
    """Return all the relevant info for a job"""

    job_json = {
        "id": job.id,
        "jobtype": job.jobtype,
        "status": job.status,
        "submitted": job.submitted_date,
    }

    if job.status == "error":
        return jsonify(job_json)

    if job.status in ("pending", "running"):
        job_json["next"] = f"/api/v1.0/job/{job.id}"
        return jsonify(job_json)

    job_json["results"] = job.results

    return jsonify(job_json)


@app.route('/api/jobs/comparippson', methods=["POST"])
@app.route('/api/v1.0/comparippson', methods=["POST"])
def submit_comparippson():
    """Submit a CompaRiPPson search"""
    if "name" not in request.json or "sequence" not in request.json:
        abort(400)

    name = request.json["name"]
    sequence = request.json["sequence"]

    job = dispatchBlast(JobType.COMPARIPPSON, name, sequence)
    return returnJobInfo(job)


@app.route('/api/jobs/clusterblast', methods=["POST"])
@app.route('/api/v1.0/clusterblast', methods=["POST"])
def submit_clusterblast():
    """Submit a ClusterBlast search"""
    if "name" not in request.json or "sequence" not in request.json:
        abort(400)

    name = request.json["name"]
    sequence = request.json["sequence"]

    job = dispatchBlast(JobType.CLUSTERBLAST, name, sequence)
    return returnJobInfo(job)


@app.route('/api/job/<job_id>')
@app.route('/api/v1.0/job/<job_id>')
def fetch_job(job_id: str):
    """Fetch the results of a background job run"""
    job = Job.query.filter(Job.id == job_id).one_or_none()
    if job is None:
        abort(404)
    return returnJobInfo(job)


@app.route('/api/convert')
@app.route('/api/v1.0/convert')
def convert():
    try:
        args = {}
        args['string'] = request.args.get('search_string', '')
        if 'search_type' in request.args:
            args['search_type'] = request.args['search_type']
        if 'return_type' in request.args:
            args['return_type'] = request.args['return_type']
        if 'verbose' in request.args:
            args['verbose'] = bool(strtobool(request.args['verbose']))
        query = Query.from_string(**args)
    except ValueError:
        abort(400)

    return jsonify(query.to_json())


@unique
class CategoryType(Enum):
    TEXT = auto()
    BOOL = auto()
    NUMERIC = auto()
    MODULEQUERY = auto()

    def __str__(self):
        return str(self.name).lower()


PREDICTION_GROUP = "antiSMASH predictions"
RIPP_GROUP = "Compound properties"
QUALITY_GROUP = "Quality filters"
TAXONOMY_GROUP = "Taxonomy"
COMPARISON_GROUP = "Similar clusters"

CATEGORY_GROUPS = [PREDICTION_GROUP, RIPP_GROUP, QUALITY_GROUP, TAXONOMY_GROUP, COMPARISON_GROUP]

CATEGORIES = {
    "acc": ("NCBI RefSeq Accession", CategoryType.TEXT, None),
    "assembly": ("NCBI assembly ID", CategoryType.TEXT, None),
    "type": ("BGC type", CategoryType.TEXT, PREDICTION_GROUP),
    "typecategory": ("BGC category", CategoryType.TEXT, PREDICTION_GROUP),
    "candidatekind": ("Candidate cluster type", CategoryType.TEXT, PREDICTION_GROUP),
    "substrate": ("Substrate", CategoryType.TEXT, PREDICTION_GROUP),
    "monomer": ("Monomer", CategoryType.TEXT, PREDICTION_GROUP),
    "profile": ("Biosynthetic profile", CategoryType.TEXT, PREDICTION_GROUP),
    "resfam": ("ResFam profile", CategoryType.TEXT, PREDICTION_GROUP),
    "pfam": ("Pfam profile", CategoryType.TEXT, PREDICTION_GROUP),
    "tigrfam": ("TIGRFAM profile", CategoryType.TEXT, PREDICTION_GROUP),
    "asdomain": ("NRPS/PKS domain", CategoryType.TEXT, PREDICTION_GROUP),
    "asdomainsubtype": ("NRPS/PKS domain subtype", CategoryType.TEXT, PREDICTION_GROUP),
    "modulequery": ("NRPS/PKS module query", CategoryType.MODULEQUERY, PREDICTION_GROUP),
    "crosscdsmodule": ("NRPS/PKS cross-CDS module", CategoryType.BOOL, PREDICTION_GROUP),
    "t2pksclass": ("PKS Type II class", CategoryType.TEXT, PREDICTION_GROUP),
    "t2pksstarter": ("PKS Type II starter moiety", CategoryType.TEXT, PREDICTION_GROUP),
    "t2pkselongation": ("PKS Type II elongation", CategoryType.TEXT, PREDICTION_GROUP),
    "smcog": ("smCoG hit", CategoryType.TEXT, PREDICTION_GROUP),
    "tfbs": ("Binding site regulator", CategoryType.TEXT, PREDICTION_GROUP),
    "compoundseq": ("Compound sequence", CategoryType.TEXT, RIPP_GROUP),
    "compoundclass": ("RiPP compound class", CategoryType.TEXT, RIPP_GROUP),
    "contigedge": ("Region on contig edge", CategoryType.BOOL, QUALITY_GROUP),
    "strain": ("Strain", CategoryType.TEXT, TAXONOMY_GROUP),
    "species": ("Species", CategoryType.TEXT, TAXONOMY_GROUP),
    "genus": ("Genus", CategoryType.TEXT, TAXONOMY_GROUP),
    "family": ("Family", CategoryType.TEXT, TAXONOMY_GROUP),
    "order": ("Order", CategoryType.TEXT, TAXONOMY_GROUP),
    "class": ("Class", CategoryType.TEXT, TAXONOMY_GROUP),
    "phylum": ("Phylum", CategoryType.TEXT, TAXONOMY_GROUP),
    "superkingdom": ("Superkingdom", CategoryType.TEXT, TAXONOMY_GROUP),
    "comparippsonmibig": ("CompaRiPPson MIBiG hit", CategoryType.TEXT, COMPARISON_GROUP),
    "clustercompareregion": ("ClusterCompare by region", CategoryType.TEXT, COMPARISON_GROUP),
    "clustercompareprotocluster": ("ClusterCompare by protocluster", CategoryType.TEXT, COMPARISON_GROUP),
    "clusterblast": ("ClusterBlast hit", CategoryType.TEXT, COMPARISON_GROUP),
    "knowncluster": ("KnownClusterBlast hit", CategoryType.TEXT, COMPARISON_GROUP),
    "subcluster": ("SubClusterBlast hit", CategoryType.TEXT, COMPARISON_GROUP),
}


@app.route("/api/available/categories")
@app.route("/api/v1.0/available_categories")
def list_available_categories():
    # type options: text, bool, numeric
    result = {
        "options": [],
        "groups": [{"header": group, "options": []} for group in CATEGORY_GROUPS],
    }
    for category, handler in CLUSTER_HANDLERS.items():
        label, data_type, group = CATEGORIES[category]
        option = {
            "label": label,
            "value": category,
            "type": str(data_type),
            "countable": handler.countable,
            "description": handler.description,
        }
        filters = available_filters_by_category(category)
        if filters:
            option["filters"] = filters
        if group is None:
            result["options"].append(option)
        else:
            index = CATEGORY_GROUPS.index(group)
            result["groups"][index]["options"].append(option)
    # don't use jsonify, it'll sort by key and ruin filter options and the like
    return Response(json.dumps(result, sort_keys=False), mimetype="text/json")


@app.route('/api/available/filters/<category>')
@app.route('/api/v1.0/available_filters/<category>')
def list_available_filters(category):
    """List available filters for a given category"""
    return jsonify(available_filters_by_category(category))


@app.route('/api/available/filters/<category>/<filter_name>/<term>')
@app.route('/api/v1.0/available_filter_values/<category>/<filter_name>/<term>')
def list_available_filter_values(category, filter_name, term):
    """List available values for a particular filter"""
    category = sanitise_string(category)
    filter_name = sanitise_string(filter_name)
    matching = available_filters_by_category(category, as_json=False).get(filter_name)
    if not matching or not isinstance(matching, TextFilter):
        return jsonify([])
    query = matching.available(sanitise_string(term)).limit(50)
    return jsonify(list(map(lambda x: {'val': x[0], 'desc': x[1]}, query.all())))
