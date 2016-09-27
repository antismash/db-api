'''Search-related functions'''
from sqlalchemy import (
    func,
)
from api.models import (
    db,
    AsDomain,
    BgcType,
    BiosyntheticGeneCluster as Bgc,
    Gene,
    Genome,
    DnaSequence,
    Locus,
    Taxa,
    t_rel_clusters_types,
)
from .clusters import (
    cluster_query_from_term,
    CLUSTER_FORMATTERS,
)
from .genes import (
    gene_query_from_term,
    GENE_FORMATTERS,
)
from .domains import (
    domain_query_from_term,
    DOMAIN_FORMATTERS,
)

#######
# The following imports are just so the code depending on search doesn't need changes
from .available import available_term_by_category  # noqa: F401
#######

FORMATTERS = {
    'cluster': CLUSTER_FORMATTERS,
    'gene': GENE_FORMATTERS,
    'domain': DOMAIN_FORMATTERS,
}


class NoneQuery(object):
    '''A 'no result' return object'''
    def all(self):
        '''Just return an empty list'''
        return []


def core_search(query):
    '''Actually run the search logic'''
    sql_query = NoneQuery()

    if query.search_type == 'cluster':
        sql_query = cluster_query_from_term(query.terms).order_by(Bgc.bgc_id)
    elif query.search_type == 'gene':
        sql_query = gene_query_from_term(query.terms).order_by(Gene.gene_id)
    elif query.search_type == 'domain':
        sql_query = domain_query_from_term(query.terms).order_by(AsDomain.as_domain_id)

    results = sql_query.all()

    return results


def format_results(query, results):
    '''Get the appropriate formatter for the query'''
    try:
        fmt_func = FORMATTERS[query.search_type][query.return_type]
        return fmt_func(results)
    except KeyError:
        return []


def json_stats(json_clusters):
    '''Calculate some stats on the search results'''
    stats = {}
    if len(json_clusters) < 1:
        return stats

    bgc_ids = set()
    for cluster in json_clusters:
        bgc_ids.add(cluster['bgc_id'])

    clusters_by_type_list = db.session.query(BgcType.term, func.count(BgcType.term)) \
                                      .join(t_rel_clusters_types).join(Bgc) \
                                      .filter(Bgc.bgc_id.in_(bgc_ids)).group_by(BgcType.term).all()
    clusters_by_type = {}
    if clusters_by_type_list is not None:
        clusters_by_type['labels'], clusters_by_type['data'] = zip(*clusters_by_type_list)
    stats['clusters_by_type'] = clusters_by_type

    clusters_by_phylum_list = db.session.query(Taxa.phylum, func.count(Taxa.phylum)) \
                                        .join(Genome).join(DnaSequence).join(Locus).join(Bgc) \
                                        .filter(Bgc.bgc_id.in_(bgc_ids)).group_by(Taxa.phylum).all()
    clusters_by_phylum = {}
    if clusters_by_phylum_list is not None:
        clusters_by_phylum['labels'], clusters_by_phylum['data'] = zip(*clusters_by_phylum_list)
    stats['clusters_by_phylum'] = clusters_by_phylum

    return stats
