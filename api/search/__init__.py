'''Search-related functions'''
from sqlalchemy import (
    func,
    sql,
)
from api.models import (
    db,
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

#######
# The following imports are just so the code depending on search doesn't need changes
from .available import available_term_by_category  # noqa: F401
#######

GENE_QUERIES = {}
GENE_FORMATTERS = {}
FORMATTERS = {
    'cluster': CLUSTER_FORMATTERS,
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
        sql_query = cluster_query_from_term(query.terms)
    elif query.search_type == 'gene':
        sql_query = gene_query_from_term(query.terms)

    results = sql_query.all()

    return results


def gene_query_from_term(term):
    '''Recursively generate an SQL query from the search terms'''
    if term.kind == 'expression':
        if term.category in GENE_QUERIES:
            return GENE_QUERIES[term.category](term.term)
        else:
            return Gene.query.filter(sql.false())
    elif term.kind == 'operation':
        left_query = gene_query_from_term(term.left)
        right_query = gene_query_from_term(term.right)
        if term.operation == 'except':
            return left_query.except_(right_query)
        elif term.operation == 'or':
            return left_query.union(right_query)
        elif term.operation == 'and':
            return left_query.intersect(right_query)

    return Gene.query.filter(sql.false())


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
