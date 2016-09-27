'''Search functions related to asDomain searches'''
from sqlalchemy import (
    func,
    or_,
    sql,
)
from .helpers import (
    break_lines,
    register_handler,
)

from api.models import (
    db,
    AsDomain,
    AsDomainProfile,
    BgcType,
    BiosyntheticGeneCluster as Bgc,
    Compound,
    DnaSequence,
    t_gene_cluster_map,
    t_rel_clusters_types,
)

DOMAIN_QUERIES = {}
DOMAIN_FORMATTERS = {}


def domain_query_from_term(term):
    '''Recursively generate an SQL query from the search terms'''
    if term.kind == 'expression':
        if term.category in DOMAIN_QUERIES:
            return DOMAIN_QUERIES[term.category](term.term)
        else:
            return AsDomain.query.filter(sql.false())
    elif term.kind == 'operation':
        left_query = domain_query_from_term(term.left)
        right_query = domain_query_from_term(term.right)
        if term.operation == 'except':
            return left_query.except_(right_query)
        elif term.operation == 'or':
            return left_query.union(right_query)
        elif term.operation == 'and':
            return left_query.intersect(right_query)

    return AsDomain.query.filter(sql.false())


@register_handler(DOMAIN_QUERIES)
def query_asdomain(term):
    '''Generate asDomain query by cluster type'''
    return AsDomain.query.join(AsDomainProfile).filter(AsDomainProfile.name.ilike(term))


@register_handler(DOMAIN_FORMATTERS)
def format_fasta(domains):
    '''Generate FASTA records for a list of domains'''
    fasta_records = []
    for domain in domains:
        sequence = break_lines(domain.translation)
        record = '>{d.gene.locus_tag}|{d.as_domain_profile.name}|{d.locus.sequence.acc}.{d.locus.sequence.version}|' \
                 '{d.locus.start_pos}-{d.locus.end_pos}({d.locus.strand})\n' \
                 '{sequence}'.format(d=domain, sequence=sequence)
        fasta_records.append(record)

    return fasta_records
