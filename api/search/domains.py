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
    Genome,
    Locus,
    Taxa,
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


def query_taxon_generic():
    '''Generic query for asDomain by taxon'''
    return AsDomain.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa)


@register_handler(DOMAIN_QUERIES)
def query_strain(term):
    '''Generate asDomain query by strain'''
    return query_taxon_generic().filter(Taxa.strain.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_species(term):
    '''Generate asDomain query by species'''
    return query_taxon_generic().filter(Taxa.species.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_genus(term):
    '''Generate asDomain query by genus'''
    return query_taxon_generic().filter(Taxa.genus.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_family(term):
    '''Generate asDomain query by family'''
    return query_taxon_generic().filter(Taxa.family.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_order(term):
    '''Generate asDomain query by order'''
    return query_taxon_generic().filter(Taxa.order.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_class(term):
    '''Generate asDomain query by class'''
    return query_taxon_generic().filter(Taxa._class.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_phylum(term):
    '''Generate asDomain query by phylum'''
    return query_taxon_generic().filter(Taxa.phylum.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_superkingdom(term):
    '''Generate asDomain query by superkingdom'''
    return query_taxon_generic().filter(Taxa.superkingdom.ilike(term))


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


@register_handler(DOMAIN_FORMATTERS)
def format_csv(domains):
    '''Generate CSV records for a list of domains'''
    csv_lines = ['#Locus tag\tDomain type\tAccession\tStart\tEnd\tStrand\tSequence']
    for domain in domains:
        csv_lines.append('{d.gene.locus_tag}\t{d.as_domain_profile.name}\t'
                         '{d.locus.sequence.acc}.{d.locus.sequence.version}\t'
                         '{d.locus.start_pos}\t{d.locus.end_pos}\t{d.locus.strand}\t'
                         '{d.translation}'.format(d=domain))
    return csv_lines
