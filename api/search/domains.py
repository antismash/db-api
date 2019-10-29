'''Search functions related to asDomain searches'''

from flask import g

from sqlalchemy import (
    sql,
)
from .helpers import (
    break_lines,
    calculate_sequence,
    register_handler,
    UnknownQueryError,
)
from api.location import location_from_string

from api.models import (
    db,
    AsDomain,
    AsDomainProfile,
    BgcType,
    ClusterblastAlgorithm,
    ClusterblastHit,
    Cds,
    DnaSequence,
    Genome,
    Module,
    Monomer,
    Profile,
    ProfileHit,
    Region,
    RelModulesMonomer,
    Taxa,
    t_rel_regions_types,
)

DOMAIN_QUERIES = {}
DOMAIN_FORMATTERS = {}


def domain_query_from_term(term):
    '''Recursively generate an SQL query from the search terms'''
    if term.kind == 'expression':
        if term.category in DOMAIN_QUERIES:
            return DOMAIN_QUERIES[term.category](term.term)
        else:
            raise UnknownQueryError()
    elif term.kind == 'operation':
        left_query = domain_query_from_term(term.left)
        right_query = domain_query_from_term(term.right)
        if term.operation == 'except':
            return left_query.except_(right_query)
        elif term.operation == 'or':
            return left_query.union(right_query)
        elif term.operation == 'and':
            return left_query.intersect(right_query)

    raise UnknownQueryError()


def query_taxon_generic():
    '''Generic query for asDomain by taxon'''
    return AsDomain.query.join(Cds).join(Region).join(DnaSequence).join(Genome).join(Taxa)


@register_handler(DOMAIN_QUERIES)
def query_taxid(term):
    '''Generate asDomain query by NCBI taxid'''
    return query_taxon_generic().filter(Taxa.tax_id == term)


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
def query_acc(term):
    '''Generate asDomain query by NCBI accession'''
    return AsDomain.query.join(Cds).join(Region).join(DnaSequence).filter(DnaSequence.accession.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_type(term):
    '''Generate asDomain query by BGC type'''
    return AsDomain.query.join(Cds) \
                   .join(Region) \
                   .join(t_rel_regions_types).join(BgcType) \
                   .filter(BgcType.term.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_monomer(term):
    '''Generate asDomain query by monomer'''
    return AsDomain.query.join(Module).join(RelModulesMonomer).join(Monomer).filter(Monomer.name.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_profile(term):
    '''Generate asDomain query by BGC profile hit'''
    return AsDomain.query.join(Cds).join(ProfileHit).join(Profile) \
                   .filter(Profile.name.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_asdomain(term):
    '''Generate asDomain query by cluster type'''
    return AsDomain.query.join(AsDomainProfile).filter(AsDomainProfile.name.ilike(term))


def domain_by_x_clusterblast(term, algorithm):
    '''Generic search for domain by XClusterBlast and hit id'''
    return AsDomain.query.join(Cds).join(Region) \
                   .join(ClusterblastHit).join(ClusterblastAlgorithm) \
                   .filter(ClusterblastAlgorithm.name == algorithm) \
                   .filter(ClusterblastHit.acc.ilike(term))


@register_handler(DOMAIN_QUERIES)
def query_clusterblast(term):
    '''Generate asDomain query by ClusterBlast hit'''
    return domain_by_x_clusterblast(term, 'clusterblast')


@register_handler(DOMAIN_QUERIES)
def query_knowncluster(term):
    '''Generate asDomain query by KnownClusterBlast hit'''
    return domain_by_x_clusterblast(term, 'knownclusterblast')


@register_handler(DOMAIN_QUERIES)
def query_subcluster(term):
    '''Generate asDomain query by SubClusterBlast hit'''
    return domain_by_x_clusterblast(term, 'subclusterblast')


##############
# Formatters #
##############

@register_handler(DOMAIN_FORMATTERS)
def format_fastaa(domains):
    '''Generate protein FASTA records for a list of domains'''
    query = db.session.query(AsDomain.as_domain_id, AsDomain.location, AsDomain.translation, AsDomainProfile.name,
                             Cds.locus_tag, DnaSequence.accession, DnaSequence.version)
    query = query.join(AsDomainProfile).join(Cds).join(Region).join(DnaSequence)
    query = query.filter(AsDomain.as_domain_id.in_(map(lambda x: x.as_domain_id, domains))).order_by(AsDomain.as_domain_id)
    search = ''
    if g.verbose:
        search = "|{}".format(g.search_str)
    fasta_records = []
    for domain in query:
        sequence = break_lines(domain.translation)
        record = '>{d.locus_tag}|{d.name}|{d.accession}.{d.version}|' \
                 '{d.location}{search}\n' \
                 '{sequence}'.format(d=domain, search=search, sequence=sequence)
        fasta_records.append(record)

    return fasta_records


@register_handler(DOMAIN_FORMATTERS)
def format_fasta(domains):
    '''Generate DNA FASTA records for a list of domains'''
    search = ''
    if g.verbose:
        search = "|{}".format(g.search_str)
    fasta_records = []
    for domain in domains:
        record = domain.cds.region.dna_sequence
        location = location_from_string(domain.location)
        sequence = break_lines(calculate_sequence(location, record.dna))
        record = '>{d.cds.locus_tag}|{d.as_domain_profile.name}|{record.accession}.{record.version}|' \
                 '{d.location}{search}\n' \
                 '{sequence}'.format(d=domain, search=search, sequence=sequence, record=record)
        fasta_records.append(record)

    return fasta_records


@register_handler(DOMAIN_FORMATTERS)
def format_csv(domains):
    '''Generate CSV records for a list of domains'''
    query = db.session.query(AsDomain.as_domain_id, AsDomain.translation, AsDomainProfile.name,
                             Cds.locus_tag, AsDomain.location, DnaSequence.accession, DnaSequence.version)
    query = query.join(AsDomainProfile).join(Cds).join(Region).join(DnaSequence)
    query = query.filter(AsDomain.as_domain_id.in_(map(lambda x: x.as_domain_id, domains))).order_by(AsDomain.as_domain_id)
    csv_lines = ['#Locus tag\tDomain type\tAccession\tStart\tEnd\tStrand\tSequence']
    for domain in query:
        csv_lines.append('{d.locus_tag}\t{d.name}\t'
                         '{d.accession}.{d.version}\t'
                         '{d.location}\t'
                         '{d.translation}'.format(d=domain))
    return csv_lines
