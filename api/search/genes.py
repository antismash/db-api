'''Gene-related search functions'''

from functools import partial

from flask import g

from sqlalchemy import (
    func,
    or_,
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
    DnaSequence,
    Cds,
    Genome,
    Module,
    Monomer,
    Pfam,
    PfamDomain,
    Profile,
    ProfileHit,
    Region,
    RelModulesMonomer,
    Resfam,
    ResfamDomain,
    Ripp,
    Taxa,
    Smcog,
    SmcogHit,
    t_rel_regions_types,
)

from api.search_parser import (
    QueryComponent,
    QueryOperand,
    QueryOperation,
)

GENE_QUERIES = {}
GENE_FORMATTERS = {}

def _generic_count_by_cds_id(query, minimum: int):
    """ Groups query results by CDS id and filters to those groups with at
        least the given number of matches
    """
    if minimum < 0:
        return query
    return query.group_by(Cds.cds_id).having(func.count(Cds.cds_id) >= minimum)


register_countable_handler = partial(register_handler, countable=True, counter=_generic_count_by_cds_id)


def gene_query_from_term(term: QueryComponent):
    '''Recursively generate an SQL query from the search terms'''
    if isinstance(term, QueryOperand):
        if term.category in GENE_QUERIES:
            handler = GENE_QUERIES[term.category]
            query = handler(term.term)
            for query_filter in term.filters:
                query = query_filter.runner.run(query, query_filter.get_options())
            query = handler.add_count_restriction(query, term.count)
            return query
        else:
            raise UnknownQueryError()
    elif isinstance(term, QueryOperation):
        left_query = gene_query_from_term(term.left)
        right_query = gene_query_from_term(term.right)
        if term.operator == 'except':
            return left_query.except_(right_query)
        elif term.operator == 'or':
            return left_query.union(right_query)
        elif term.operator == 'and':
            return left_query.intersect(right_query)

    raise UnknownQueryError()


def query_taxon_generic():
    '''Generate Gene query by taxonomy'''
    return Cds.query.join(Region).join(DnaSequence).join(Genome).join(Taxa)


@register_handler(GENE_QUERIES)
def query_taxid(term):
    '''Generate Gene query by NCBI taxid'''
    return query_taxon_generic().filter(Taxa.tax_id == term)


@register_handler(GENE_QUERIES)
def query_strain(term):
    '''Generate Gene query by strain'''
    return query_taxon_generic().filter(Taxa.strain.ilike(term))


@register_handler(GENE_QUERIES)
def query_species(term):
    '''Generate Gene query by species'''
    return query_taxon_generic().filter(Taxa.species.ilike(term))


@register_handler(GENE_QUERIES)
def query_genus(term):
    '''Generate Gene query by genus'''
    return query_taxon_generic().filter(Taxa.genus.ilike(term))


@register_handler(GENE_QUERIES)
def query_family(term):
    '''Generate Gene query by family'''
    return query_taxon_generic().filter(Taxa.family.ilike(term))


@register_handler(GENE_QUERIES)
def query_order(term):
    '''Generate Gene query by order'''
    return query_taxon_generic().filter(Taxa.order.ilike(term))


@register_handler(GENE_QUERIES)
def query_class(term):
    '''Generate Gene query by class'''
    return query_taxon_generic().filter(Taxa._class.ilike(term))


@register_handler(GENE_QUERIES)
def query_phylum(term):
    '''Generate Gene query by phylum'''
    return query_taxon_generic().filter(Taxa.phylum.ilike(term))


@register_handler(GENE_QUERIES)
def query_superkingdom(term):
    '''Generate Gene query by superkingdom'''
    return query_taxon_generic().filter(Taxa.superkingdom.ilike(term))


@register_handler(GENE_QUERIES)
def query_acc(term):
    '''Generate Gene query by NCBI accession number'''
    return Cds.query.join(Region).join(DnaSequence).filter(DnaSequence.accession.ilike(term))


@register_handler(GENE_QUERIES)
def query_type(term):
    '''Generate Gene query by cluster type'''
    return Cds.query.join(Region) \
                    .join(t_rel_regions_types).join(BgcType) \
                    .filter(or_(BgcType.term.ilike('%{}%'.format(term)), BgcType.description.ilike('%{}%'.format(term))))


@register_countable_handler(GENE_QUERIES)
def query_monomer(term):
    '''Generate Gene query by monomer'''
    return Cds.query.join(AsDomain).join(Module).join(RelModulesMonomer).join(Monomer) \
                    .filter(Monomer.name.ilike(term))


@register_handler(GENE_QUERIES)
def query_compoundseq(term):
    '''Generate Gene query by compound sequence'''
    return Cds.query.join(Ripp).filter(Ripp.peptide_sequence.ilike(term))


@register_handler(GENE_QUERIES)
def query_compoundclass(term):
    '''Generate Gene query by compound class'''
    return Cds.query.join(Ripp).filter(Ripp.subclass.ilike("%{}%".format(term)))


@register_handler(GENE_QUERIES)
def query_profile(term):
    '''Generate Gene query by BGC profile'''
    return Cds.query.join(ProfileHit).join(Profile) \
                    .filter(Profile.name.ilike(term))


@register_countable_handler(GENE_QUERIES, description="Genes containing a hit to the given PFAM ID")
def query_pfam(term):
    """Return a query for a gene by Pfam hit"""
    search = "%{}%".format(term)
    query = Cds.query.join(PfamDomain).join(Pfam)
    if term.lower().startswith("pfam"):
        return query.filter(Pfam.pfam_id.ilike(search))
    return query.filter(or_(Pfam.pfam_id.ilike(search), Pfam.name.ilike(search), Pfam.description.ilike(search)))


@register_handler(GENE_QUERIES)
def query_smcog(term):
    '''Generate Gene query by smCoG hit'''
    return Cds.query.join(SmcogHit, Cds.cds_id == SmcogHit.cds_id) \
                    .join(Smcog, SmcogHit.smcog_id == Smcog.smcog_id) \
                    .filter(Smcog.name.ilike(term))


@register_countable_handler(GENE_QUERIES)
def query_asdomain(term):
    '''Generate Gene query by AsDomain'''
    return Cds.query.join(AsDomain).join(AsDomainProfile) \
              .filter(AsDomainProfile.name.ilike(term))


def gene_by_x_clusterblast(term, algorithm):
    '''Generic search for gene by XClusterBlast match'''
    return Cds.query.join(Region) \
                    .join(ClusterblastHit).join(ClusterblastAlgorithm) \
                    .filter(ClusterblastAlgorithm.name == algorithm) \
                    .filter(ClusterblastHit.acc.ilike(term))


@register_handler(GENE_QUERIES)
def query_clusterblast(term):
    '''Generate Gene query by ClusterBlast hit'''
    return gene_by_x_clusterblast(term, 'clusterblast')


@register_handler(GENE_QUERIES)
def query_knowncluster(term):
    '''Generate Gene query by KnownClusterBlast hit'''
    return gene_by_x_clusterblast(term, 'knownclusterblast')


@register_handler(GENE_QUERIES)
def query_subcluster(term):
    '''Generate Gene query by SubClusterBlast hit'''
    return gene_by_x_clusterblast(term, 'subclusterblast')


##############
# Formatters #
#############

@register_handler(GENE_FORMATTERS)
def format_fasta(genes):
    '''Generate DNA FASTA records for a list of genes'''
    search = ''
    if g.verbose:
        search = "|{}".format(g.search_str)
    fasta_records = []
    for gene in genes:
        location = location_from_string(gene.location)
        record = gene.region.dna_sequence
        sequence = break_lines(calculate_sequence(location, record.dna))
        assert sequence
        result = ('>{gene.locus_tag}|{record.accession}.{record.version}|{gene.location}{search}\n'
                  '{sequence}').format(gene=gene, search=search, sequence=sequence, record=record)
        fasta_records.append(result)
    return fasta_records


@register_handler(GENE_FORMATTERS)
def format_fastaa(genes):
    '''Generate protein FASTA records for a list of genes'''
    search = ''
    if g.verbose:
        search = "|{}".format(g.search_str)
    fasta_records = []
    for gene in genes:
        record = gene.region.dna_sequence
        sequence = break_lines(gene.translation)
        record = ('>{g.locus_tag}|{record.accession}.{record.version}|{g.location}){search}\n'
                  '{sequence}').format(g=gene, search=search, sequence=sequence, record=record)
        fasta_records.append(record)

    return fasta_records


@register_handler(GENE_FORMATTERS)
def format_csv(genes):
    '''Generate CSV records for a list of genes'''
    csv_lines = ['#Locus tag\tAccession\tStart\tEnd\tStrand']
    for gene in genes:
        record = gene.region.dna_sequence
        csv_lines.append('{g.locus_tag}\t{record.accession}.{record.version}\t'
                         '{g.location}'.format(g=gene, record=record))
    return csv_lines


@register_handler(GENE_QUERIES)
def clusters_by_resfam(term):
    '''Return a query for a region by Resfam hit'''
    search = "%{}%".format(term)
    return Cds.query.join(ResfamDomain).join(Resfam).filter(or_(Resfam.accession.ilike(search), Resfam.name.ilike(search), Resfam.description.ilike(search)))
