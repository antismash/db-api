'''Gene-related search functions'''

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
)

from api.models import (
    db,
    AsDomain,
    AsDomainProfile,
    BgcType,
    BiosyntheticGeneCluster as Bgc,
    ClusterblastAlgorithm,
    ClusterblastHit,
    Compound,
    DnaSequence,
    Cds,
    Genome,
    Locus,
    Monomer,
    Profile,
    ProfileHit,
    Taxa,
    RelAsDomainsMonomer,
    Smcog,
    SmcogHit,
    t_cds_cluster_map,
    t_rel_clusters_types,
)

GENE_QUERIES = {}
GENE_FORMATTERS = {}


def gene_query_from_term(term):
    '''Recursively generate an SQL query from the search terms'''
    if term.kind == 'expression':
        if term.category in GENE_QUERIES:
            return GENE_QUERIES[term.category](term.term)
        else:
            return Cds.query.filter(sql.false())
    elif term.kind == 'operation':
        left_query = gene_query_from_term(term.left)
        right_query = gene_query_from_term(term.right)
        if term.operation == 'except':
            return left_query.except_(right_query)
        elif term.operation == 'or':
            return left_query.union(right_query)
        elif term.operation == 'and':
            return left_query.intersect(right_query)

    return Cds.query.filter(sql.false())


def query_taxon_generic():
    '''Generate Gene query by taxonomy'''
    return Cds.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa)


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
    return Cds.query.join(Locus).join(DnaSequence).filter(DnaSequence.acc.ilike(term))


@register_handler(GENE_QUERIES)
def query_type(term):
    '''Generate Gene query by cluster type'''
    return Cds.query.join(t_cds_cluster_map, Cds.cds_id == t_cds_cluster_map.c.cds_id) \
                    .join(Bgc, t_cds_cluster_map.c.bgc_id == Bgc.bgc_id) \
                    .join(t_rel_clusters_types).join(BgcType) \
                    .filter(or_(BgcType.term.ilike('%{}%'.format(term)), BgcType.description.ilike('%{}%'.format(term))))


@register_handler(GENE_QUERIES)
def query_monomer(term):
    '''Generate Gene query by monomer'''
    return Cds.query.join(AsDomain).join(RelAsDomainsMonomer).join(Monomer) \
                    .filter(Monomer.name.ilike(term))


@register_handler(GENE_QUERIES)
def query_compoundseq(term):
    '''Generate Gene query by compound sequence'''
    return Cds.query.join(Compound, Cds.locus_tag == Compound.locus_tag) \
                    .filter(Compound.peptide_sequence.ilike(term))


@register_handler(GENE_QUERIES)
def query_compoundclass(term):
    '''Generate Gene query by compound class'''
    return Cds.query.join(Compound, Cds.locus_tag == Compound.locus_tag) \
                    .filter(Compound._class.ilike(term))


@register_handler(GENE_QUERIES)
def query_profile(term):
    '''Generate Gene query by BGC profile'''
    return Cds.query.join(ProfileHit).join(Profile) \
                    .filter(Profile.name.ilike(term))


@register_handler(GENE_QUERIES)
def query_smcog(term):
    '''Generate Gene query by smCoG hit'''
    return Cds.query.join(SmcogHit, Cds.cds_id == SmcogHit.cds_id) \
                    .join(Smcog, SmcogHit.smcog_id == Smcog.smcog_id) \
                    .filter(Smcog.name.ilike(term))


@register_handler(GENE_QUERIES)
def query_asdomain(term):
    '''Generate Gene query by AsDomain'''
    return Cds.query.join(AsDomain).join(AsDomainProfile) \
              .filter(AsDomainProfile.name.ilike(term))


def gene_by_x_clusterblast(term, algorithm):
    '''Generic search for gene by XClusterBlast match'''
    return Cds.query.join(t_cds_cluster_map, Cds.cds_id == t_cds_cluster_map.c.cds_id) \
                    .join(Bgc, t_cds_cluster_map.c.bgc_id == Bgc.bgc_id) \
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
    query = db.session.query(Cds.cds_id, Cds.locus_tag, Locus.start_pos, Locus.end_pos, Locus.strand,
                             DnaSequence.acc, DnaSequence.version,
                             func.substr(DnaSequence.dna, Locus.start_pos + 1, Locus.end_pos - Locus.start_pos).label('sequence'))
    query = query.join(Locus).join(DnaSequence)
    query = query.filter(Cds.cds_id.in_(map(lambda x: x.cds_id, genes))).order_by(Cds.cds_id)
    search = ''
    if g.verbose:
        search = "|{}".format(g.search_str)
    fasta_records = []
    for gene in query:
        sequence = break_lines(calculate_sequence(gene.strand, gene.sequence))
        record = '>{g.locus_tag}|{g.acc}.{g.version}|' \
                 '{g.start_pos}-{g.end_pos}({g.strand}){search}\n' \
                 '{sequence}'.format(g=gene, search=search, sequence=sequence)
        fasta_records.append(record)

    return fasta_records


@register_handler(GENE_FORMATTERS)
def format_fastaa(genes):
    '''Generate protein FASTA records for a list of genes'''
    query = db.session.query(Cds.cds_id, Cds.locus_tag, Locus.start_pos, Locus.end_pos, Locus.strand,
                             DnaSequence.acc, DnaSequence.version, Cds.translation)
    query = query.join(Locus).join(DnaSequence)
    query = query.filter(Cds.cds_id.in_(map(lambda x: x.cds_id, genes))).order_by(Cds.cds_id)
    search = ''
    if g.verbose:
        search = "|{}".format(g.search_str)
    fasta_records = []
    for gene in query:
        sequence = break_lines(gene.translation)
        record = '>{g.locus_tag}|{g.acc}.{g.version}|' \
                 '{g.start_pos}-{g.end_pos}({g.strand}){search}\n' \
                 '{sequence}'.format(g=gene, search=search, sequence=sequence)
        fasta_records.append(record)

    return fasta_records


@register_handler(GENE_FORMATTERS)
def format_csv(genes):
    '''Generate CSV records for a list of genes'''
    query = db.session.query(Cds.locus_tag, Locus.start_pos, Locus.end_pos, Locus.strand, DnaSequence.acc, DnaSequence.version)
    query = query.join(Locus).join(DnaSequence)
    query = query.filter(Cds.cds_id.in_(map(lambda x: x.cds_id, genes))).order_by(Cds.cds_id)
    csv_lines = ['#Locus tag\tAccession\tStart\tEnd\tStrand']
    for gene in query:
        csv_lines.append('{g.locus_tag}\t{g.acc}.{g.version}\t'
                         '{g.start_pos}\t{g.end_pos}\t{g.strand}'.format(g=gene))
    return csv_lines
