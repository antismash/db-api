'''Gene-related search functions'''

import string

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
    BgcType,
    BiosyntheticGeneCluster as Bgc,
    DnaSequence,
    Gene,
    t_gene_cluster_map,
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


@register_handler(GENE_QUERIES)
def query_type(term):
    '''Generate Gene query by cluster type'''
    return Gene.query.join(t_gene_cluster_map, Gene.gene_id == t_gene_cluster_map.c.gene_id) \
                     .join(Bgc, t_gene_cluster_map.c.bgc_id == Bgc.bgc_id) \
                     .join(t_rel_clusters_types).join(BgcType) \
                     .filter(or_(BgcType.term.ilike('%{}%'.format(term)), BgcType.description.ilike('%{}%'.format(term))))



@register_handler(GENE_FORMATTERS)
def format_fasta(genes):
    '''Generate FASTA records for a list of genes'''
    fasta_records = []
    for gene in genes:
        sequence = break_lines(_extract_sequence(gene))
        record = '>{g.locus_tag}|{g.locus.sequence.acc}.{g.locus.sequence.version}|' \
                 '{g.locus.start_pos}-{g.locus.end_pos}({g.locus.strand})\n' \
                 '{sequence}'.format(g=gene, sequence=sequence)
        fasta_records.append(record)

    return fasta_records


def _extract_sequence(gene):
    '''Extract the sequence of a Gene'''
    sequence = db.session.query(func.substr(DnaSequence.dna, gene.locus.start_pos + 1, gene.locus.end_pos - gene.locus.start_pos)) \
                         .filter_by(sequence_id=gene.locus.sequence_id).one()[0]
    if gene.locus.strand == '-':
        sequence = reverse_completement(sequence)
    return sequence


TRANS_TABLE = string.maketrans('ATGCatgc', 'TACGtacg')


def reverse_completement(sequence):
    '''return the reverse complement of a sequence'''
    return str(sequence).translate(TRANS_TABLE)[::-1]
