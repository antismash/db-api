'''Cluster-related search options'''

from flask import g

from sqlalchemy import (
    or_,
    sql,
)
from sqlalchemy.orm import joinedload
from .helpers import (
    break_lines,
    register_handler,
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
    Genome,
    DnaSequence,
    Module,
    Monomer,
    Profile,
    ProfileHit,
    Protocluster,
    Region,
    RelModulesMonomer,
    Ripp,
    Smcog,
    SmcogHit,
    Taxa,
    t_rel_regions_types,
)

CLUSTERS = {}
CLUSTER_FORMATTERS = {}


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_json(clusters):
    '''Convert model.BiosyntheticGeneClusters into JSON'''
    query = db.session.query(Region)
    query = query.options(joinedload('bgc_types')).options(joinedload('clusterblast_hits'))
    query = query.filter(Region.region_id.in_(map(lambda x: x.region_id, clusters)))
    query = query.order_by(Region.region_id)
    json_clusters = []
    for cluster in query.all():
        json_cluster = {}
        json_cluster['bgc_id'] = cluster.region_id
        json_cluster['region_number'] = cluster.region_number

        location = location_from_string(cluster.location)

        json_cluster['start_pos'] = location.start
        json_cluster['end_pos'] = location.end

        json_cluster['acc'] = cluster.dna_sequence.accession
        json_cluster['assembly_id'] = cluster.dna_sequence.genome.assembly_id.split('.')[0] if cluster.dna_sequence.genome.assembly_id else ''
        json_cluster['version'] = cluster.dna_sequence.version

        json_cluster['genus'] = cluster.dna_sequence.genome.tax.genus
        json_cluster['species'] = cluster.dna_sequence.genome.tax.species
        json_cluster['strain'] = cluster.dna_sequence.genome.tax.strain

        term = '-'.join(sorted([t.term for t in cluster.bgc_types]))
        if len(cluster.bgc_types) == 1:
            json_cluster['description'] = cluster.bgc_types[0].description
            json_cluster['term'] = term
        else:
            descs = ' & '.join(sorted([t.description for t in cluster.bgc_types],
                                      key=str.casefold))
            json_cluster['description'] = 'Hybrid cluster: {}'.format(descs)
            json_cluster['term'] = '{} hybrid'.format(term)

        json_cluster['similarity'] = None
        json_cluster['cbh_description'] = None
        json_cluster['cbh_acc'] = None

        knownclusterblasts = [hit for hit in cluster.clusterblast_hits if hit.algorithm.name == 'knownclusterblast' and hit.rank == 1]
        if len(knownclusterblasts) > 0:
            json_cluster['similarity'] = knownclusterblasts[0].similarity
            json_cluster['cbh_description'] = knownclusterblasts[0].description
            json_cluster['cbh_acc'] = knownclusterblasts[0].acc
            json_cluster['cbh_rank'] = knownclusterblasts[0].rank

        json_cluster['contig_edge'] = cluster.contig_edge
        json_cluster['minimal'] = cluster.minimal

        json_clusters.append(json_cluster)
    return json_clusters


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_csv(clusters):
    '''Convert model.BiosyntheticGeneClusters into CSV'''
    json_clusters = clusters_to_json(clusters)
    csv_lines = ['#Genus\tSpecies\tStrain\tNCBI accession\tFrom\tTo\tBGC type\tOn contig edge\tFast mode only\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL\tDownload URL']
    for cluster in json_clusters:
        csv_lines.append('{genus}\t{species}\t{strain}\t{acc}.{version}\t{start_pos}\t{end_pos}\t{term}\t'
                         '{contig_edge}\t{minimal}\t'
                         '{cbh_description}\t{similarity}\t{cbh_acc}\t'
                         'https://antismash-db.secondarymetabolites.org/go/{assembly_id}/{acc}.{version}/{start_pos}.{end_pos}\t'
                         'https://antismash-db.secondarymetabolites.org/api/v1.0/download/genbank/{assembly_id}/{acc}.{version}/{start_pos}.{end_pos}'.format(**cluster))
    return csv_lines


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_fasta(clusters):
    '''Convert model.BiosyntheticGeneCluster into FASTA'''
    query = db.session.query(Region, DnaSequence.accession, DnaSequence.version,
                             Taxa.tax_id, Taxa.genus, Taxa.species, Taxa.strain)
    query = query.options(joinedload('bgc_types'))
    query = query.join(DnaSequence).join(Genome).join(Taxa)
    query = query.filter(Region.region_id.in_(map(lambda x: x.region_id, clusters))).order_by(Region.region_id)
    search = ''
    if g.verbose:
        search = "|{}".format(g.search_str)
    for cluster in query:
        location = location_from_string(cluster.Region.location)
        seq = break_lines(cluster.Region.dna_sequence.dna[location.start+1:location.end])
        compiled_type = '-'.join(sorted([t.term for t in cluster.Region.bgc_types], key=str.casefold))
        fasta = '>{c.accession}.{c.version}|{location.start}-{location.end}|' \
                '{compiled_type}|' \
                '{c.genus} {c.species} {c.strain}{search}\n{seq}' \
                .format(c=cluster, region_number=cluster.Region.region_number,
                        compiled_type=compiled_type, search=search, seq=seq, location=location)
        yield fasta


def cluster_query_from_term(term):
    '''Recursively generate an SQL query from the search terms'''
    if term.kind == 'expression':
        if term.category == 'unknown':
            term.category = guess_cluster_category(term)
        if term.category in CLUSTERS:
            return CLUSTERS[term.category](term.term)
        else:
            return Region.query.filter(sql.false())
    elif term.kind == 'operation':
        left_query = cluster_query_from_term(term.left)
        right_query = cluster_query_from_term(term.right)
        if term.operation == 'except':
            return left_query.except_(right_query)
        elif term.operation == 'or':
            return left_query.union(right_query)
        elif term.operation == 'and':
            return left_query.intersect(right_query)

    return Region.query.filter(sql.false())


def guess_cluster_category(term):
    '''Guess cluster search category from term'''
    if BgcType.query.filter(BgcType.term.ilike(term.term)).count() > 0:
        return 'type'
    if DnaSequence.query.filter(DnaSequence.accession.ilike(term.term)).count() > 0:
        return 'acc'
    if Taxa.query.filter(Taxa.genus.ilike(term.term)).count() > 0:
        return 'genus'
    if Taxa.query.filter(Taxa.species.ilike(term.term)).count() > 0:
        return 'species'

    return term.category


@register_handler(CLUSTERS)
def clusters_by_type(term):
    '''Return a query for a bgc by type or type description search'''
    all_subtypes = db.session.query(BgcType).filter(or_(BgcType.term == term, BgcType.description.ilike('%{}%'.format(term)))).cte(recursive=True)
    all_subtypes = all_subtypes.union(db.session.query(BgcType).filter(BgcType.parent_id == all_subtypes.c.bgc_type_id))
    return db.session.query(Region).join(t_rel_regions_types).join(all_subtypes)


@register_handler(CLUSTERS)
def clusters_by_taxid(term):
    '''Return a query for a bgc by NCBI taxid'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.tax_id == term)


@register_handler(CLUSTERS)
def clusters_by_strain(term):
    '''Return a query for a bgc by strain search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.strain.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_species(term):
    '''Return a query for a bgc by species search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.species.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_genus(term):
    '''Return a query for a bgc by genus search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.genus.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_family(term):
    '''Return a query for a bgc by family search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.family.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_order(term):
    '''Return a query for a bgc by order search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.taxonomic_order.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_class(term):
    '''Return a query for a bgc by class search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa._class.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_phylum(term):
    '''Return a query for a bgc by phylum search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.phylum.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_superkingdom(term):
    '''Return a query for a bgc by superkingdom search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.superkingdom.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_monomer(term):
    '''Return a query for a bgc by monomer or monomer description search'''
    return Region.query.join(Module).join(RelModulesMonomer).join(Monomer).filter(Monomer.name.ilike(term.lower()))


@register_handler(CLUSTERS)
def clusters_by_acc(term):
    '''Return a query for a bgc by accession number search'''
    return Region.query.join(DnaSequence).filter(DnaSequence.accession.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_assembly(term):
    """Return a query for a bgc by assembly_id search"""
    return Region.query.join(DnaSequence).join(Genome).filter(Genome.assembly_id.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_compoundseq(term):
    '''Return a query for a bgc by compound sequence search'''
    return Region.query.join(Protocluster).join(Ripp).filter(Ripp.peptide_sequence.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_compoundclass(term):
    '''Return a query for a bgc by compound class'''
    return Region.query.join(Protocluster).join(Ripp).filter(Ripp.subclass.ilike(term))


@register_handler(CLUSTERS)
def clusters_by_profile(term):
    '''Return a query for a bgc by profile name'''
    return Region.query.join(Cds) \
                 .join(ProfileHit).join(Profile) \
                 .filter(Profile.name.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_smcog(term):
    '''Return a query for a bgc by smcog'''
    return Region.query.join(Cds) \
                 .join(SmcogHit).join(Smcog) \
                 .filter(Smcog.name.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_asdomain(term):
    '''Return a query for a bgc by asdomain name'''
    return Region.query.join(Cds, Region.region_id == Cds.region_id) \
                 .join(AsDomain).join(AsDomainProfile) \
                 .filter(AsDomainProfile.name.ilike(term))


@register_handler(CLUSTERS)
def clusters_by_contigedge(term):
    return Region.query.filter(Region.contig_edge.is_(term))


@register_handler(CLUSTERS)
def clusters_by_minimal(term):
    return Region.query.filter(Region.minimal.is_(term))


def clusters_by_x_clusterblast(term, algorithm):
    '''Generic query for XClusterBlast hits'''
    return Region.query.join(ClusterblastHit).join(ClusterblastAlgorithm) \
                 .filter(ClusterblastAlgorithm.name == algorithm) \
                 .filter(ClusterblastHit.acc.ilike("%{}%".format(term)))  # TODO: not sure if this shouldn't be straight ilike


@register_handler(CLUSTERS)
def clusters_by_clusterblast(term):
    '''Return a query for a bgc by ClusterBlast hit'''
    return clusters_by_x_clusterblast(term, 'clusterblast')


@register_handler(CLUSTERS)
def clusters_by_knowncluster(term):
    '''Return a query for a bgc by KnownClusterBlast hit'''
    return clusters_by_x_clusterblast(term, 'knownclusterblast')


@register_handler(CLUSTERS)
def clusters_by_subcluster(term):
    '''Return a query for a bgc by SubClusterBlast hit'''
    return clusters_by_x_clusterblast(term, 'subclusterblast')
