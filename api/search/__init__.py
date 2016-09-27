'''Search-related functions'''
from sqlalchemy import (
    distinct,
    func,
    null,
    or_,
    sql,
)
from api.models import (
    db,
    AsDomain,
    AsDomainProfile,
    BgcType,
    BiosyntheticGeneCluster as Bgc,
    Compound,
    Gene,
    Genome,
    DnaSequence,
    Locus,
    Monomer,
    Profile,
    ProfileHit,
    Taxa,
    t_gene_cluster_map,
    t_rel_clusters_compounds,
    t_rel_clusters_types,
    RelCompoundsMonomer,
)


AVAILABLE = {}
CLUSTERS = {}
CLUSTER_FORMATTERS = {}
GENE_QUERIES = {}


class NoneQuery(object):
    '''A 'no result' return object'''
    def all(self):
        '''Just return an empty list'''
        return []


def register_handler(handler):
    '''Decorator to register a function as a handler'''
    def real_decorator(function):
        name = function.func_name.split('_')[-1]
        handler[name] = function

        def inner(*args, **kwargs):
            return function(*args, **kwargs)
        return inner
    return real_decorator


def core_search(query):
    '''Actually run the search logic'''
    sql_query = NoneQuery()

    if query.search_type == 'cluster':
        sql_query = cluster_query_from_term(query.terms)
    elif query.search_type == 'gene':
        sql_query = gene_query_from_term(query.terms)

    results = sql_query.all()

    return results


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_json(clusters):
    '''Convert model.BiosyntheticGeneClusters into JSON'''
    json_clusters = []
    for cluster in clusters:
        json_cluster = {}
        json_cluster['bgc_id'] = cluster.bgc_id
        json_cluster['cluster_number'] = cluster.cluster_number

        json_cluster['start_pos'] = cluster.locus.start_pos
        json_cluster['end_pos'] = cluster.locus.end_pos

        json_cluster['acc'] = cluster.locus.sequence.acc
        json_cluster['version'] = cluster.locus.sequence.version

        json_cluster['genus'] = cluster.locus.sequence.genome.tax.genus
        json_cluster['species'] = cluster.locus.sequence.genome.tax.species
        json_cluster['strain'] = cluster.locus.sequence.genome.tax.strain

        term = '-'.join([t.term for t in cluster.bgc_types])
        if len(cluster.bgc_types) == 1:
            json_cluster['description'] = cluster.bgc_types[0].description
            json_cluster['term'] = term
        else:
            json_cluster['description'] = 'Hybrid cluster: {}'.format(term)
            json_cluster['term'] = '{} hybrid'.format(term)

        json_cluster['similarity'] = None
        json_cluster['cbh_description'] = None
        json_cluster['cbh_acc'] = None

        knownclusterblasts = [hit for hit in cluster.clusterblast_hits if hit.algorithm.name == 'knownclusterblast']
        if len(knownclusterblasts) > 0:
            json_cluster['similarity'] = knownclusterblasts[0].similarity
            json_cluster['cbh_description'] = knownclusterblasts[0].description
            json_cluster['cbh_acc'] = knownclusterblasts[0].acc

        json_clusters.append(json_cluster)
    return json_clusters


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_csv(clusters):
    '''Convert model.BiosyntheticGeneClusters into CSV'''
    json_clusters = clusters_to_json(clusters)
    csv_lines = ['#Genus\tSpecies\tNCBI accession\tCluster number\tBGC type\tFrom\tTo\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL']
    for cluster in json_clusters:
        csv_lines.append('{genus}\t{species}\t{acc}.{version}\t{cluster_number}\t{term}\t{start_pos}\t{end_pos}\t'
                         '{cbh_description}\t{similarity}\t{cbh_acc}\t'
                         'http://antismash-db.secondarymetabolites.org/output/{acc}/index.html#cluster-{cluster_number}'.format(**cluster))
    return csv_lines


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_fasta(clusters):
    '''Convert model.BiosyntheticGeneCluster into FASTA'''
    fasta_records = []
    for cluster in clusters:
        compiled_type = '-'.join([t.term for t in cluster.bgc_types])
        seq = break_lines(cluster.locus.sequence.dna)
        fasta = '>{c.locus.sequence.acc}.{c.locus.sequence.version}|Cluster {c.cluster_number}|' \
                '{compiled_type}|{c.locus.start_pos}-{c.locus.end_pos}|' \
                '{c.locus.sequence.genome.tax.genus} {c.locus.sequence.genome.tax.species} ' \
                '{c.locus.sequence.genome.tax.strain}\n{seq}' \
                .format(c=cluster, compiled_type=compiled_type, seq=seq)
        fasta_records.append(fasta)

    return fasta_records


def break_lines(string, width=80):
    '''Break up a long string to lines of width (default: 80)'''
    parts = []
    for w in range(0, len(string), width):
        parts.append(string[w:w + width])

    return '\n'.join(parts)


def cluster_query_from_term(term):
    '''Recursively generate an SQL query from the search terms'''
    if term.kind == 'expression':
        if term.category == 'unknown':
            term.category = guess_cluster_category(term)
        if term.category in CLUSTERS:
            return CLUSTERS[term.category](term.term)
        else:
            return Bgc.query.filter(sql.false())
    elif term.kind == 'operation':
        left_query = cluster_query_from_term(term.left)
        right_query = cluster_query_from_term(term.right)
        if term.operation == 'except':
            return left_query.except_(right_query)
        elif term.operation == 'or':
            return left_query.union(right_query)
        elif term.operation == 'and':
            return left_query.intersect(right_query)

    return Bgc.query.filter(sql.false())


def guess_cluster_category(term):
    '''Guess cluster search category from term'''
    if BgcType.query.filter(BgcType.term.ilike(term.term)).count() > 0:
        return 'type'
    if DnaSequence.query.filter(DnaSequence.acc.ilike(term.term)).count() > 0:
        return 'acc'
    if Taxa.query.filter(Taxa.genus.ilike(term.term)).count() > 0:
        return 'genus'
    if Taxa.query.filter(Taxa.species.ilike(term.term)).count() > 0:
        return 'species'

    return term.category


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


@register_handler(CLUSTERS)
def clusters_by_type(term):
    '''Return a query for a bgc by type or type description search'''
    all_subtypes = db.session.query(BgcType).filter(or_(BgcType.term == term, BgcType.description.ilike('%{}%'.format(term)))).cte(recursive=True)
    all_subtypes = all_subtypes.union(db.session.query(BgcType).filter(BgcType.parent_id == all_subtypes.c.bgc_type_id))
    return db.session.query(Bgc).join(t_rel_clusters_types).join(all_subtypes)


@register_handler(CLUSTERS)
def clusters_by_strain(term):
    '''Return a query for a bgc by strain search'''
    return Bgc.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.strain.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_species(term):
    '''Return a query for a bgc by species search'''
    return Bgc.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.species.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_genus(term):
    '''Return a query for a bgc by genus search'''
    return Bgc.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.genus.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_family(term):
    '''Return a query for a bgc by family search'''
    return Bgc.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.family.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_order(term):
    '''Return a query for a bgc by order search'''
    return Bgc.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.taxonomic_order.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_class(term):
    '''Return a query for a bgc by class search'''
    return Bgc.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa).filter(Taxa._class.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_phylum(term):
    '''Return a query for a bgc by phylum search'''
    return Bgc.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.phylum.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_superkingdom(term):
    '''Return a query for a bgc by superkingdom search'''
    return Bgc.query.join(Locus).join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.superkingdom.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_monomer(term):
    '''Return a query for a bgc by monomer or monomer description search'''
    return Bgc.query.join(t_rel_clusters_compounds).join(RelCompoundsMonomer, t_rel_clusters_compounds.c.compound_id == RelCompoundsMonomer.compound_id).join(Monomer).filter(
        or_(Monomer.name.ilike(term), Monomer.description.ilike('%{}%'.format(term))))


@register_handler(CLUSTERS)
def clusters_by_acc(term):
    '''Return a query for a bgc by accession number search'''
    return Bgc.query.join(Locus).join(DnaSequence).filter(DnaSequence.acc.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_compoundseq(term):
    '''Return a query for a bgc by compound sequence search'''
    return Bgc.query.join(t_rel_clusters_compounds).join(Compound).filter(Compound.peptide_sequence.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def cluster_by_profile(term):
    '''Return a query for a bgc by profile name'''
    return Bgc.query.join(t_gene_cluster_map, t_gene_cluster_map.c.bgc_id == Bgc.bgc_id) \
                    .join(Gene, t_gene_cluster_map.c.gene_id == Gene.gene_id) \
                    .join(ProfileHit).join(Profile) \
                    .filter(Profile.name.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def cluster_by_asdomain(term):
    '''Return a query for a bgc by asdomain name'''
    return Bgc.query.join(t_gene_cluster_map, t_gene_cluster_map.c.bgc_id == Bgc.bgc_id) \
                    .join(Gene, t_gene_cluster_map.c.gene_id == Gene.gene_id) \
                    .join(AsDomain).join(AsDomainProfile) \
                    .filter(AsDomainProfile.name.ilike(term))


WHITELIST = set()
for item in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
    WHITELIST.add(item)
    WHITELIST.add(item.lower())
for item in '0123456789':
    WHITELIST.add(item)
WHITELIST.add('_')
WHITELIST.add('-')
WHITELIST.add('(')
WHITELIST.add(')')
WHITELIST.add(' ')


def sanitise_string(search_string):
    '''Remove all non-whitelisted characters from the search string

    >>> sanitise_string('fOo')
    'fOo'
    >>> sanitise_string('%bar')
    'bar'

    '''
    cleaned = []
    for symbol in search_string:
        if symbol in WHITELIST:
            cleaned.append(symbol)

    return ''.join(cleaned)


def available_term_by_category(category, term):
    '''List all available terms by category'''
    cleaned_category = sanitise_string(category)
    cleaned_term = sanitise_string(term)

    if cleaned_category in AVAILABLE:
        query = AVAILABLE[cleaned_category](cleaned_term)
        return map(lambda x: {'val': x[0], 'desc': x[1]}, query.all())

    return []


@register_handler(AVAILABLE)
def available_superkingdom(term):
    '''Generate query for available superkingdoms'''
    return db.session.query(distinct(Taxa.superkingdom), null()).filter(Taxa.superkingdom.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_phylum(term):
    '''Generate query for available phyla'''
    return db.session.query(distinct(Taxa.phylum), null()).filter(Taxa.phylum.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_class(term):
    '''Generate query for available class'''
    return db.session.query(distinct(Taxa._class), null()).filter(Taxa._class.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_order(term):
    '''Generate query for available order'''
    return db.session.query(distinct(Taxa.taxonomic_order), null()).filter(Taxa.taxonomic_order.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_family(term):
    '''Generate query for available family'''
    return db.session.query(distinct(Taxa.family), null()).filter(Taxa.family.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_genus(term):
    '''Generate query for available genus'''
    return db.session.query(distinct(Taxa.genus), null()).filter(Taxa.genus.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_species(term):
    '''Generate query for available species'''
    return db.session.query(distinct(Taxa.species), null()).filter(Taxa.species.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_strain(term):
    '''Generate query for available strain'''
    return db.session.query(distinct(Taxa.strain), null()).filter(Taxa.strain.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_acc(term):
    '''Generate query for available accession'''
    return db.session.query(distinct(DnaSequence.acc), null()).filter(DnaSequence.acc.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_compound(term):
    '''Generate query for available compound by peptide sequence'''
    return db.session.query(distinct(Compound.peptide_sequence), null()).filter(Compound.peptide_sequence.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_monomer(term):
    '''Generate query for available monomer'''
    return db.session.query(distinct(Monomer.name), Monomer.description).filter(or_(Monomer.name.ilike('{}%'.format(term)), Monomer.description.ilike('{}%'.format(term))))


@register_handler(AVAILABLE)
def available_type(term):
    '''Generate query for available type'''
    return db.session.query(distinct(BgcType.term), BgcType.description).filter(or_(BgcType.term.ilike('{}%'.format(term)), BgcType.description.ilike('{}%'.format(term))))


@register_handler(AVAILABLE)
def available_profile(term):
    '''Generate query for available asDomain profile'''
    return db.session.query(distinct(Profile.name), Profile.description) \
             .filter(or_(Profile.name.ilike('{}%'.format(term)), Profile.description.ilike('%{}%'.format(term))))


@register_handler(AVAILABLE)
def available_asdomain(term):
    '''Generate query for available asDomain profile'''
    return db.session.query(distinct(AsDomainProfile.name), AsDomainProfile.description) \
             .filter(or_(AsDomainProfile.name.ilike('{}%'.format(term)), AsDomainProfile.description.ilike('%{}%'.format(term))))
