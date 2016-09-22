'''Search-related functions'''
import re
from .helpers import get_db
import sql

from sqlalchemy import (
    distinct,
    func,
    or_,
)
from .models import (
    db,
    BgcType,
    BiosyntheticGeneCluster as Bgc,
    Compound,
    Genome,
    DnaSequence,
    Locus,
    Monomer,
    Taxa,
    t_rel_clusters_compounds,
    t_rel_clusters_types,
    RelCompoundsMonomer,
)


AVAILABLE = {}
CLUSTERS = {}


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


def create_cluster_json(bgc_id):
    '''Create the JSONifiable record for a given cluster'''
    cur = get_db().cursor()
    cur.execute(sql.CLUSTER_INFO, (bgc_id, bgc_id))
    ret = cur.fetchall()
    cluster_json = {}
    for i, name in enumerate(ret[0]._fields):
        cluster_json[name] = ret[0][i]
    if len(ret) > 1:
        cluster_json['description'] = 'Hybrid cluster: '
        for i in range(1, len(ret)):
            cluster_json['term'] += '-{}'.format(ret[i].term)
        cluster_json['description'] += cluster_json['term']
        cluster_json['term'] += ' hybrid'
    return cluster_json


def create_cluster_csv(bgc_id):
    '''Create a CSV record for a given cluster'''
    cur = get_db().cursor()
    cur.execute(sql.CLUSTER_INFO, (bgc_id, bgc_id))
    ret = cur.fetchall()
    cluster = {}
    for i, name in enumerate(ret[0]._fields):
        cluster[name] = ret[0][i]
    if len(ret) > 1:
        cluster['description'] = 'Hybrid cluster: '
        for i in range(1, len(ret)):
            cluster['term'] += '-{}'.format(ret[i].term)
        cluster['description'] += cluster['term']
        cluster['term'] += ' hybrid'

    return '{species}\t{acc}.{version}\t{cluster_number}\t{term}\t{start_pos}\t{end_pos}\t{cbh_description}\t{similarity}\t{cbh_acc}\thttp://antismash-db.secondarymetabolites.org/output/{acc}/index.html#cluster-{cluster_number}'.format(**cluster)


def search_bgcs(search_string, offset=0, paginate=0, mapfunc=create_cluster_json):
    '''search for BGCs specified by the given search string, returning a list of found bgcs'''
    if '[' in search_string:
        parsed_query = parse_search_string(search_string)
    else:
        parsed_query = parse_simple_search(search_string)

    collected_sets = []
    all_clusters = set()
    for entry in parsed_query:
        found_clusters = clusters_by_category(entry['category'], entry['term'])
        collected_sets.append(found_clusters)
        all_clusters = all_clusters.union(found_clusters)

    final = all_clusters.copy()
    for i, result in enumerate(collected_sets):
        if parsed_query[i]['operation'] == 'or':
            final = final.union(result)
        elif parsed_query[i]['operation'] == 'not':
            final = final.difference(result)
        else:
            final = final.intersection(result)
    bgc_list = list(final)
    bgc_list.sort()
    total = len(bgc_list)
    stats = calculate_stats(bgc_list)
    if paginate > 0:
        end = min(offset + paginate, total)
    else:
        end = total
    return total, stats, map(mapfunc, bgc_list[offset:end])


def core_search(query):
    '''Actually run the search logic'''
    sql_query = NoneQuery()

    if query.search_type == 'cluster':
        sql_query = cluster_query_from_term(query.terms)

    results = sql_query.all()

    return results


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


def clusters_to_csv(clusters):
    '''Convert model.BiosyntheticGeneClusters into CSV'''
    json_clusters = clusters_to_json(clusters)
    print(json_clusters)
    csv_lines = ['#Species\tNCBI accession\tCluster number\tBGC type\tFrom\tTo\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL']
    for cluster in json_clusters:
        csv_lines.append('{species}\t{acc}.{version}\t{cluster_number}\t{term}\t{start_pos}\t{end_pos}\t'
                         '{cbh_description}\t{similarity}\t{cbh_acc}\t'
                         'http://antismash-db.secondarymetabolites.org/output/{acc}/index.html#cluster-{cluster_number}'.format(**cluster))
    return csv_lines


def cluster_query_from_term(term):
    '''Recursively generate an SQL query from the search terms'''
    if term.kind == 'expression':
        if term.category in CLUSTERS:
            return CLUSTERS[term.category](term.term)
        else:
            return NoneQuery()
    elif term.kind == 'operation':
        left_query = cluster_query_from_term(term.left)
        right_query = cluster_query_from_term(term.right)
        if term.operation == 'except':
            if isinstance(left_query, NoneQuery):
                return NoneQuery()
            if isinstance(right_query, NoneQuery):
                return left_query
            return left_query.except_(right_query)
        elif term.operation == 'or':
            if isinstance(left_query, NoneQuery):
                return right_query
            if isinstance(right_query, NoneQuery):
                return left_query
            return left_query.union(right_query)
        elif term.operation == 'and':
            if isinstance(left_query, NoneQuery) or isinstance(right_query, NoneQuery):
                return NoneQuery()
            return left_query.intersect(right_query)

    return NoneQuery()


def calculate_stats(bgc_list):
    '''Calculate some stats on the search results'''
    cur = get_db().cursor()
    stats = {}
    if len(bgc_list) < 1:
        return stats

    cur.execute(sql.SEARCH_SUMMARY_TYPES, (bgc_list,))
    clusters_by_type_list = cur.fetchall()
    clusters_by_type = {}
    if clusters_by_type_list is not None:
        clusters_by_type['labels'], clusters_by_type['data'] = zip(*clusters_by_type_list)
    stats['clusters_by_type'] = clusters_by_type

    cur.execute(sql.SEARCH_SUMMARY_PHYLUM, (bgc_list,))
    clusters_by_phylum_list = cur.fetchall()
    clusters_by_phylum = {}
    if clusters_by_phylum_list is not None:
        clusters_by_phylum['labels'], clusters_by_phylum['data'] = zip(*clusters_by_phylum_list)
    stats['clusters_by_phylum'] = clusters_by_phylum

    return stats


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



def parse_search_string(search_string):
    '''Parse a search string

    Given a search string like "[fieldname1]searchterm1 [fieldname2]searchterm2", return the parsed representation.
    >>> parse_search_string("[type]lanthipeptide [genus]Streptomyces")
    [{'category': 'type', 'term': 'lanthipeptide', 'operation': 'and'}, {'category': 'genus', 'term': 'Streptomyces', 'operation': 'and'}]

    Optionally, the fieldname can be followed by ':OP', where 'OP' is either 'and', 'or' or 'not'
    >>> parse_search_string("[genus:and]Streptomyces [type:or]ripp [type:not]lasso")
    [{'category': 'genus', 'term': 'Streptomyces', 'operation': 'and'}, {'category': 'type', 'term': 'ripp', 'operation': 'or'}, {'category': 'type', 'term': 'lasso', 'operation': 'not'}]
    '''

    parsed = []
    pattern = r'\[(\w+)(:\w+)?\](\w+)'
    for match in re.finditer(pattern, search_string):
        if match.group(2) is None:
            operation = "and"
        else:
            operation = match.group(2)[1:]
        parsed.append({'category': match.group(1), 'term': match.group(3), 'operation': operation})

    return parsed


def parse_simple_search(search_string):
    '''Parse a search string that doesn't specify categories'''
    cur = get_db().cursor()

    parsed = []
    cleaned_terms = [sanitise_string(t) for t in search_string.split()]

    for term in cleaned_terms:
        cur.execute(sql.SEARCH_IS_TYPE, (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'type', 'term': ret.term, 'operation': 'and'})
            continue

        cur.execute(sql.SEARCH_IS_ACC, (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'acc', 'term': ret.acc, 'operation': 'and'})
            continue

        cur.execute(sql.SEARCH_IS_GENUS, (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'genus', 'term': ret.genus, 'operation': 'and'})
            continue

        cur.execute(sql.SEARCH_IS_SPECIES, ('% {}'.format(term), ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'species', 'term': ret.species, 'operation': 'and'})
            continue

        cur.execute(sql.SEARCH_IS_MONOMER, (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'monomer', 'term': ret.name, 'operation': 'and'})
            continue

        parsed.append({'category': 'compound_seq', 'term': term, 'operation': 'and'})

    return parsed


def clusters_by_category(category, term):
    '''Get a set of gene clusters by category'''

    found_clusters = set()
    cur = get_db().cursor()

    try:
        sql_expression = get_sql_by_category_fuzzy(category)
        search = ("%{}%".format(term),)
    except AttributeError:
        try:
            sql_expression = get_sql_by_category(category)
            search = (term, )
        except AttributeError:
            try:
                sql_expression = get_sql_by_category_or_desc(category)
                search = (term, "%{}%".format(term))
            except AttributeError:
                return found_clusters

    cur.execute(sql_expression, search)
    clusters = cur.fetchall()
    for cluster in clusters:
        found_clusters.add(cluster.bgc_id)

    return found_clusters


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


def get_sql_by_category(category):
    '''Get the appropriate SQL expression'''
    attr = 'CLUSTER_BY_{}'.format(category.upper())
    return getattr(sql, attr)


def get_sql_by_category_fuzzy(category):
    '''Get the appropriate SQL expression in fuzzy mode'''
    attr = 'CLUSTER_BY_{}_FUZZY'.format(category.upper())
    return getattr(sql, attr)


def get_sql_by_category_or_desc(category):
    '''Get the appropriate SQL expression for category or description'''
    attr = 'CLUSTER_BY_{}_OR_DESCRIPTION'.format(category.upper())
    return getattr(sql, attr)


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
        return query.all()

    return []


@register_handler(AVAILABLE)
def available_superkingdom(term):
    '''Generate query for available superkingdoms'''
    return db.session.query(distinct(Taxa.superkingdom)).filter(Taxa.superkingdom.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_phylum(term):
    '''Generate query for available phyla'''
    return db.session.query(distinct(Taxa.phylum)).filter(Taxa.phylum.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_class(term):
    '''Generate query for available class'''
    return db.session.query(distinct(Taxa._class)).filter(Taxa._class.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_order(term):
    '''Generate query for available order'''
    return db.session.query(distinct(Taxa.taxonomic_order)).filter(Taxa.taxonomic_order.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_family(term):
    '''Generate query for available family'''
    return db.session.query(distinct(Taxa.family)).filter(Taxa.family.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_genus(term):
    '''Generate query for available genus'''
    return db.session.query(distinct(Taxa.genus)).filter(Taxa.genus.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_species(term):
    '''Generate query for available species'''
    return db.session.query(distinct(Taxa.species)).filter(Taxa.species.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_strain(term):
    '''Generate query for available strain'''
    return db.session.query(distinct(Taxa.strain)).filter(Taxa.strain.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_acc(term):
    '''Generate query for available accession'''
    return db.session.query(distinct(DnaSequence.acc)).filter(DnaSequence.acc.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_compound(term):
    '''Generate query for available compound by peptide sequence'''
    return db.session.query(distinct(Compound.peptide_sequence)).filter(Compound.peptide_sequence.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_monomer(term):
    '''Generate query for available monomer'''
    return db.session.query(distinct(Monomer.name)).filter(Monomer.name.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_type(term):
    '''Generate query for available type'''
    return db.session.query(distinct(BgcType.term)).filter(BgcType.term.ilike('{}%'.format(term)))
