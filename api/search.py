'''Search-related functions'''
import re
from .helpers import get_db
import sql


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

    return '{species}\t{acc}.{version}\t{cluster_number}\t{term}\t{start_pos}\t{end_pos}\t{cbh_description}\t{similarity}\t{cbh_acc}'.format(**cluster)


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
    for result in collected_sets:
        final = final.intersection(result)
    bgc_list = list(final)
    bgc_list.sort()
    total = len(bgc_list)
    if paginate > 0:
        end = min(offset + paginate, total)
    else:
        end = total
    return total, map(mapfunc, bgc_list[offset:end])


def parse_search_string(search_string):
    '''Parse a search string

    Given a search string like "[fieldname1]searchterm1 [fieldname2]searchterm2", return the parsed representation.
    >>> parse_search_string("[type]lanthipeptide [genus]Streptomyces")
    [{'category': 'type', 'term': 'lanthipeptide'}, {'category': 'genus', 'term': 'Streptomyces'}]
    '''

    parsed = []
    pattern = r'\[(\w+)\](\w+)'
    for match in re.finditer(pattern, search_string):
        parsed.append({'category': match.group(1), 'term': match.group(2)})

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
            parsed.append({'category': 'type', 'term': ret.term})
            continue

        cur.execute(sql.SEARCH_IS_ACC, (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'acc', 'term': ret.acc})
            continue

        cur.execute(sql.SEARCH_IS_GENUS, (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'genus', 'term': ret.genus})
            continue

        cur.execute(sql.SEARCH_IS_SPECIES, ('% {}'.format(term), ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'species', 'term': ret.species})
            continue

        cur.execute(sql.SEARCH_IS_MONOMER, (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'monomer', 'term': ret.name})
            continue

        parsed.append({'category': 'compound_seq', 'term': term})

    return parsed


def clusters_by_category(category, term):
    '''Get a set of gene clusters by category'''

    found_clusters = set()
    cur = get_db().cursor()

    search_term = "%{}%".format(term)

    try:
        sql_expression = get_sql_by_category(category)
    except AttributeError:
        return found_clusters

    cur.execute(sql_expression, (search_term, ))
    clusters = cur.fetchall()
    for cluster in clusters:
        found_clusters.add(cluster.bgc_id)

    return found_clusters


def get_sql_by_category(category):
    '''Get the appropriate SQL expression'''
    attr = 'CLUSTER_BY_{}'.format(category.upper())
    return getattr(sql, attr)


WHITELIST = set()
for item in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
    WHITELIST.add(item)
    WHITELIST.add(item.lower())
for item in '0123456789':
    WHITELIST.add(item)
WHITELIST.add('_')
WHITELIST.add('-')


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
