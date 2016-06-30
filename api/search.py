'''Search-related functions'''
import re
from .helpers import get_db
import sql


def search_bgcs(search_string):
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
    return list(final)


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
    cleaned_terms = [_sanitise_string(t) for t in search_string.split()]

    for term in cleaned_terms:
        cur.execute("SELECT term FROM antismash.bgc_types WHERE lower(term) = lower(%s)", (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'type', 'term': ret.term})
            continue

        cur.execute("SELECT acc FROM antismash.dna_sequences WHERE lower(acc) = lower(%s)", (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'acc', 'term': ret.acc})
            continue

        cur.execute("SELECT genus FROM antismash.taxa WHERE lower(genus) = lower(%s)", (term, ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'genus', 'term': ret.genus})
            continue


        cur.execute("SELECT species FROM antismash.taxa WHERE lower(species) LIKE lower(%s)", ('% {}'.format(term), ))
        ret = cur.fetchone()
        if ret is not None:
            parsed.append({'category': 'species', 'term': ret.species})
            continue

        cur.execute("SELECT name FROM antismash.monomers WHERE lower(name) = lower(%s)", (term, ))
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
        sql_expression = _get_sql_by_category(category)
    except AttributeError:
        return found_clusters

    cur.execute(sql_expression, (search_term, ))
    clusters = cur.fetchall()
    for cluster in clusters:
        found_clusters.add(cluster.bgc_id)

    return found_clusters


def _get_sql_by_category(category):
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


def _sanitise_string(search_string):
    '''Remove all non-whitelisted characters from the search string

    >>> _sanitise_string('fOo')
    'fOo'
    >>> _sanitise_string('%bar')
    'bar'

    '''
    cleaned = []
    for symbol in search_string:
        if symbol in WHITELIST:
            cleaned.append(symbol)

    return ''.join(cleaned)
