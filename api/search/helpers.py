'''general helper functions for search'''


def register_handler(handler):
    '''Decorator to register a function as a handler'''
    def real_decorator(function):
        name = function.__name__.split('_')[-1]
        handler[name] = function

        def inner(*args, **kwargs):
            return function(*args, **kwargs)
        return inner
    return real_decorator


def break_lines(string, width=80):
    '''Break up a long string to lines of width (default: 80)'''
    parts = []
    for w in range(0, len(string), width):
        parts.append(string[w:w + width])

    return '\n'.join(parts)


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


def calculate_sequence(strand, sequence):
    '''Calculate strand-aware sequence'''
    if strand == '-':
        sequence = reverse_completement(sequence)
    return sequence


TRANS_TABLE = str.maketrans('ATGCatgc', 'TACGtacg')


def reverse_completement(sequence):
    '''return the reverse complement of a sequence'''
    return str(sequence).translate(TRANS_TABLE)[::-1]
