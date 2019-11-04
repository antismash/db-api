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


def sanitise_string(search_string):
    '''Explicitly replace problematic characters, and leaves the rest of sanitisation
       to the driver
    '''
    # escape any literal undercores, since they're a single char wildcard
    cleaned = search_string.replace("_", "\\_")
    # statement terminator, it is escaped by the driver, but remove it just to be sure
    cleaned = cleaned.replace(";", "_")
    return cleaned


def calculate_sequence(location, sequence):
    '''Calculate strand-aware sequence'''
    result = []
    for part in location.parts:
        result.append(sequence[part.start:part.end])
    result = "".join(result)
    if location.strand == -1:
        result = reverse_complement(result)
    else:
        assert location.strand == 1
    assert result
    return result


TRANS_TABLE = str.maketrans('ATGCatgc', 'TACGtacg')


def reverse_complement(sequence):
    '''return the reverse complement of a sequence'''
    return str(sequence).translate(TRANS_TABLE)[::-1]
