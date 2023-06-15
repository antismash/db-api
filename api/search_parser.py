'''Parser for search strings to Query data structure'''

import re
from typing import Any

from .search.helpers import (
    ensure_operator_valid,
    Filter,
    UnknownOperatorError,
)
from .search.filters import (
    NumericFilter,
    QualitativeFilter,
    TextFilter,
    available_filters_by_category,
)


def process_filter(data: dict[str, Any], category: str) -> list[tuple[Filter, dict[str, Any]]]:
    """ Finds matching Filter instances for the filter described by JSON
        Returns a tuple of the filter and the data, ready to be run when a query is present
    """
    available_filters = available_filters_by_category(category, as_json=False)
    if "name" not in data:
        raise ValueError(f"Missing filter type for category: '{category}'")
    matching = [available for available in available_filters if available.name == data["name"]]
    if len(matching) != 1:
        raise ValueError(f"Invalid filter '{data['name']}' for category: '{category}'")
    match = matching[0]
    return (match, data)


class Query(object):
    '''A query for the database'''
    def __init__(self, terms, search_type='cluster', return_type='json', verbose=False):
        '''Set up a query with terms, optionally giving a search_type and return_type'''
        self.terms = terms
        self._search_type = search_type.lower()
        self._return_type = return_type.lower()
        self._verbose = verbose

    @property
    def search_type(self):
        return self._search_type

    @property
    def return_type(self):
        return self._return_type

    @property
    def verbose(self):
        return self._verbose

    def __repr__(self):
        return 'Query(search_type: {!r}, return_type: {!r}, terms: \'{}\')'.format(
            self.search_type, self.return_type, self.terms)

    def __str__(self):
        return "Query(search: {search}, terms: {terms})".format(
            search=self.search_type, terms=str(self.terms))

    def to_json(self):
        """Get a serialisable version of the query."""
        return {
            "terms": self.terms.to_json(),
            "search": self.search_type,
            "return_type": self.return_type,
            "verbose": self.verbose,
        }

    @classmethod
    def from_json(cls, json_query):
        '''Generate query from a json structure'''
        if 'terms' not in json_query:
            raise ValueError('Invalid query')

        terms = QueryTerm.from_json(json_query['terms'])
        extra_args = {}
        if 'search' in json_query:
            extra_args['search_type'] = json_query['search']
        if 'return_type' in json_query:
            extra_args['return_type'] = json_query['return_type']
        if 'verbose' in json_query:
            extra_args['verbose'] = json_query['verbose']
        return cls(terms, **extra_args)

    @classmethod
    def from_string(cls, string, search_type='cluster', return_type='json', verbose=False):
        '''Generate query from a string'''
        terms = QueryTerm.from_string(string)
        return cls(terms, search_type=search_type, return_type=return_type, verbose=verbose)


def split_term_and_category(text: str, term_requires_parens: bool = False) -> tuple[str, str]:
    assert text.startswith("["), text
    end = text.find("]")
    if end < 0:
        raise ValueError(f"Unterminated category in expression: {text}")
    category = text[1:end]
    term = text[end + 1:]
    if term_requires_parens:
        assert term.startswith("(") and term.endswith(")")
    return category, term


class QueryTerm(object):
    '''A term in a Query'''
    KEYWORDS = set(['AND', 'OR', 'EXCEPT'])

    BOOL_CATEGORIES = set(['contigedge'])
    COUNT_GUARD = -1

    def __init__(self, kind, **kwargs):
        '''Initialize a term, either an operation or an expression

        Raise a ValueError if not all of the necessary kwargs are supplied.
        Operations need 'operation', 'left' and 'right'.
        Expressions need 'category' and 'term'.
        '''
        self.kind = kind
        self.count = int(kwargs.get("count", self.COUNT_GUARD))
        if kind == 'operation':
            if not set(['operation', 'left', 'right']).issubset(kwargs.keys()):
                raise ValueError("For operations, you need to specify 'operation', 'left' and 'right'")
            self.operation = kwargs['operation'].lower()
            self.left = kwargs['left']
            self.right = kwargs['right']

        elif kind == 'expression':
            if not set(['category', 'term']).issubset(kwargs.keys()):
                raise ValueError("For expressions, you need to specify 'category' and 'term'")
            self.category = kwargs['category']
            self.term = kwargs['term']
            self.filters = [process_filter(f, self.category) for f in kwargs.get("filters", [])]
            if self.category in self.BOOL_CATEGORIES and not isinstance(self.term, bool):
                self.term = self.term.casefold() in {'true', 'yes', 't', 'y', '1'}

        else:
            raise ValueError('Invalid term type {!r}'.format(kind))

    def __repr__(self):
        if self.kind == 'expression':
            return 'QueryTerm(category: {!r}, term: {!r})'.format(self.category, self.term)
        if self.kind == 'operation':
            return 'QueryTerm(operation: {!r},\n\tleft: {!r}\n\tright: {!r}\n)'.format(self.operation, self.left, self.right)


    def __str__(self):
        if self.kind == 'expression':
            filters = " WITH ".join([f"[{f['name']}]({(f['operator'] + ' ') if 'operator' in f else ''}{f['value']})" for _, f in self.filters])
            return f"[{self.category}]{self.term}{' WITH ' if filters else ''}{filters}"
        if self.kind == 'operation':
            return '( {l} {o} {r} )'.format(l=self.left, o=self.operation.upper(), r=self.right)

    def to_json(self):
        if self.kind == 'expression':
            return { 'term_type': 'expr', 'category': self.category, 'term': self.term }
        if self.kind == 'operation':
            return {
                'term_type': 'op',
                'operation': self.operation,
                'left': self.left.to_json(),
                'right': self.right.to_json(),
            }

    @classmethod
    def from_json(cls, term):
        '''Recursively generate terms from a json data structure'''
        if 'term_type' not in term:
            raise ValueError('Invalid term')

        if term['term_type'] == 'expr':
            if not set(['category', 'term']).issubset(term.keys()):
                raise ValueError("For expressions, you need to specify 'category' and 'term'")
            count = term.get("count", cls.COUNT_GUARD)
            return cls('expression', count=count, category=term['category'], term=term['term'], filters=term.get('filters', []))

        elif term['term_type'] == 'op':
            if not set(['operation', 'left', 'right']).issubset(term.keys()):
                raise ValueError("For operations, you need to specify 'operation', 'left' and 'right'")

            left = cls.from_json(term['left'])
            right = cls.from_json(term['right'])
            return cls('operation', operation=term['operation'], left=left, right=right)

        else:
            raise ValueError('Invalid term_type {!r}'.format(term['term_type']))

    @classmethod
    def from_string(cls, string):
        '''Gernerate terms from a string'''
        tokens = cls._generate_tokens(string)

        return cls.get_term(tokens)

    @classmethod
    def get_term(cls, token_list):
        '''Recursively create a QueryTerm tree from the token_list'''
        if len(token_list) < 2:
            raise ValueError('Unexpected end of expression')

        left = cls.get_expression(token_list)
        next_token = token_list[0]
        if next_token.upper() in cls.KEYWORDS:
            del token_list[0]
            right = cls.get_term(token_list)
            return cls('operation', operation=next_token.lower(), left=left, right=right)
        if next_token in ('END', ')'):
            return left
        right = cls.get_term(token_list)
        return cls('operation', operation='and', left=left, right=right)

    @classmethod
    def get_expression(cls, token_list):
        count = cls.COUNT_GUARD
        if len(token_list) >= 2 and token_list[0].isdigit() and token_list[1] == "*":
            count = int(token_list[0])
            del token_list[0]
            cls._get_token(token_list, "*")

        if cls._get_token(token_list, '('):
            term = cls.get_term(token_list)
            if not cls._get_token(token_list, ')'):
                raise ValueError('Invalid token {l[0]}'.format(l=token_list))
            return term

        raw_expr = token_list[0]
        if raw_expr in cls.KEYWORDS:
            raise ValueError('Invalid use of keyword {!r}'.format(raw_expr))
        if raw_expr == 'END':
            raise ValueError('Invalid use of keyword {!r}'.format(raw_expr))

        del token_list[0]

        category = 'unknown'
        term = raw_expr

        if raw_expr.startswith('['):
            category, term = split_term_and_category(raw_expr)

        filters = []
        while token_list[0] == "WITH":
            del token_list[0]
            filters.append(cls.parse_filter(category, token_list))

        return QueryTerm('expression', count=count, category=category, term=term, filters=filters)

    @staticmethod
    def parse_filter(parent_category: str, token_list: list[str]):
        if parent_category == "unknown":
            raise ValueError(f"Cannot use filters without defining category: {parent_category!r}")
        category, term = split_term_and_category(token_list[0])
        del token_list[0]
        value_parts = []
        if not token_list or token_list[0] != "(":
            raise ValueError("Malformed filter value for category: {parent_category!r}")
        del token_list[0]
        while token_list and token_list[0] != ")":
            value_parts.append(token_list[0])
            del token_list[0]
        if not token_list or not value_parts:
            raise ValueError("Malformed filter value for category: {parent_category!r}")
        assert token_list[0] == ")"
        assert value_parts
        del token_list[0]

        possible_filters = available_filters_by_category(parent_category, as_json=False)
        matching_filter = None
        for filt in possible_filters:
            if category == filt.name:
                matching_filter = filt
                break
        if not matching_filter:
            raise ValueError(f"Invalid filter category for {parent_category!r}: {category!r}")

        if isinstance(matching_filter, NumericFilter) or isinstance(matching_filter, QualitativeFilter):
            if len(value_parts) != 2:
                raise ValueError(f"Invalid numeric filter format: {' '.join(value_parts)}")
            try:
                ensure_operator_valid(value_parts[0])
            except UnknownOperatorError:
                raise ValueError(f"Invalid or missing comparison operator for {parent_category!r} in {category!r}")
            operator = value_parts[0]
            value = float(value_parts[1])
            # reduce to ints where equivalent, for simplicity in rebuilding text
            if value == int(value):
                value = int(value)
            return {
                "name": filt.name,
                "operator": operator,
                "value": value,
            }
        elif isinstance(matching_filter, TextFilter):
            value = " ".join(value_parts)
            return {
                "name": filt.name,
                "value": value,
            }
        else:
            raise NotImplementedError(f"unknown filter type: '{type(matching_filter)}'")

    @staticmethod
    def _generate_tokens(string):
        tokens = string.split()
        final_tokens = []
        for token in tokens:
            final_tokens.extend([t for t in re.split('(\(|\))', token) if t != ''])
        final_tokens.append('END')

        return final_tokens

    @staticmethod
    def _get_token(token_list, expected):
        token = token_list[0]
        if token == expected:
            del token_list[0]
            return True
        return False
