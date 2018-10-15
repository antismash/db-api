'''Parser for search strings to Query data structure'''

import re


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


class QueryTerm(object):
    '''A term in a Query'''
    KEYWORDS = set(['AND', 'OR', 'EXCEPT'])

    def __init__(self, kind, **kwargs):
        '''Initialize a term, either an operation or an expression

        Raise a ValueError if not all of the necessary kwargs are supplied.
        Operations need 'operation', 'left' and 'right'.
        Expressions need 'category' and 'term'.
        '''
        self.kind = kind
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

        else:
            raise ValueError('Invalid term type {!r}'.format(kind))

    def __repr__(self):
        if self.kind == 'expression':
            return 'QueryTerm(category: {!r}, term: {!r})'.format(self.category, self.term)
        if self.kind == 'operation':
            return 'QueryTerm(operation: {!r},\n\tleft: {!r}\n\tright: {!r}\n)'.format(self.operation, self.left, self.right)


    def __str__(self):
        if self.kind == 'expression':
            return '[{s.category}]{s.term}'.format(s=self)
        if self.kind == 'operation':
            return '( {l} {o} {r} )'.format(l=self.left, o=self.operation.upper(), r=self.right)

    @classmethod
    def from_json(cls, term):
        '''Recursively generate terms from a json data structure'''
        if 'term_type' not in term:
            raise ValueError('Invalid term')

        if term['term_type'] == 'expr':
            if not set(['category', 'term']).issubset(term.keys()):
                raise ValueError("For expressions, you need to specify 'category' and 'term'")

            return cls('expression', category=term['category'], term=term['term'])

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
        if next_token in cls.KEYWORDS:
            del token_list[0]
            right = cls.get_term(token_list)
            return cls('operation', operation=next_token.lower(), left=left, right=right)
        if next_token in ('END', ')'):
            return left
        right = cls.get_term(token_list)
        return cls('operation', operation='and', left=left, right=right)

    @classmethod
    def get_expression(cls, token_list):
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
            end = raw_expr.find(']')
            if end > -1:
                category = raw_expr[1:end]
                term = raw_expr[end + 1:]
        return QueryTerm('expression', category=category, term=term)

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
