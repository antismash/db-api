'''Parser for search strings to Query data structure'''

from abc import abstractmethod
import re
from typing import Any, Protocol

from .search.helpers import (
    ensure_operator_valid,
    Filter,
    UnknownOperatorError,
)
from .search.filters import (
    BooleanFilter,
    Filter,
    NumericFilter,
    QualitativeFilter,
    TextFilter,
    available_filters_by_category,
)


COUNT_GUARD = -1

class QueryItem:
    def __init__(self, kind: str):
        self.kind = kind


class QueryComponent(Protocol):
    kind: str

    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        raise NotImplementedError


class QueryFilter(QueryItem):
    def __init__(self, category: str, name: str, runner: Filter, value: str = None, operator: str = None):
        if isinstance(runner, NumericFilter):
            if value is None or operator is None:
                raise ValueError("Numeric filters require both an operator and a value")
            if isinstance(value, str) and "." in value:
                value = float(value)
            elif isinstance(value, str):
                value = int(value)
            ensure_operator_valid(operator)
        elif isinstance(runner, TextFilter):
            if value is None:
                raise ValueError("Text filters require a value")
            elif operator:
                raise ValueError("Text filters cannot have an operator")
        elif isinstance(runner, BooleanFilter):
            if value is not None or operator:
                raise ValueError("Boolean filters cannot have an operator or a value")

        super().__init__("filter")
        self.category = category
        assert isinstance(name, str), name
        self.name = name
        self.value = value
        self.operator = operator
        self.runner = runner

    def get_options(self) -> dict[str, Any]:
        result = self.to_json()
        return result

    def to_json(self) -> dict[str, Any]:
        result = {
            "name": self.name,
        }
        if self.value is not None:
            result["value"] = self.value
        if self.operator is not None:
            result["operator"] = self.operator
        return result

    @classmethod
    def from_json(cls, category: str, data: dict[str, Any]) -> "QueryFilter":
        name = data["name"]
        runner = available_filters_by_category(category, name=name, as_json=False)
        return cls(category, name, runner, data.get("value"), data.get("operator"))

    def __str__(self) -> str:
        chunks = [self.name]
        if self.operator:
            chunks.append(f"{self.operator}:{self.value}")
        elif self.value is not None:
            chunks.append(str(self.value))
        return f" WITH [{'|'.join(chunks)}]"


class QueryOperand(QueryItem):
    BOOL_CATEGORIES = set(['contigedge'])

    def __init__(self, category: str, value: str = "", count: int = None, filters: list[QueryFilter] = None):
        super().__init__("expression")
        self.category = category
        self.term = value
        self.filters = filters or []
        self.count = count or -1
        assert self.count is not None

    def to_json(self) -> dict[str, Any]:
        result = {
            "termType": "expr",
            "category": self.category,
            "value": self.term,
        }
        if self.count > 1:
            result["count"] = self.count
        if self.filters:
            result["filters"] = [f.to_json() for f in self.filters]
        return result

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "QueryOperand":
        if not data:
            raise ValueError("Query elements cannot be empty")
        if not set(["category", "value"]).issubset(data.keys()):
            raise ValueError("For expressions, you need to specify 'category' and 'value'")
        filters = [QueryFilter.from_json(data["category"], f) for f in data.get("filters", [])]
        return cls(data["category"], value=data["value"], count=data.get("count"), filters=filters)

    def __str__(self) -> str:
        chunks = []
        if self.count > 1:
            chunks.extend([str(self.count), "*"])
        chunks.append("{")
        chunks.append(f"[{self.category}|{self.term}]")
        chunks.extend(str(f) for f in self.filters)
        chunks.append("}")
        return "".join(chunks)


class QueryOperation(QueryItem):
    OPERATORS = {"OR", "AND", "EXCEPT"}
    def __init__(self, operator: str, left: QueryComponent, right: QueryComponent):
        super().__init__("operation")
        if operator.upper() not in self.OPERATORS:
            raise ValueError(f"Unknown operator: {operator}")
        self.operator = operator.lower()
        self.left = left
        self.right = right

    def to_json(self) -> dict[str, Any]:
        return {
            "termType": "op",
            "operation": self.operator,
            "left": self.left.to_json(),
            "right": self.right.to_json(),
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "QueryOperation":
        try:
            return cls(data["operation"], QueryTerm.from_json(data["left"]), QueryTerm.from_json(data["right"]))
        except KeyError as err:
            raise ValueError(str(err))  # for the API to return things


class QueryParser:
    SPLITTER = "(\\" + "|\\".join("*{}[]()|") + ")"

    def __init__(self, text: str):
        self._index = 0
        self._tokens = self._tokenise(text)
        self.root: QueryItem = self._parse_operator() if self._peek() == "(" else self._parse_operand()
        if self._index + 1 < len(self._tokens):
            raise ValueError("Invalid query format")

    def _tokenise(self, text: str) -> list[str]:
        simple = text.split()
        tokens = []
        for token in simple:
            tokens.extend(re.split(self.SPLITTER, token))
        return [token for token in tokens if token]

    def _next(self) -> str:
        token = self._peek()
        while not token:
            self._index += 1
            token = self._peek()
        self._index += 1
        return token

    def _expect(self, expected: str) -> None:
        found = self._next()
        if found != expected:
            raise ValueError(f"Expected '{expected}', found '{found}'")

    def _peek(self) -> str:
        if self._index >= len(self._tokens):
            raise ValueError("Incomplete or badly formatted query")
        return self._tokens[self._index]

    def _parse_operator(self) -> QueryOperation:
        self._expect("(")
        if self._peek() == "(":
            left = self._parse_operator()
        else:
            left = self._parse_operand()
        operator = self._next()
        if self._peek() == "(":
            right = self._parse_operator()
        else:
            right = self._parse_operand()
        self._expect(")")
        return QueryOperation(operator, left, right)

    def _get_category_components(self, start="[", end="]", separator="|") -> list[str]:
        self._expect(start)
        if self._peek().startswith(separator):
            raise ValueError("Missing category name from query section")
        base = []
        while self._peek() != end:
            base.append(self._next())
        self._expect(end)
        # single spaces may be meaningful here, so rebuild them
        return [item.strip() for item in " ".join(base).split(separator) if item.strip()]

    def _parse_operand(self) -> QueryOperand:
        count = COUNT_GUARD
        if self._peek().isdigit():
            count = int(self._next())
            self._expect("*")
        self._expect("{")
        query = self._get_category_components()
        category = query[0]
        if category in QueryOperand.BOOL_CATEGORIES:
            assert len(query) == 1  # possibly values in {'true', 'yes', 't', 'y', '1'}, from old versions
        filters = []
        while self._peek() != "}":
            self._expect("WITH")
            filters.append(self._parse_filter(category))
        self._expect("}")
        return QueryOperand(*query, count=count, filters=filters)

    def _parse_filter(self, category: str) -> QueryFilter:
        components = self._get_category_components()
        filter_name = components[0]
        if not filter_name:
            raise ValueError("Filter names are required")
        matching_filter = available_filters_by_category(category, name=filter_name, as_json=False)
        if matching_filter is None:
            raise ValueError(f"Invalid filter '{filter_name}' for category: '{category}'")
        if isinstance(matching_filter, TextFilter):
            if len(components) != 2:
                raise ValueError("Missing value for filter")
            return QueryFilter(category, filter_name, matching_filter, value=components[1])
        if isinstance(matching_filter, NumericFilter):
            if len(components) != 2:
                raise ValueError("Missing operation and/or value for filter")
            op, value = components[-1].split(":")
            return QueryFilter(category, filter_name, matching_filter, operator=op, value=value)
        if isinstance(matching_filter, BooleanFilter):
            assert len(components) == 1
            return QueryFilter(category, filter_name, matching_filter)
        raise NotImplementedError(f"Unknown filter type: '{type(matching_filter)}'")


def process_filter(data: dict[str, Any], category: str) -> list[tuple[Filter, dict[str, Any]]]:
    """ Finds matching Filter instances for the filter described by JSON
        Returns a tuple of the filter and the data, ready to be run when a query is present
    """
    if "name" not in data:
        raise ValueError(f"Missing filter type for category: '{category}'")
    match = available_filters_by_category(category, as_json=False).get(data["name"])
    if match is None:
        raise ValueError(f"Invalid filter '{data['name']}' for category: '{category}'")
    return (match, data)


class Query(object):
    '''A query for the database'''
    def __init__(self, terms, search_type='cluster', return_type='json', verbose=False):
        '''Set up a query with terms, optionally giving a search_type and return_type'''
        self.terms = terms
        self._search_type = search_type.lower()
        if self._search_type == "region":
            self._search_type = "cluster"
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


def split_term_and_category(text: str) -> tuple[str, str]:
    assert text.startswith("["), text
    end = text.find("]")
    if end < 0:
        raise ValueError(f"Unterminated category in expression: {text}")
    category = text[1:end]
    term = text[end + 1:]
    return category, [chunk for chunk in term.split("|") if chunk]


class QueryTerm(object):
    '''A term in a Query'''

    def __init__(self, query_object: QueryItem):
        '''Initialize a term, either an operation or an expression

        Raise a ValueError if not all of the necessary kwargs are supplied.
        Operations need 'operation', 'left' and 'right'.
        Expressions need 'category' and 'term'.
        '''
        if not isinstance(query_object, QueryItem):
            raise ValueError(f"bad query object: {query_object!r}")
        self._object = query_object

    @property
    def term(self) -> QueryItem:
        return self._object

    @property
    def kind(self) -> str:
        return self._object.kind

    @property
    def count(self) -> str:
        assert isinstance(self._object, QueryOperand)
        return self._object.count

    def __str__(self):
        return str(self._object)

    def to_json(self):
        return self._object.to_json()

    @classmethod
    def from_json(cls, term):
        '''Recursively generate terms from a json data structure'''
        term["termType"] = term.pop("term_type", term.get("termType"))
        if 'termType' not in term:
            raise ValueError('Invalid term')
        try:
            if term['termType'] == 'expr':
                return QueryOperand.from_json(term)
            return QueryOperation.from_json(term)
        except KeyError as err:
            raise ValueError(str(err))  # for the API to return correctly

    @classmethod
    def from_string(cls, string):
        '''Generate terms from a string'''
        return QueryParser(string).root

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
        matching_filter = possible_filters.get(category)
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
                "name": category,
                "operator": operator,
                "value": value,
            }
        elif isinstance(matching_filter, TextFilter):
            value = " ".join(value_parts)
            return {
                "name": category,
                "value": value,
            }
        else:
            raise NotImplementedError(f"unknown filter type: '{type(matching_filter)}'")
