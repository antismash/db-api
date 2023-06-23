'''general helper functions for search'''
from enum import auto, Enum, unique
from typing import Any, Callable, Optional


class UnknownQueryError(Exception):
    pass


class InvalidQueryError(Exception):
    pass


class UnknownOperatorError(ValueError):
    def __str__(self):
        return f"unknown operator: '{super().__str__()}'"


COMPARISON_OPERATORS = {"<", "<=", "==", ">=", ">"}


def ensure_operator_valid(operator: str) -> bool:
    if operator not in COMPARISON_OPERATORS:
        raise UnknownOperatorError(operator)


@unique
class DataType(Enum):
    BOOLEAN = auto()
    NUMERIC = auto()
    TEXT = auto()

    def __str__(self) -> str:
        return str(self.name).lower()


class Filter:
    def __init__(self, name: str, data_type: DataType, func: Callable, labels: Optional[dict[str, float]] = None) -> None:
        self.name = name
        self.data_type = data_type
        self.labels = labels
        if self.labels:
            assert self.data_type is DataType.NUMERIC
        self.func = func

    def get_options(self, value: str) -> dict[str, Any]:
        options = {
            "label": self.name,
            "type": str(self.data_type),
            "value": value,
        }
        if self.labels:
            options["choices"] = dict(self.labels)
        return options

    def run(self, query, data: dict[str, Any]):
        raise NotImplementedError()


class BooleanFilter(Filter):
    def __init__(self, name: str, func: Callable, labels: Optional[dict[str, float]] = None):
        super().__init__(name, DataType.BOOLEAN, func, labels)

    def run(self, query, data: dict[str, Any]):
        if list(data) != ["name"]:
            raise ValueError("badly formed boolean filter")
        assert data["name"] == self.name
        return self.func(query)


class NumericFilter(Filter):
    def __init__(self, name: str, func: Callable, labels: Optional[dict[str, float]] = None):
        super().__init__(name, DataType.NUMERIC, func, labels)

    def run(self, query, data: dict[str, Any]):
        if list(data) != ["name", "operator", "value"]:
            raise ValueError("badly formed numeric filter")
        assert data["name"] == self.name
        ensure_operator_valid(data["operator"])
        value = float(data["value"])
        return self.func(query, operator=data["operator"], value=value)


class QualitativeFilter(NumericFilter):
    def __init__(self, name: str, func: Callable, labels: dict[str, float]):
        super().__init__(name, func, labels)


class TextFilter(Filter):
    def __init__(self, name: str, func: Callable, available: Callable[[str], Any]):
        super().__init__(name, DataType.TEXT, func)
        self._available = available

    def run(self, query, data: dict[str, Any]):
        if list(data) != ["name", "value"]:
            raise ValueError("badly formed text filter")
        assert data["name"] == self.name
        return self.func(query, value=sanitise_string(data["value"]))

    def available(self, search_string: str):
        return self._available(sanitise_string(search_string))

    def available(self, search_string: str):
        return self._available(sanitise_string(search_string))


class Handler:
    def __init__(self, core: Callable[[str], Any], countable: bool = False,
                 counter: Callable[[Any, int], Any] = None):
        self._core = core
        self._counter = counter
        self.countable = countable
        if self.countable and not self._counter:
            raise ValueError("A countable handler must also supply a counter method")

    def add_count_restriction(self, query, minimum: int):
        if not self.countable or minimum < 0:
            return query
        return self._counter(query, minimum)

    def __call__(self, term: str = None):  # for backwards compatible behaviour
        # term will be None if it's a boolean presence query
        if term is None:
            return self._core()
        return self._core(term)


def register_handler(handler, countable: bool = False, counter: Callable = None):
    '''Decorator to register a function as a handler'''
    def real_decorator(function):
        name = function.__name__.split('_')[-1]
        handler[name] = Handler(function, countable, counter)

        def inner(*args, **kwargs):
            return handler[name](*args, **kwargs)
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
