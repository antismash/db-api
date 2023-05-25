"""Available filters by category

This is used for the web UI in order to create additional filtering options
"""

from functools import partial

from api.models import (
    ClusterblastHit,
    ComparippsonHit,
    RegulatorConfidence,
)

from .helpers import (
    COMPARISON_OPERATORS,
    Filter,
    NumericFilter,
    QualitativeFilter,
    TextFilter,
    sanitise_string,
)


def _filter_clusterblast(query, name: str = None, operator: str = None, value: float = None):
    assert name and operator in COMPARISON_OPERATORS and value is not None
    return query.filter(eval(f"ClusterblastHit.{name} {operator} {float(value)}"))


def _filter_comparippson_numeric(query, name: str = None, operator: str = None, value: float = None):
    assert name and operator in COMPARISON_OPERATORS and value is not None
    return query.filter(eval(f"ComparippsonHit.{name} {operator} {float(value)}"))


def _filter_tfbs_quality(query, name: str = None, operator: str = None, value: float = None):
    assert name == "quality"
    assert operator in COMPARISON_OPERATORS
    assert value is not None
    return query.join(RegulatorConfidence).filter(eval(f"RegulatorConfidence.strength {operator} {float(value)}"))


# these keys must match search categories
AVAILABLE_FILTERS: dict[str, Filter] = {
    "comparippsonmibig": [
        NumericFilter("similarity", partial(_filter_comparippson_numeric, name="similarity")),
    ],
    "comparippsonasdb": [
        NumericFilter("similarity", partial(_filter_comparippson_numeric, name="similarity")),
    ],
    "tfbs": [
        QualitativeFilter("quality", _filter_tfbs_quality, {"strong": 30, "medium": 20, "weak": 10}),
        NumericFilter("score", _filter_tfbs_quality),
    ],
    "knowncluster": [
        NumericFilter("similarity", partial(_filter_clusterblast, name="similarity")),
    ],
}


def available_filters_by_category(category, as_json=True):
    """List all available filters by category"""
    cleaned_category = sanitise_string(category)

    if cleaned_category in AVAILABLE_FILTERS:
        return [f.get_options() if as_json else f for f in AVAILABLE_FILTERS[cleaned_category]]

    return []
