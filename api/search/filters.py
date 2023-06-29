"""Available filters by category

This is used for the web UI in order to create additional filtering options
"""

from functools import partial

from sqlalchemy import func, or_

from api.models import (
    db,
    BgcType,
    Candidate,
    ClusterblastHit,
    ClusterCompareHit,
    ComparippsonHit,
    Module,
    Monomer,
    Protocluster,
    RegulatorConfidence,
    RelModulesMonomer,
    Substrate,
)
from api.search import available as available_endpoints

from .helpers import (
    COMPARISON_OPERATORS,
    Filter,
    BooleanFilter,
    NumericFilter,
    QualitativeFilter,
    TextFilter,
    sanitise_string,
)

CLUSTERCOMPARE_FIELDS = ["score", "identity_metric", "order_metric", "components_metric"]


def _filter_candidate_kind_by_type(query, value: str = None):
    assert value is not None
    condition = or_(BgcType.term == value, BgcType.description.ilike(f'%{value}%'))
    subquery = db.session.query(Candidate.candidate_id).join(Candidate.protoclusters).join(BgcType).filter(condition)
    query = query.filter(Candidate.candidate_id.in_(subquery))
    return query


def _filter_candidate_kind_by_count(query, operator: str = None, value: float = None):
    assert operator in COMPARISON_OPERATORS
    assert value is not None

    condition = eval(f"func.count(Candidate.candidate_id) {operator} {float(value)}")
    subquery = db.session.query(Candidate.candidate_id).join(Candidate.protoclusters).group_by(Candidate.candidate_id).having(condition).subquery()
    return query.filter(Candidate.candidate_id.in_(subquery))


def _filter_clusterblast(query, operator: str = None, value: float = None, field: str = None):
    assert field and operator in COMPARISON_OPERATORS and value is not None
    return query.filter(eval(f"ClusterblastHit.{field} {operator} {float(value)}"))


def _filter_clustercompare_by_field(query, operator: str = None, value: float = None, field: str = None):
    assert field in CLUSTERCOMPARE_FIELDS, field
    assert operator in COMPARISON_OPERATORS
    assert value is not None
    condition = eval(f"ClusterCompareHit.{field} {operator} {float(value)}")
    return query.filter(condition)


def _filter_comparippson_numeric(query, operator: str = None, value: float = None, field: str = None):
    assert field and operator in COMPARISON_OPERATORS and value is not None
    return query.filter(eval(f"ComparippsonHit.{field} {operator} {float(value)}"))


def _filter_module_by_monomer(query, value: str = None):
    assert value is not None
    condition = or_(Monomer.name == value.lower(), Monomer.description.ilike(f'%{value}%'))
    subquery = db.session.query(RelModulesMonomer.module_id).join(Monomer).filter(condition).subquery()
    return query.filter(Module.module_id.in_(subquery))


def _filter_module_by_multigene(query):
    return query.filter(Module.multi_gene == True)


def _filter_module_by_substrate(query, value: str = None):
    assert value is not None
    condition = or_(Substrate.name == value.lower(), Substrate.description.ilike(f'%{value}%'))
    subquery = db.session.query(RelModulesMonomer.module_id).join(Substrate).filter(condition).subquery()
    return query.filter(Module.module_id.in_(subquery))


def _filter_tfbs_by_field(query, operator: str = None, value: float = None, field: str = None):
    assert operator in COMPARISON_OPERATORS
    assert value is not None
    assert field
    return query.join(RegulatorConfidence).filter(eval(f"RegulatorConfidence.{field} {operator} {float(value)}"))


_CLUSTERBLAST_FILTERS = {
    "similarity": NumericFilter("Similarity", partial(_filter_clusterblast, field="similarity")),
}
_CLUSTER_COMPARE_FILTERS = {
    field: NumericFilter(field.title(), partial(_filter_clustercompare_by_field, field=field))
    for field in CLUSTERCOMPARE_FIELDS
}
_COMPARIPPSON_SIMILARITY = NumericFilter("Similarity", partial(_filter_comparippson_numeric, field="similarity"))

# these keys must match search categories
AVAILABLE_FILTERS: dict[str, dict[str, Filter]] = {
    "candidatekind": {
        "bgctype": TextFilter("BGC Type", _filter_candidate_kind_by_type, available_endpoints.available_type),
        "numprotoclusters": NumericFilter("Protocluster count", _filter_candidate_kind_by_count),
    },
    "clusterblast": _CLUSTERBLAST_FILTERS,
    "clustercompareprotocluster": _CLUSTER_COMPARE_FILTERS,
    "clustercompareregion": _CLUSTER_COMPARE_FILTERS,
    "comparippsonmibig": {
        "similarity": _COMPARIPPSON_SIMILARITY,
    },
    "comparippsonasdb": {
        "similarity": _COMPARIPPSON_SIMILARITY,
    },
    "modulequery": {
        "substrate": TextFilter("Substrate", _filter_module_by_substrate, available_endpoints.available_substrate),
        "monomer": TextFilter("Monomer", _filter_module_by_monomer, available_endpoints.available_monomer),
        "multigene": BooleanFilter("Multi-gene", _filter_module_by_multigene),
    },
    "tfbs": {
        "quality": QualitativeFilter("Quality", partial(_filter_tfbs_by_field, field="strength"), {"strong": 30, "medium": 20, "weak": 10}),
        "score": NumericFilter("Score", partial(_filter_tfbs_by_field, field="score")),
    },
    "knowncluster": _CLUSTERBLAST_FILTERS,
    "subcluster": _CLUSTERBLAST_FILTERS,
}


def available_filters_by_category(category: str, name: str = None, as_json=True) -> dict:
    """ List all available filters by category
        If name is supplied as well as category, then only that filter will be returned.
    """
    cleaned_category = sanitise_string(category)
    filters = AVAILABLE_FILTERS.get(cleaned_category, {})
    if name is not None:
        if as_json:
            return filters[name].get_options(name)
        return filters[name]
    if as_json:
        return [f.get_options(name) for name, f in filters.items()]
    return dict(filters)
