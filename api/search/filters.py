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
    ComparippsonHit,
    Module,
    Monomer,
    Protocluster,
    RegulatorConfidence,
    RelModulesMonomer,
    Substrate,
)

from .helpers import (
    COMPARISON_OPERATORS,
    Filter,
    NumericFilter,
    QualitativeFilter,
    TextFilter,
    sanitise_string,
)


def _filter_candidate_kind_by_type(query, name: str = None, value: str = None):
    assert name == "bgctype"
    assert value is not None
    condition = or_(BgcType.term == value, BgcType.description.ilike(f'%{value}%'))
    subquery = db.session.query(Candidate.candidate_id).join(Candidate.protoclusters).join(BgcType).filter(condition)
    query = query.filter(Candidate.candidate_id.in_(subquery))
    return query


def _filter_candidate_kind_by_count(query, name: str = None, operator: str = None, value: float = None):
    assert name == "numprotoclusters"
    assert operator in COMPARISON_OPERATORS
    assert value is not None

    condition = eval(f"func.count(Candidate.candidate_id) {operator} {float(value)}")
    subquery = db.session.query(Candidate.candidate_id).join(Candidate.protoclusters).group_by(Candidate.candidate_id).having(condition).subquery()
    return query.filter(Candidate.candidate_id.in_(subquery))


def _filter_clusterblast(query, name: str = None, operator: str = None, value: float = None):
    assert name and operator in COMPARISON_OPERATORS and value is not None
    return query.filter(eval(f"ClusterblastHit.{name} {operator} {float(value)}"))


def _filter_comparippson_numeric(query, name: str = None, operator: str = None, value: float = None):
    assert name and operator in COMPARISON_OPERATORS and value is not None
    return query.filter(eval(f"ComparippsonHit.{name} {operator} {float(value)}"))


def _filter_module_by_monomer(query, name: str = None, value: str = None):
    assert value is not None
    condition = or_(Monomer.name == value.lower(), Monomer.description.ilike(f'%{value}%'))
    subquery = db.session.query(RelModulesMonomer.module_id).join(Monomer).filter(condition).subquery()
    return query.filter(Module.module_id.in_(subquery))


def _filter_module_by_substrate(query, name: str = None, value: str = None):
    assert value is not None
    condition = or_(Substrate.name == value.lower(), Substrate.description.ilike(f'%{value}%'))
    subquery = db.session.query(RelModulesMonomer.module_id).join(Substrate).filter(condition).subquery()
    return query.filter(Module.module_id.in_(subquery))


def _filter_tfbs_quality(query, name: str = None, operator: str = None, value: float = None):
    assert name == "quality"
    assert operator in COMPARISON_OPERATORS
    assert value is not None
    return query.join(RegulatorConfidence).filter(eval(f"RegulatorConfidence.strength {operator} {float(value)}"))


# these keys must match search categories
AVAILABLE_FILTERS: dict[str, Filter] = {
    "candidatekind": [
        TextFilter("bgctype", _filter_candidate_kind_by_type),
        NumericFilter("numprotoclusters", _filter_candidate_kind_by_count),
    ],
    "comparippsonmibig": [
        NumericFilter("similarity", partial(_filter_comparippson_numeric, name="similarity")),
    ],
    "comparippsonasdb": [
        NumericFilter("similarity", partial(_filter_comparippson_numeric, name="similarity")),
    ],
    "modulequery": [
        TextFilter("substrate", _filter_module_by_substrate),
        TextFilter("monomer", _filter_module_by_monomer),
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
