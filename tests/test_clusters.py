'Tests for the cluster search logic'

from api.search_parser import QueryTerm
from api.search import (
    clusters,
    filters,
    modules,
)


def get_count(query):
    """ Queries can be constructed with a query.distinct(_field_), but may not be.
        .count() does not deduplicate in that case, but .all() *does* deduplicate.
        Since the tests aren't aware of the use of joins and distinct, this
        function exists to prevent that from rearing its head (again).
    """
    return len(query.all())


def test_guess_cluster_category():
    tests = [
        ('lanthipeptide', 'type'),
        ('NC_003888', 'acc'),
        ('Streptomyces', 'genus'),
        ('coelicolor', 'species'),
        ('not-in-database', 'unknown')
    ]

    for search_term, expected in tests:
        term = QueryTerm.from_string(search_term)
        assert clusters.guess_cluster_category(term) == expected, search_term


# using GCF_000203835.1 (S.Coelicolor) and GCF_000590515.1 (S.sp.PRh5)as test
# data, the taxid lookups change as NCBI changes them, which is frustrating, but
# to make this easier to handle in the counts by taxonomy tests below, these
# constants have been created to handle the split, before and after

TOTAL_REGION_COUNT = 133
SCO_REGION_COUNT = 28
OTHER_REGION_COUNT = TOTAL_REGION_COUNT - SCO_REGION_COUNT

SCO_TAXID_ID = 1  # not the actual TAXID, just the serial identifier that TAXID was given in the database
SCO_STRAIN = "A3(2)"

def test_clusters_by_taxid():
    assert get_count(clusters.clusters_by_taxid(SCO_TAXID_ID)) == SCO_REGION_COUNT


def test_clusters_by_strain():
    assert get_count(clusters.clusters_by_strain(SCO_STRAIN)) == SCO_REGION_COUNT


def test_clusters_by_species():
    assert get_count(clusters.clusters_by_species('coelicolor')) == SCO_REGION_COUNT


def test_clusters_by_genus():
    assert get_count(clusters.clusters_by_genus('streptomyces')) == TOTAL_REGION_COUNT


def test_clusters_by_family():
    assert get_count(clusters.clusters_by_family('streptomycetaceae')) == TOTAL_REGION_COUNT


def test_clusters_by_order():
    assert get_count(clusters.clusters_by_order('kitasatosporales')) == TOTAL_REGION_COUNT


def test_clusters_by_class():
    assert get_count(clusters.clusters_by_class('actinomycetes')) == TOTAL_REGION_COUNT


def test_clusters_by_phylum():
    assert get_count(clusters.clusters_by_phylum('actinomycetota')) == TOTAL_REGION_COUNT


def test_clusters_by_superkingdom():
    assert get_count(clusters.clusters_by_superkingdom('bacteria')) == TOTAL_REGION_COUNT


def test_clusters_by_monomer():
    assert get_count(clusters.clusters_by_substrate('ala')) == 2
    # and that shared substrate ends up as two monomers
    assert get_count(clusters.clusters_by_monomer('ala')) == 1
    assert get_count(clusters.clusters_by_monomer('d-ala')) == 1


def test_clusters_by_acc():
    assert get_count(clusters.clusters_by_acc('NC_003888')) == 26


def test_clusters_by_types_and_categories():
    type_count = get_count(clusters.clusters_by_type("nrps"))
    category_count = get_count(clusters.clusters_by_typecategory("nrps"))
    # the category must have at least as many hits as the subtype, probably more
    assert type_count < category_count
    # but the exact counts might shift with rule changes
    assert type_count == 23
    assert category_count == 30


def test_clusters_by_candidate():
    base_query = clusters.clusters_by_candidatekind("chemical hybrid")
    base = get_count(base_query)
    assert base == 6
    filtered = filters._filter_candidate_kind_by_type(base_query, name="bgctype", value="t1pks")
    found = get_count(filtered)
    assert 1 < found < base
    filtered = filters._filter_candidate_kind_by_type(filtered, name="bgctype", value="prodigiosin")
    assert 0 < get_count(filtered) < found


def base_test_clusters_by_clustercompare(query, target):
    matches = query.all()
    assert len(matches) == 1
    hit = matches[0]
    assert hit.accession == "NC_003888"
    assert hit.location == "[3524827:3603907]"

    # then with filters
    matching_cc_hit = [h for h in hit.cluster_compare_hits if h.reference_accession == target][0]
    for field in filters.CLUSTERCOMPARE_FIELDS:
        value = getattr(matching_cc_hit, field)
        # singly
        assert get_count(filters._filter_clustercompare_by_field(query, operator=">=", value=value * 0.99, field=field)) == 1
        assert get_count(filters._filter_clustercompare_by_field(query, operator="<", value=value * 0.5, field=field)) == 0
        # in combination
        filtered = filters._filter_clustercompare_by_field(query, operator="<", value=value * 1.01, field=field)
        filtered = filters._filter_clustercompare_by_field(filtered, operator=">", value=value * 0.99, field=field)
        assert get_count(filtered) == 1
        filtered = filters._filter_clustercompare_by_field(query, operator="<", value=0.0, field=field)
        assert get_count(filtered) == 0
    return True


def test_clusters_by_cluster_compare_protoclusters():
    target = "BGC0000315"
    query = clusters.clusters_by_clustercompareprotocluster(target)
    assert base_test_clusters_by_clustercompare(query, target)


def test_clusters_by_cluster_compare_regions():
    target = "BGC0000315"
    query = clusters.clusters_by_clustercompareregion(target)
    assert base_test_clusters_by_clustercompare(query, target)


def test_clusters_by_compoundseq():
    assert get_count(clusters.clusters_by_compoundseq('ASFGE')) == 1


def test_clusters_by_compoundclass():
    assert get_count(clusters.clusters_by_compoundclass('Class I')) == 3


def test_clusters_by_profile():
    assert get_count(clusters.clusters_by_profile('LANC_like')) == 5


def test_clusters_by_asdomain():
    assert get_count(clusters.clusters_by_asdomain('ACP')) == 36


def test_clusters_by_asdomainsubtype():
    assert get_count(clusters.clusters_by_asdomainsubtype("Trans-AT-KS")) == 3
    assert get_count(clusters.clusters_by_asdomainsubtype("Hybrid-KS")) == 13


def test_clusters_by_clusterblast():
    assert get_count(clusters.clusters_by_clusterblast('NC_003888')) == -1


def test_clusters_by_knowncluster():
    assert get_count(clusters.clusters_by_knowncluster('BGC0000660')) == 1


def test_clusters_by_subcluster():
    assert get_count(clusters.clusters_by_subcluster('AF386507')) == 3  # 1 if GCF_000590515.1 was minimal


def test_clusters_by_resfam():
    assert get_count(clusters.clusters_by_resfam("ClassB")) == 1  # SCO5091: 5,533,653-5,534,546


def test_clusters_by_pfam():
    assert get_count(clusters.clusters_by_pfam("Bac_rhamnosid_N")) == 1  # SCO0488: 507,394-510,810


def test_clusters_by_tigrfam():
    assert get_count(clusters.clusters_by_tigrfam("TIGR00552")) == 1  # SCO0506: 542,269-543,099


def test_clusters_by_modules():
    def count(query):
        return get_count(clusters.clusters_by_modulequery(query))

    counts = {
        "ACP": count("T=ACP"),
        "PCP": count("T=PCP"),
        "PP-binding": count("T=PP-binding"),
        "PKS_PP": count("T=PKS_PP"),
        "none": count("T=0"),
    }

    assert all(counts.values()), counts

    # joined sections
    assert count("S=PKS_KS|L=AMP-binding") == 0
    assert count("M=?|T=ACP") < min(count("M=?"), counts["ACP"])

    # counts won't be the same as pure addition when using unique rows, as some
    # hits contain both already
    # OR
    sub_counts = [counts["ACP"], counts["PCP"]]
    assert min(sub_counts) < count("T=PCP,ACP") < sum(sub_counts)
    # ANY
    sub_counts = [counts["ACP"], counts["PCP"], counts["PP-binding"], counts["PKS_PP"]]
    assert min(sub_counts) < count("T=?") < sum(sub_counts)
    # NONE OR
    sub_counts = [counts["PP-binding"], counts["none"]]
    assert min(sub_counts) < count("T=0,PP-binding") < sum(sub_counts)
    # OR NONE
    assert min(sub_counts) < count("T=PP-binding,0") < sum(sub_counts)
    # IGNORE
    try:
        count("T=*")
        assert False, "failed to raise error"
    except modules.InvalidQueryError as err:
        assert "no restrictions in query" in str(err)

    counts = {
        "DH": count("M=PKS_DH"),
        "ER": count("M=PKS_ER"),
        "KR": count("M=PKS_KR"),
    }

    assert all(counts.values()), counts

    # AND
    kr_and_er = count("M=PKS_KR+PKS_ER")
    assert kr_and_er < counts["KR"] + counts["ER"]
    dh_and_kr = count("M=PKS_KR+PKS_DH")
    assert dh_and_kr < counts["KR"] + counts["DH"]
    # commutativity of zero hits
    assert count("M=INVALID+PKS_KR") == 0
    assert count("M=PKS_KR+INVALID") == 0
    # AND AND
    assert count("M=PKS_DH+PKS_ER+PKS_KR") <= min(dh_and_kr, kr_and_er)
    # AND OR
    and_or = count("M=PKS_DH+PKS_KR,PKS_ER")
    assert and_or > max(dh_and_kr, counts["ER"])
    # OR AND
    assert count("M=PKS_ER,PKS_DH+PKS_KR") == and_or
    # AND OR AND
    assert count("M=PKS_DH+PKS_KR,PKS_ER+PKS_KR") >= max(dh_and_kr, kr_and_er)
    assert count("M=cMT") == 0  # if this is wrong, then the following will be wrong
    assert count("M=cMT+PKS_KR,PKS_ER+PKS_KR") == kr_and_er
    assert count("M=PKS_ER+PKS_KR,PKS_KR+CMT") == kr_and_er

    # THEN
    kr_then_er = count("M=PKS_KR>PKS_ER")
    er_then_kr = count("M=PKS_ER>PKS_KR")
    assert not kr_then_er  # if the data changes, this might break but it'll be interesting
    assert kr_then_er + er_then_kr == kr_and_er
    # THEN AND, AND THEN
    assert count("M=PKS_ER>PKS_KR+PKS_DH") == count("M=PKS_DH+PKS_ER>PKS_KR") > 0
    # THEN THEN
    assert count("M=PKS_DH>PKS_ER>PKS_KR") <= er_then_kr
    assert count("M=PKS_KR>PKS_ER>PKS_KR") == 0  # first then results in zero hits, and the second should restrict to that
    # THEN ANY
    assert count("M=PKS_ER>?") >= er_then_kr
    # ANY THEN
    assert count("M=?>PKS_KR") >= er_then_kr
    # THEN ANY THEN
    assert count("M=PKS_DH>?>PKS_KR") == count("M=PKS_DH>PKS_ER>PKS_KR")

    # THEN NONE  # TODO
    # NONE THEN  # TODO

    # COMPLETE   # TODO
    # MULTIMODULE # TODO

    # bad combos to reject
    combos = [
        "M:PKS_ER+0",  # AND     NONE
        "M:PKS_ER+?",  # AND     ANY
        "M:PKS_ER+*",  # AND     IGNORE
        "M:*+PKS_ER",  # IGNORE  AND
        "M:*>PKS_ER",  # IGNORE  THEN
        "M:*,PKS_ER",  # IGNORE  OR
        "M:?+PKS_ER",  # ANY     AND
        "M:?,PKS_ER",  # ANY     OR
        "M:0+PKS_ER",  # NONE    AND
        "M:PKS_ER,*",  # OR      IGNORE
        "M:PKS_ER,?",  # OR      ANY
        "M:PKS_ER>*",  # THEN    IGNORE
    ]
    for combo in combos:
        try:
            count(combo)
            assert False, "failed to raise error"
        except modules.InvalidQueryError as err:
            pass
        except Exception:
            print(combo)
            raise


def test_clusters_by_module_filters():
    assert get_count(clusters.clusters_by_substrate('ala')) == 2
    base = clusters.clusters_by_modulequery("L=AMP-binding")
    base_count = get_count(base)
    assert 1 < base_count
    with_substrate = filters._filter_module_by_substrate(base, value="leu")
    substrate_count = get_count(with_substrate)
    assert 1 < substrate_count < base_count
    with_monomer = filters._filter_module_by_monomer(with_substrate, value="d-leu")
    monomer_count = get_count(with_monomer)
    assert 1 <= monomer_count < substrate_count


def test_clusters_by_crosscds_module():
    # GCF_000590515.1, NZ_JABQ01000025:1-41,346, Z951_RS18340 and Z951_RS18345
    get_count(clusters.clusters_by_crosscdsmodule()) == 1


def test_clusters_by_tfbs():
    assert get_count(clusters.clusters_by_tfbs("ZuR")) == 41  # no quality requirement


def test_clusters_by_comparippsonmibig():
    # GCF_000203835.1, NC_003888.3:7,409,664 - 7,432,456, SCO6682
    # by accession
    assert get_count(clusters.clusters_by_comparippsonmibig("BGC0000553")) == 1
    # by compound
    assert get_count(clusters.clusters_by_comparippsonmibig("SRO15-2212")) == 1
    # by type
    assert get_count(clusters.clusters_by_comparippsonmibig("lanthipeptide")) == 2

    # with filters
    query = clusters.clusters_by_comparippsonmibig("BGC")
    assert get_count(query) == 2
    filtered = filters._filter_comparippson_numeric(query, "similarity", ">=", 0.3)
    assert get_count(filtered) == 1


def test_cluster_query_count():
    # only two entries of the above have a proline monomer prediction
    # NZ_JABQ01000048:0-79694 has four proline modules
    # NZ_JABQ01000073:100-54745 has a single proline module
    term = QueryTerm("expression", term="pro", category="monomer", count=1)
    assert get_count(clusters.cluster_query_from_term(term)) == 2
    term = QueryTerm("expression", term="pro", category="monomer", count=2)
    assert get_count(clusters.cluster_query_from_term(term)) == 1
