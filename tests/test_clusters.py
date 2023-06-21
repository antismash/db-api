'Tests for the cluster search logic'

from api.search_parser import QueryTerm
from api.search import clusters
from api.search import modules


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


def test_clusters_by_acc():
    assert get_count(clusters.clusters_by_acc('NC_003888')) == 26


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


def test_clusters_by_modules():
    def count(query):
        return get_count(clusters.clusters_by_modulequery(query))

    counts = {
        "ACP": count("T=ACP"),
        "PCP": count("T=PCP"),
        "PP-binding": count("T=PP-binding"),
        "none": count("T=0"),
    }

    assert all(counts.values()), counts

    # joined sections
    assert count("S=PKS_KS|L=AMP-binding") == 0
    assert count("M=?|T=ACP") < min(count("M=?"), counts["ACP"])

    # OR
    assert count("T=PCP,ACP") == counts["ACP"] + counts["PCP"]
    # ANY
    assert count("T=?") == counts["ACP"] + counts["PCP"] + counts["PP-binding"]
    # NONE OR
    assert count("T=0,PP-binding") == counts["PP-binding"] + counts["none"]
    # OR NONE
    assert count("T=PP-binding,0") == counts["PP-binding"] + counts["none"]
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
    # AND AND
    assert count("M=PKS_DH+PKS_ER+PKS_KR") <= min(dh_and_kr, kr_and_er)
    # AND OR
    and_or = count("M=PKS_DH+PKS_KR,PKS_ER")
    assert and_or > max(dh_and_kr, counts["ER"])
    # OR AND
    assert count("M=PKS_ER,PKS_DH+PKS_KR") == and_or
    # AND OR AND
    assert count("M=PKS_DH+PKS_KR,PKS_ER+PKS_KR") >= max(dh_and_kr, kr_and_er)
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
