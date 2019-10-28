'Tests for the cluster search logic'

from api.search_parser import QueryTerm
from api.search import clusters


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


SCO_CLUSTER_COUNT = 29
STREPTO_CLUSTER_COUNT = 122


def test_clusters_by_taxid():
    assert clusters.clusters_by_taxid(100226).count() == SCO_CLUSTER_COUNT


def test_clusters_by_strain():
    assert clusters.clusters_by_strain('A3(2)').count() == SCO_CLUSTER_COUNT


def test_clusters_by_species():
    assert clusters.clusters_by_species('coelicolor').count() == SCO_CLUSTER_COUNT


def test_clusters_by_genus():
    assert clusters.clusters_by_genus('streptomyces').count() == STREPTO_CLUSTER_COUNT


def test_clusters_by_family():
    assert clusters.clusters_by_family('streptomycetaceae').count() == STREPTO_CLUSTER_COUNT


def test_clusters_by_order():
    assert clusters.clusters_by_order('streptomycetales').count() == STREPTO_CLUSTER_COUNT


def test_clusters_by_class():
    assert clusters.clusters_by_class('actinobacteria').count() == STREPTO_CLUSTER_COUNT


def test_clusters_by_phylum():
    assert clusters.clusters_by_phylum('actinobacteria').count() == STREPTO_CLUSTER_COUNT


def test_clusters_by_superkingdom():
    assert clusters.clusters_by_superkingdom('bacteria').count() == STREPTO_CLUSTER_COUNT


def test_clusters_by_monomer():
    assert clusters.clusters_by_substrate('ala').count() == 2


def test_clusters_by_acc():
    assert clusters.clusters_by_acc('NC_003888').count() == 27


def test_clusters_by_compoundseq():
    assert clusters.clusters_by_compoundseq('ASFGE').count() == 1


def test_clusters_by_compoundclass():
    assert clusters.clusters_by_compoundclass('Class I').count() == 4


def test_clusters_by_profile():
    assert clusters.clusters_by_profile('LANC_like').count() == 5


def test_clusters_by_asdomain():
    assert clusters.clusters_by_asdomain('ACP').count() == 104


def test_clusters_by_clusterblast():
    assert clusters.clusters_by_clusterblast('NC_003888_c3').count() == 1


def test_clusters_by_knowncluster():
    assert clusters.clusters_by_knowncluster('BGC0000660').count() == 1


def test_clusters_by_subcluster():
    assert clusters.clusters_by_subcluster('AF386507').count() == 3  # 1 if GCF_000590515.1 was minimal
