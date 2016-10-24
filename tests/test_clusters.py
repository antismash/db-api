'Tests for the cluster search logic'

from api.search_parser import QueryTerm
from api.search import clusters
from api.models import BiosyntheticGeneCluster as Bgc


def test_guess_cluster_category():
    tests = [
        ('lantipeptide', 'type'),
        ('NC_003888', 'acc'),
        ('Streptomyces', 'genus'),
        ('lactis', 'species'),
        ('not-in-database', 'unknown')
    ]

    for search_term, expected in tests:
        term = QueryTerm.from_string(search_term)
        assert clusters.guess_cluster_category(term) == expected, search_term


def test_clusters_by_taxid():
    assert clusters.clusters_by_taxid(929102).count() == 1


def test_clusters_by_strain():
    assert clusters.clusters_by_strain('CV56').count() == 1


def test_clusters_by_species():
    assert clusters.clusters_by_species('lactis').count() == 1


def test_clusters_by_genus():
    assert clusters.clusters_by_genus('lactococcus').count() == 1


def test_clusters_by_family():
    assert clusters.clusters_by_family('streptococcaceae').count() == 1


def test_clusters_by_order():
    assert clusters.clusters_by_order('lactobacillales').count() == 1


def test_clusters_by_class():
    assert clusters.clusters_by_class('bacilli').count() == 1


def test_clusters_by_phylum():
    assert clusters.clusters_by_phylum('firmicutes').count() == 1


def test_clusters_by_superkingdom():
    assert clusters.clusters_by_superkingdom('bacteria').count() == Bgc.query.count()


def test_clusters_by_monomer():
    assert clusters.clusters_by_monomer('ala').count() == 2


def test_clusters_by_acc():
    assert clusters.clusters_by_acc('NC_017486').count() == 1


def test_clusters_by_compoundseq():
    assert clusters.clusters_by_compoundseq('ITSISLC').count() == 1


def test_clusters_by_compoundclass():
    assert clusters.clusters_by_compoundclass('Class-I').count() == 4


def test_clusters_by_profile():
    assert clusters.clusters_by_profile('LANC').count() == 4


def test_clusters_by_asdomain():
    assert clusters.clusters_by_asdomain('ACP').count() == 43


def test_clusters_by_clusterblast():
    assert clusters.clusters_by_clusterblast('CP002365_c2').count() == 1


def test_clusters_by_knowncluster():
    assert clusters.clusters_by_knowncluster('BGC0000535_c1').count() == 1


def test_clusters_by_subcluster():
    assert clusters.clusters_by_subcluster('AF386507_1_c1').count() == 1
