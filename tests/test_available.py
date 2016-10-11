from api.search import available


def test_available_term_by_category_invalid():
    assert available.available_term_by_category('foo', 'bar') == []


def test_available_term_by_category_taxonomy():
    tests = [
        (('superkingdom', 'b'), [{'val': 'Bacteria', 'desc': None}]),
        (('phylum', 'f'), [{'val': 'Firmicutes', 'desc': None}]),
        (('class', 'b'), [{'val': 'Bacilli', 'desc': None}]),
        (('order', 'l'), [{'val': 'Lactobacillales', 'desc': None}]),
        (('family', 's'), [{'val': 'Streptococcaceae', 'desc': None}, {'val': 'Streptomycetaceae', 'desc': None}]),
        (('genus', 'l'), [{'val': 'Lactococcus', 'desc': None}]),
        (('species', 'l'), [{'val': 'lactis', 'desc': None}]),
        (('strain', 'c'), [{'val': 'CV56', 'desc': None}]),
    ]

    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected


def test_available_term_by_category():
    tests = [
        (('acc', 'nc_01'), [{'val': 'NC_017486', 'desc': None}]),
        (('compoundseq', 'a'), [{'val': 'ASFGEGTFTSPSSYAIGTRCPICC', 'desc': None}]),
        (('compoundclass', 'c'), [{'val': 'Class-I', 'desc': None}, {'val': 'Class-III', 'desc': None}]),
        (('monomer', 'ala'), [{'val': 'ala', 'desc': 'Alanine'}]),
        (('type', 'lanti'), [{'val': 'lantipeptide', 'desc': 'Lanthipeptide'}]),
        (('profile', 'fabf'), [{'val': 'FabF', 'desc': 'FabF'}]),
        (('asdomain', 'PKS_DH2'), [{'val': 'PKS_DH2', 'desc': 'Dehydrogenase-2 domain'}]),
        (('clusterblast', 'HM219853'), [{'val': 'HM219853_c1', 'desc': 'Lactococcus lactis subsp. lactis nisin biosynthetic gene clust...'}]),
        (('knowncluster', 'kirro'), [{'val': 'BGC0001070_c1', 'desc': 'Kirromycin biosynthetic gene cluster'}]),
        (('subcluster', 'novobiocin'), [{'val': 'AF170880_2_c2', 'desc': 'novobiocin noviose deoxysugar'}]),
    ]
    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected, args
