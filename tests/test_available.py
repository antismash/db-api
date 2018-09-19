from api.search import available


def test_available_term_by_category_invalid():
    assert available.available_term_by_category('foo', 'bar') == []


def test_available_term_by_category_taxonomy():
    tests = [
        (('superkingdom', 'b'), [{'val': 'Bacteria', 'desc': None}]),
        (('phylum', 'act'), [{'val': 'Actinobacteria', 'desc': None}]),
        (('class', 'act'), [{'val': 'Actinobacteria', 'desc': None}]),
        (('order', 'streptom'), [{'val': 'Streptomycetales', 'desc': None}]),
        (('family', 'streptom'), [{'val': 'Streptomycetaceae', 'desc': None}]),
        (('genus', 'streptom'), [{'val': 'Streptomyces', 'desc': None}]),
        (('species', 'coeli'), [{'val': 'coelicolor', 'desc': None}]),
        (('strain', 'a3('), [{'val': 'A3(2)', 'desc': None}]),
    ]

    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected


def test_available_term_by_category():
    tests = [
        (('acc', 'nc_003888'), [{'val': 'NC_003888', 'desc': None}]),
        (('compoundseq', 'a'), [{'val': 'ASFGEGTFTSPSSYAIGTRCPICC', 'desc': None}]),
        (('compoundclass', 'c'), [{'val': 'Class-I', 'desc': None}, {'val': 'Class-III', 'desc': None}]),
        (('monomer', 'ala'), [{'val': 'ala', 'desc': 'Alanine'}, {'desc': 'Alanine-thiazole', 'val': 'ala-thz'}]),
        (('type', 'lanti'), [{'val': 'lantipeptide', 'desc': 'Lanthipeptide'}]),
        (('profile', 'fabf'), [{'val': 'FabF', 'desc': 'FabF'}]),
        (('asdomain', 'PKS_DH2'), [{'val': 'PKS_DH2', 'desc': 'Dehydrogenase-2 domain'}]),
        (('clusterblast', 'AB469822'), [{'val': 'AB469822_c1', 'desc': 'Streptomyces griseoviridis DNA, includes prodigiosin biosynthe...'}]),
        (('knowncluster', 'kendo'), [{'val': 'BGC0001066_c1', 'desc': 'Kendomycin'}]),
        (('subcluster', 'bal'), [{'val': 'Y16952_2_c2', 'desc': 'balhimycin hydroxyphenylglycine'}]),
    ]
    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected, args
