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
        (('genus', 'streptom'), [{'val': 'Streptomonospora', 'desc': None}, {'val': 'Streptomyces', 'desc': None}]),
        (('species', 'coeli'), [{'val': 'coelicolor', 'desc': None}]),
        (('strain', 'a3('), [{'val': 'A3(2)', 'desc': None}]),
    ]

    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected


def test_available_term_by_category():
    tests = [
        (('acc', 'nc_003888'), [{'val': 'NC_003888', 'desc': None}]),
        (('compoundseq', 'a'), [{'val': 'ASFGEGTFTSPSSYAIGTRCPICC', 'desc': None}]),
        (('compoundclass', 'c'), [{'val': 'Class %s' % c, 'desc': None} for c in ['I', 'II', 'III']]),
        (('monomer', 'ala'), [{'val': 'd-ala', 'desc': 'modified Alanine'}]),
        (('substrate', 'ala'), [{'val': 'ala', 'desc': 'Alanine'}, {'val': 'ala-thz', 'desc': 'Alanine-thiazole'}]),
        (('type', 'lanthi'), [{'val': 'lanthipeptide', 'desc': 'Lanthipeptide'}]),
        (('profile', 'fabf'), [{'val': 'FabF', 'desc': 'FabF'}]),
        (('asdomain', 'PKS_DH2'), [{'val': 'PKS_DH2', 'desc': 'Dehydrogenase-2 domain'}]),
        (('clusterblast', 'NZ_JOCE01000008'), [{'val': 'NZ_JOCE01000008_c8', 'desc': 'Streptomyces violaceoruber strain NRRL S-12 contig8.1, whole genome shotgun sequence'}]),
        (('knowncluster', 'kendo'), [{'val': 'BGC0001066', 'desc': 'Kendomycin'}]),
        (('subcluster', 'bal'), [
                                    {'val': 'Y16952_c1', 'desc': 'balhimycin dihydroxyphenylglycine'},
                                    {'val': 'Y16952_c2', 'desc': 'balhimycin hydroxyphenylglycine'},
                                    {'val': 'Y16952_c4', 'desc': 'balhimycin glucose / vancosamine'},
                                ]),
        (('resfam', 'RF0001'), [{'desc': '16S ribosomal RNA methyltransferase [ARO:3000857]', 'val': '16S_rRNA_methyltrans'}]),
        (('resfam', '6S ribosomal RNA meth'), [{'desc': '16S ribosomal RNA methyltransferase [ARO:3000857]', 'val': '16S_rRNA_methyltrans'}]),
        (('resfam', '16S_rRNA_methyl'), [{'desc': '16S ribosomal RNA methyltransferase [ARO:3000857]', 'val': '16S_rRNA_methyltrans'}]),
    ]
    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected, args
