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
        (('monomer', 'ala'), [{'val': 'd-ala', 'desc': 'modified alanine'}]),
        (('substrate', 'alle'), [{'val': 'alle', 'desc': 'allo-isoleucine'}]),
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
        (('t2pksprofile', 'AT'), [
                                    {'val': 'AT', 'desc': 'acyltransferase'},
                                    {'val': 'KSIII', 'desc': 'non-acetate starter unit ketosynthase III'},
                                 ]),
        (('t2pksproductclass', 'iso'), [{'val': 'benzoisochromanequinone', 'desc': None}]),
        (('t2pksstarter', 'cetyl'), [{'val': 'acetyl-CoA', 'desc': None}]),
        (('goterm', 'saccharide catabolic'), [{'val': 'GO:0000272', 'desc': 'polysaccharide catabolic process'}]),
        (('goterm', 'GO:0000340'), [{'val': 'GO:0000340', 'desc': 'RNA 7-methylguanosine cap binding'}]),
        (('smcog', "4'-"), [
                                {'val': 'SMCOG1012', 'desc': "4'-phosphopantetheinyl transferase"},
                                {'val': 'SMCOG1060', 'desc': "4'-phosphopantetheinyl transferase"}
                           ]),
    ]
    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected, args
