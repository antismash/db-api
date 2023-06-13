from api.search import available

SCO_STRAIN = "A3(2)"


def test_available_term_by_category_invalid():
    assert available.available_term_by_category('foo', 'bar') == []


def test_available_term_by_category_taxonomy():
    tests = [
        (('superkingdom', 'b'), [{'val': 'Bacteria', 'desc': None}]),
        (('phylum', 'act'), [
            {'val': 'Actinomycetota', 'desc': None},
        ]),
        (('class', 'act'), [
            {'val': 'Actinomycetes', 'desc': None},
        ]),
        (('order', 'Kitas'), [{'val': 'Kitasatosporales', 'desc': None}]),
        (('family', 'streptom'), [{'val': 'Streptomycetaceae', 'desc': None}]),
        (('genus', 'streptom'), [{'val': 'Streptomyces', 'desc': None}]),
        (('species', 'coeli'), [{'val': 'coelicolor', 'desc': None}]),
        (('strain', SCO_STRAIN[:3]), [{'val': SCO_STRAIN, 'desc': None}]),
    ]

    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected, args


def test_available_term_by_category():
    tests = [
        (('acc', 'nc_003888'), [{'val': 'NC_003888', 'desc': None}]),
        (("clustercompare", "BGC000031"), [
            {"val": "BGC0000310", "desc": "bacitracin"},
            {"val": "BGC0000313", "desc": "beauvericin"},
            {"val": "BGC0000315", "desc": "CDA1b, CDA2a, CDA2b, CDA3a, CDA3b, CDA4a, CDA4b"},
            {"val": "BGC0000319", "desc": "cephamycin C"},
        ]),
        (('compoundseq', 'a'), [{'val': 'ASFGEGTFTSPSSYAIGTRCPICC', 'desc': None}]),
        (('compoundclass', 'c'), [{'val': 'Class %s' % c, 'desc': None} for c in ['I', 'II', 'III']]),
        (('monomer', 'ala'), [
            {'val': 'ala', 'desc': 'alanine'},
            {'val': 'd-ala', 'desc': 'modified alanine'},
        ]),
        (('substrate', 'aile'), [{'val': 'aIle', 'desc': 'allo-isoleucine'}]),
        (('type', 'lanthi'), [
            {'val': 'lanthipeptide-class-i', 'desc': 'Class I Lanthipeptide'},
            {'val': 'lanthipeptide-class-ii', 'desc': 'Class II Lanthipeptide'},
            {'val': 'lanthipeptide-class-iii', 'desc': 'Class III Lanthipeptide'},
            {'val': 'lanthipeptide-class-iv', 'desc': 'Class IV Lanthipeptide'},
            {'val': 'lanthipeptide-class-v', 'desc': 'Class V Lanthipeptide'},
            {'val': 'lipolanthine', 'desc': 'Lanthipeptide class containing N-terminal fatty acids'},
        ]),
        (('profile', 'fabf'), [{'val': 'FabF', 'desc': 'FabF'}]),
        (('asdomain', 'PKS_DH2'), [{'val': 'PKS_DH2', 'desc': 'Dehydrogenase-2 domain'}]),
        (('knowncluster', 'kendo'), [
            {'val': 'BGC0001066', 'desc': 'kendomycin'},
            {'val': 'BGC0002671', 'desc': 'kendomycin B'},
        ]),
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
        # the following are less likely to exist
        (('clusterblast', 'NZ_JABQ01000018'), [{'val': 'NZ_JABQ01000018_c93106-113862', 'desc': 'Streptomyces sp. PRh5 contig018, whole genome shotgun sequence'}]),
    ]
    for args, expected in tests:
        assert available.available_term_by_category(*args) == expected, args
