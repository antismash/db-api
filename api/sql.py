'''SQL query strings'''

CLUSTER_BY_TYPE = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters
    JOIN antismash.rel_clusters_types USING (bgc_id)
    JOIN antismash.bgc_types USING (bgc_type_id)
    WHERE lower(term) LIKE lower(%s)"""

CLUSTER_BY_MONOMER = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.rel_clusters_compounds USING (bgc_id)
    JOIN antismash.rel_compounds_monomers USING (compound_id)
    JOIN antismash.monomers USING (monomer_id)
    WHERE lower(name) LIKE lower(%s)"""

CLUSTER_BY_ACC = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    WHERE lower(acc) LIKE lower(%s)"""

CLUSTER_BY_COMPOUND_SEQ = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters
    JOIN antismash.rel_clusters_compounds USING (bgc_id)
    JOIN antismash.compounds USING (compound_id)
    WHERE lower(peptide_sequence) LIKE lower(%s)"""

CLUSTER_BY_SPECIES = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.genomes g ON seq.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
    WHERE lower(t.species) LIKE lower(%s)"""

CLUSTER_BY_GENUS = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.genomes g ON seq.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
    WHERE lower(t.genus) LIKE lower(%s)"""

CLUSTER_BY_FAMILY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.genomes g ON seq.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
    WHERE lower(t.family) LIKE lower(%s)"""

CLUSTER_BY_ORDER = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.genomes g ON seq.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
    WHERE lower(t.taxonomic_order LIKE lower(%s)"""

CLUSTER_BY_CLASS = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.genomes g ON seq.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
    WHERE lower(t.class) LIKE lower(%s)"""

CLUSTER_BY_PHYLUM = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.genomes g ON seq.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
    WHERE lower(t.phylum) LIKE lower(%s)"""

CLUSTER_BY_SUPERKINGDOM = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.genomes g ON seq.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
    WHERE lower(t.superkingdom) LIKE lower(%s)"""

CLUSTER_INFO = """
SELECT * FROM
    (SELECT
        bgc_id,
        start_pos,
        end_pos,
        cluster_number,
        seq.acc,
        version,
        t.term,
        t.description
    FROM antismash.biosynthetic_gene_clusters bgc
        JOIN antismash.loci l ON bgc.locus = l.locus_id
        JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
        JOIN antismash.rel_clusters_types USING (bgc_id)
        JOIN antismash.bgc_types t USING (bgc_type_id)
            WHERE bgc_id = %s) bgc
LEFT OUTER JOIN
    (SELECT
        bgc_id,
        acc AS cbh_acc,
        description AS cbh_description,
        similarity
    FROM antismash.clusterblast_hits
        WHERE bgc_id = %s AND rank = 1 AND
            algorithm_id = (SELECT algorithm_id FROM antismash.clusterblast_algorithms WHERE name = 'knownclusterblast')) cbh
USING (bgc_id)"""
