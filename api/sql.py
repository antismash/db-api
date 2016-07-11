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
        t.description,
        taxa.species
    FROM antismash.biosynthetic_gene_clusters bgc
        JOIN antismash.loci l ON bgc.locus = l.locus_id
        JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
        JOIN antismash.rel_clusters_types USING (bgc_id)
        JOIN antismash.bgc_types t USING (bgc_type_id)
        JOIN antismash.genomes genomes ON seq.genome = genomes.genome_id
        JOIN antismash.taxa taxa ON genomes.taxon = taxa.tax_id
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

TAXTREE_SUPERKINGOM = """
SELECT superkingdom FROM antismash.taxa GROUP BY superkingdom ORDER BY superkingdom"""

TAXTREE_PHYLUM = """
SELECT phylum FROM antismash.taxa
    WHERE lower(superkingdom) = lower(%s)
    GROUP BY phylum
    ORDER BY phylum"""

TAXTREE_CLASS = """
SELECT class AS cls FROM antismash.taxa
    WHERE lower(superkingdom) = lower(%s)
    AND lower(phylum) = lower(%s)
    GROUP BY class
    ORDER BY class"""

TAXTREE_ORDER = """
SELECT taxonomic_order FROM antismash.taxa
    WHERE lower(superkingdom) = lower(%s)
    AND lower(phylum) = lower(%s)
    AND lower(class) = lower(%s)
    GROUP BY taxonomic_order
    ORDER BY taxonomic_order"""

TAXTREE_FAMILY = """
SELECT family FROM antismash.taxa
    WHERE lower(superkingdom) = lower(%s)
    AND lower(phylum) = lower(%s)
    AND lower(class) = lower(%s)
    AND lower(taxonomic_order) = lower(%s)
    GROUP BY family
    ORDER BY family"""

TAXTREE_GENUS = """
SELECT genus FROM antismash.taxa
    WHERE lower(superkingdom) = lower(%s)
    AND lower(phylum) = lower(%s)
    AND lower(class) = lower(%s)
    AND lower(taxonomic_order) = lower(%s)
    AND lower(family) = lower(%s)
    GROUP BY genus
    ORDER BY genus"""

TAXTREE_SPECIES = """
SELECT tax_id, species, acc, version FROM antismash.taxa t
    JOIN antismash.genomes g ON t.tax_id = g.taxon
    JOIN antismash.dna_sequences s ON s.genome = g.genome_id
    WHERE lower(superkingdom) = lower(%s)
    AND lower(phylum) = lower(%s)
    AND lower(class) = lower(%s)
    AND lower(taxonomic_order) = lower(%s)
    AND lower(family) = lower(%s)
    AND lower(genus) = lower(%s)
    ORDER BY species"""

STATS_CLUSTER_COUNT = "SELECT COUNT(bgc_id) FROM antismash.biosynthetic_gene_clusters"

STATS_GENOME_COUNT = "SELECT COUNT(genome_id) FROM antismash.genomes"

STATS_SEQUENCE_COUNT = "SELECT COUNT(sequence_id) FROM antismash.dna_sequences"

STATS_COUNTS_BY_TYPE = """
SELECT term, description, count FROM antismash.bgc_types
    JOIN (
        SELECT bgc_type_id, COUNT(1) FROM antismash.rel_clusters_types
        GROUP BY bgc_type_id
    ) q USING (bgc_type_id)
    ORDER BY count DESC"""

STATS_TAXON_SEQUENCES = """
SELECT tax_id, genus, species, COUNT(acc) as tax_count
    FROM antismash.dna_sequences
    JOIN antismash.genomes ON genome=genome_id
    JOIN antismash.taxa ON taxon=tax_id
    GROUP BY tax_id
    ORDER BY tax_count DESC"""

STATS_TAXON_SECMETS = """
SELECT
        tax_id,
        species,
        COUNT(DISTINCT bgc_id) AS bgc_count,
        COUNT(DISTINCT acc) AS seq_count,
        (COUNT(DISTINCT bgc_id)::float / COUNT(DISTINCT acc)) AS clusters_per_seq
    FROM antismash.biosynthetic_gene_clusters c
    JOIN antismash.loci l ON c.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.genomes g ON seq.genome=g.genome_id
    JOIN antismash.taxa t ON g.taxon=t.tax_id
    GROUP BY tax_id
    ORDER BY clusters_per_seq DESC
    LIMIT 1"""

SECMET_TREE = """
SELECT bgc_id, cluster_number, acc, term, description, species
    FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences seq ON l.sequence = seq.sequence_id
    JOIN antismash.rel_clusters_types USING (bgc_id)
    JOIN antismash.bgc_types USING (bgc_type_id)
    JOIN antismash.genomes g ON seq.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
    ORDER BY species, acc, cluster_number"""
