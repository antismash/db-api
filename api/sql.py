'''SQL query strings'''

CLUSTER_BY_TYPE_OR_DESCRIPTION = """
WITH RECURSIVE all_subtypes AS (
    SELECT
        bgc_type_id
    FROM antismash.bgc_types
    WHERE
        term = %s
    OR
        lower(description) LIKE lower(%s)
    UNION SELECT
        t.bgc_type_id
    FROM antismash.bgc_types t
    JOIN all_subtypes a ON (t.parent = a.bgc_type_id)
) SELECT bgc_id FROM antismash.biosynthetic_gene_clusters
    JOIN antismash.rel_clusters_types USING (bgc_id)
    JOIN all_subtypes USING (bgc_type_id)"""

CLUSTER_BY_MONOMER_OR_DESCRIPTION = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.rel_clusters_compounds USING (bgc_id)
    JOIN antismash.rel_compounds_monomers USING (compound_id)
    JOIN antismash.monomers USING (monomer_id)
    WHERE lower(name) = lower(%s) OR lower(description) LIKE lower(%s)"""

CLUSTER_BY_ACC_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    WHERE lower(acc) LIKE lower(%s)"""

CLUSTER_BY_COMPOUND_SEQ_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters
    JOIN antismash.rel_clusters_compounds USING (bgc_id)
    JOIN antismash.compounds USING (compound_id)
    WHERE lower(peptide_sequence) LIKE lower(%s)"""

CLUSTER_BY_SPECIES_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    WHERE lower(t.species) LIKE lower(%s)"""

CLUSTER_BY_GENUS_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    WHERE lower(t.genus) LIKE lower(%s)"""

CLUSTER_BY_FAMILY_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    WHERE lower(t.family) LIKE lower(%s)"""

CLUSTER_BY_ORDER_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    WHERE lower(t.taxonomic_order) LIKE lower(%s)"""

CLUSTER_BY_CLASS_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    WHERE lower(t.class) LIKE lower(%s)"""

CLUSTER_BY_PHYLUM_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    WHERE lower(t.phylum) LIKE lower(%s)"""

CLUSTER_BY_SUPERKINGDOM_FUZZY = """
SELECT bgc_id FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
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
        JOIN antismash.loci l USING (locus_id)
        JOIN antismash.dna_sequences seq USING (sequence_id)
        JOIN antismash.rel_clusters_types USING (bgc_id)
        JOIN antismash.bgc_types t USING (bgc_type_id)
        JOIN antismash.genomes genomes USING (genome_id)
        JOIN antismash.taxa taxa USING (tax_id)
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
    JOIN antismash.genomes g USING (tax_id)
    JOIN antismash.dna_sequences s USING (genome_id)
    WHERE lower(superkingdom) = lower(%s)
    AND lower(phylum) = lower(%s)
    AND lower(class) = lower(%s)
    AND lower(taxonomic_order) = lower(%s)
    AND lower(family) = lower(%s)
    AND lower(genus) = lower(%s)
    ORDER BY species, acc"""

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
SELECT t.tax_id, genus, species, COUNT(acc) AS tax_count
    FROM antismash.dna_sequences
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    GROUP BY t.tax_id
    ORDER BY tax_count DESC"""

STATS_TAXON_SECMETS = """
SELECT
        t.tax_id,
        t.species,
        COUNT(DISTINCT bgc_id) AS bgc_count,
        COUNT(DISTINCT acc) AS seq_count,
        (COUNT(DISTINCT bgc_id)::float / COUNT(DISTINCT acc)) AS clusters_per_seq
    FROM antismash.biosynthetic_gene_clusters c
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    GROUP BY t.tax_id
    ORDER BY clusters_per_seq DESC
    LIMIT 1"""

SECMET_TREE = """
SELECT bgc_id, cluster_number, acc, term, description, species
    FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l USING (locus_id)
    JOIN antismash.dna_sequences seq USING (sequence_id)
    JOIN antismash.rel_clusters_types USING (bgc_id)
    JOIN antismash.bgc_types USING (bgc_type_id)
    JOIN antismash.genomes g USING (genome_id)
    JOIN antismash.taxa t USING (tax_id)
    ORDER BY species, acc, cluster_number"""

SEARCH_IS_TYPE = "SELECT term FROM antismash.bgc_types WHERE lower(term) = lower(%s)"

SEARCH_IS_ACC = "SELECT acc FROM antismash.dna_sequences WHERE lower(acc) = lower(%s)"

SEARCH_IS_GENUS = "SELECT genus FROM antismash.taxa WHERE lower(genus) = lower(%s)"

SEARCH_IS_SPECIES = "SELECT species FROM antismash.taxa WHERE lower(species) LIKE lower(%s)"

SEARCH_IS_MONOMER = "SELECT name FROM antismash.monomers WHERE lower(name) = lower(%s)"

AVAILABLE_TYPE_FUZZY = "SELECT term FROM antismash.bgc_types WHERE lower(term) LIKE lower(%s)"

AVAILABLE_MONOMER_FUZZY = "SELECT name FROM antismash.monomers WHERE lower(name) LIKE lower(%s)"

AVAILABLE_COMPOUND_FUZZY = "SELECT peptide_sequence FROM antismash.compounds WHERE lower(peptide_sequence) LIKE lower(%s)"

AVAILABLE_ACC_FUZZY = "SELECT acc FROM antismash.dna_sequences WHERE lower(acc) LIKE lower(%s)"

AVAILABLE_SPECIES_FUZZY = "SELECT DISTINCT substring(species from (position(' ' in species)+1)) FROM antismash.taxa WHERE lower(species) LIKE lower(%s)"

AVAILABLE_GENUS_FUZZY = "SELECT DISTINCT genus FROM antismash.taxa WHERE lower(genus) LIKE lower(%s)"

AVAILABLE_FAMILY_FUZZY = "SELECT DISTINCT family FROM antismash.taxa WHERE lower(family) LIKE lower(%s)"

AVAILABLE_ORDER_FUZZY = "SELECT DISTINCT taxonomic_order FROM antismash.taxa WHERE lower(taxonomic_order) LIKE lower(%s)"

AVAILABLE_CLASS_FUZZY = "SELECT DISTINCT class FROM antismash.taxa WHERE lower(class) LIKE lower(%s)"

AVAILABLE_PHYLUM_FUZZY = "SELECT DISTINCT phylum FROM antismash.taxa WHERE lower(phylum) LIKE lower(%s)"

AVAILABLE_SUPERKINGDOM_FUZZY = "SELECT DISTINCT superkingdom FROM antismash.taxa WHERE lower(superkingdom) LIKE lower(%s)"

SEARCH_SUMMARY_TYPES = """
SELECT term, COUNT(term) FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.rel_clusters_types USING (bgc_id)
    JOIN antismash.bgc_types USING (bgc_type_id)
        WHERE bgc_id = ANY(%s)
    GROUP BY term
"""

SEARCH_SUMMARY_PHYLUM = """
SELECT phylum, COUNT(phylum) FROM antismash.biosynthetic_gene_clusters bgc
    JOIN antismash.loci l ON bgc.locus = l.locus_id
    JOIN antismash.dna_sequences s ON l.sequence = s.sequence_id
    JOIN antismash.genomes g ON s.genome = g.genome_id
    JOIN antismash.taxa t ON g.taxon = t.tax_id
        WHERE bgc_id = ANY(%s)
    GROUP BY phylum
"""
