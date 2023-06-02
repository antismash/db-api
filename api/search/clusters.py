'''Cluster-related search options'''

from flask import g

from sqlalchemy import (
    or_,
    sql,
)
from sqlalchemy.orm import joinedload
from .helpers import (
    break_lines,
    register_handler,
    UnknownQueryError,
)
from api.location import location_from_string
from api.models import (
    db,
    AsDomain,
    AsDomainProfile,
    AsDomainSubtype,
    BgcType,
    BindingSite,
    Candidate,
    CandidateType,
    ClusterblastAlgorithm,
    ClusterblastHit,
    Cds,
    ComparippsonHit,
    ComparippsonMibigReference,
    Genome,
    DnaSequence,
    Module,
    ModuleDomainFunction,
    Monomer,
    Pfam,
    PfamDomain,
    Profile,
    ProfileHit,
    Protocluster,
    Region,
    Regulator,
    RelModulesMonomer,
    ResfamDomain,
    Resfam,
    Ripp,
    Smcog,
    SmcogHit,
    Substrate,
    T2pk,
    T2pksCdsDomain,
    T2pksProductClass,
    T2pksStarter,
    T2pksStarterElongation,
    Taxa,
    Tigrfam,
    TigrfamDomain,
    t_rel_as_domain_to_subtype,
    t_rel_regions_types,
)

from .modules import (
    InvalidQueryError,
    parse_module_query,
)

CLUSTERS = {}
CLUSTER_FORMATTERS = {}

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Executable, ClauseElement, _literal_as_text

class explain(Executable, ClauseElement):
    def __init__(self, stmt, analyse=False):
        self.statement = _literal_as_text(stmt)
        self.analyse = analyse
        self.inline = getattr(stmt, 'inline', None)


@compiles(explain, 'postgresql')
def pg_explain(element, compiler, **kw):
    text = "EXPLAIN "
    if element.analyse:
        text += "ANALYZE "
    text += compiler.process(element.statement, **kw)
    compiler.isinsert = compiler.isupdate = compiler.isdelete = False

    return text


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_json(clusters):
    '''Convert model.BiosyntheticGeneClusters into JSON'''
    query = db.session.query(Region, Genome.assembly_id, DnaSequence.accession, DnaSequence.version, DnaSequence.record_number, Taxa.genus, Taxa.species, Taxa.strain)
    query = query.options(joinedload(Region.bgc_types)).options(joinedload(Region.clusterblast_hits))
    query = query.join(DnaSequence, Region.dna_sequence).join(Genome, DnaSequence.genome).join(Taxa, Genome.tax)
    query = query.filter(Region.region_id.in_(map(lambda x: x.region_id, clusters)))
    query = query.order_by(Region.region_id)

    json_clusters = []
    for cluster in query.all():
        json_cluster = {}
        json_cluster['bgc_id'] = cluster.Region.region_id
        json_cluster['record_number'] = cluster.record_number
        json_cluster['region_number'] = cluster.Region.region_number

        location = location_from_string(cluster.Region.location)

        json_cluster['start_pos'] = location.start
        json_cluster['end_pos'] = location.end

        json_cluster['acc'] = cluster.accession
        json_cluster['assembly_id'] = cluster.assembly_id
        json_cluster['version'] = cluster.version

        json_cluster['genus'] = cluster.genus
        json_cluster['species'] = cluster.species
        json_cluster['strain'] = cluster.strain

        term = ' - '.join(sorted([t.term for t in cluster.Region.bgc_types]))
        if len(cluster.Region.bgc_types) == 1:
            json_cluster['description'] = cluster.Region.bgc_types[0].description
            json_cluster['term'] = term
        else:
            descs = ' & '.join(sorted([t.description for t in cluster.Region.bgc_types],
                                      key=str.casefold))
            json_cluster['description'] = 'Hybrid region: {}'.format(descs)
            json_cluster['term'] = '{} hybrid'.format(term)

        json_cluster['similarity'] = None
        json_cluster['cbh_description'] = None
        json_cluster['cbh_acc'] = None

        knownclusterblasts = [hit for hit in cluster.Region.clusterblast_hits if hit.algorithm.name == 'knownclusterblast' and hit.rank == 1]
        if len(knownclusterblasts) > 0:
            json_cluster['similarity'] = knownclusterblasts[0].similarity
            json_cluster['cbh_description'] = knownclusterblasts[0].description
            json_cluster['cbh_acc'] = knownclusterblasts[0].acc
            json_cluster['cbh_rank'] = knownclusterblasts[0].rank

        json_cluster['contig_edge'] = cluster.Region.contig_edge

        json_clusters.append(json_cluster)
    return json_clusters


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_csv(clusters):
    '''Convert model.BiosyntheticGeneClusters into CSV'''
    json_clusters = clusters_to_json(clusters)
    csv_lines = ['#Genus\tSpecies\tStrain\tNCBI accession\tFrom\tTo\tBGC type\tOn contig edge\tMost similar known cluster\tSimilarity in %\tMIBiG BGC-ID\tResults URL\tDownload URL']
    for cluster in json_clusters:
        csv_lines.append('{genus}\t{species}\t{strain}\t{acc}.{version}\t{start_pos}\t{end_pos}\t{term}\t'
                         '{contig_edge}\t{cbh_description}\t{similarity}\t{cbh_acc}\t'
                         'https://antismash-db.secondarymetabolites.org/go/{acc}.{version}/{start_pos}-{end_pos}\t'
                         'https://antismash-db.secondarymetabolites.org/api/v1.0/download/genbank/{assembly_id}/{acc}/{version}/{start_pos}/{end_pos}'.format(**cluster))
    return csv_lines


@register_handler(CLUSTER_FORMATTERS)
def clusters_to_fasta(clusters):
    '''Convert model.BiosyntheticGeneCluster into FASTA'''
    query = db.session.query(Region, DnaSequence.accession, DnaSequence.version,
                             Taxa.tax_id, Taxa.genus, Taxa.species, Taxa.strain)
    query = query.options(joinedload('bgc_types'))
    query = query.join(DnaSequence).join(Genome).join(Taxa)
    query = query.filter(Region.region_id.in_(map(lambda x: x.region_id, clusters))).order_by(Region.region_id)
    search = ''
    if g.verbose:
        search = "|{}".format(g.search_str)
    for cluster in query:
        location = location_from_string(cluster.Region.location)
        seq = break_lines(cluster.Region.dna_sequence.dna[location.start+1:location.end])
        compiled_type = ' - '.join(sorted([t.term for t in cluster.Region.bgc_types], key=str.casefold))
        fasta = '>{c.accession}.{c.version}|{location.start}-{location.end}|' \
                '{compiled_type}|' \
                '{c.genus} {c.species} {c.strain}{search}\n{seq}' \
                .format(c=cluster, region_number=cluster.Region.region_number,
                        compiled_type=compiled_type, search=search, seq=seq, location=location)
        yield fasta


def cluster_query_from_term(term):
    '''Recursively generate an SQL query from the search terms'''
    if term.kind == 'expression':
        if term.category == 'unknown':
            term.category = guess_cluster_category(term)
        if term.category in CLUSTERS:
            query = CLUSTERS[term.category](term.term)
            for query_filter, data in term.filters:
                query = query_filter.run(query, data)
            return query
        else:
            raise UnknownQueryError()
    elif term.kind == 'operation':
        left_query = cluster_query_from_term(term.left)
        right_query = cluster_query_from_term(term.right)
        if term.operation == 'except':
            return left_query.except_(right_query)
        elif term.operation == 'or':
            return left_query.union(right_query)
        elif term.operation == 'and':
            return left_query.intersect(right_query)

    raise UnknownQueryError()


def guess_cluster_category(term):
    '''Guess cluster search category from term'''
    if BgcType.query.filter(BgcType.term.ilike(f"{term.term}%")).count() > 0:
        return 'type'
    if DnaSequence.query.filter(DnaSequence.accession.ilike(term.term)).count() > 0:
        return 'acc'
    if Taxa.query.filter(Taxa.genus.ilike(term.term)).count() > 0:
        return 'genus'
    if Taxa.query.filter(Taxa.species.ilike(term.term)).count() > 0:
        return 'species'

    return term.category


@register_handler(CLUSTERS)
def clusters_by_type(term):
    '''Return a query for a bgc by type or type description search'''
    all_subtypes = db.session.query(BgcType).filter(or_(BgcType.term == term, BgcType.description.ilike('%{}%'.format(term)))).cte(recursive=True)
    all_subtypes = all_subtypes.union(db.session.query(BgcType).filter(BgcType.parent_id == all_subtypes.c.bgc_type_id))
    return db.session.query(Region).join(t_rel_regions_types).join(all_subtypes)


@register_handler(CLUSTERS)
def clusters_by_candidatekind(term):
    '''Return a query for a bgc by type or type description search'''
    return Candidate.query.join(Region).join(CandidateType).filter(CandidateType.description.ilike(f"%{term}%"))


@register_handler(CLUSTERS)
def clusters_by_taxid(term):
    '''Return a query for a bgc by NCBI taxid'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.tax_id == term)


@register_handler(CLUSTERS)
def clusters_by_strain(term):
    '''Return a query for a bgc by strain search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.strain.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_species(term):
    '''Return a query for a bgc by species search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.species.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_genus(term):
    '''Return a query for a bgc by genus search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.genus.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_family(term):
    '''Return a query for a bgc by family search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.family.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_order(term):
    '''Return a query for a bgc by order search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.taxonomic_order.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_class(term):
    '''Return a query for a bgc by class search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa._class.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_phylum(term):
    '''Return a query for a bgc by phylum search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.phylum.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_superkingdom(term):
    '''Return a query for a bgc by superkingdom search'''
    return Region.query.join(DnaSequence).join(Genome).join(Taxa).filter(Taxa.superkingdom.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_monomer(term):
    '''Return a query for a bgc by monomer or monomer description search'''
    return Region.query.join(Module).join(RelModulesMonomer).join(Monomer).filter(Monomer.name.ilike(term.lower()))


@register_handler(CLUSTERS)
def clusters_by_substrate(term):
    '''Return a query for a bgc by substrate or substrate description search'''
    return Region.query.join(Module).join(RelModulesMonomer).join(Substrate).filter(Substrate.name.ilike(term.lower()))


@register_handler(CLUSTERS)
def clusters_by_acc(term):
    '''Return a query for a bgc by accession number search'''
    return Region.query.join(DnaSequence).filter(DnaSequence.accession.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_assembly(term):
    """Return a query for a bgc by assembly_id search"""
    return Region.query.join(DnaSequence).join(Genome).filter(Genome.assembly_id.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_compoundseq(term):
    '''Return a query for a bgc by compound sequence search'''
    return Region.query.join(Protocluster).join(Ripp).filter(Ripp.peptide_sequence.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_compoundclass(term):
    '''Return a query for a bgc by compound class'''
    return Region.query.join(Protocluster).join(Ripp).filter(Ripp.subclass.ilike(term))


@register_handler(CLUSTERS)
def clusters_by_profile(term):
    '''Return a query for a bgc by profile name'''
    return Region.query.join(Cds) \
                 .join(ProfileHit).join(Profile) \
                 .filter(Profile.name.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_smcog(term):
    '''Return a query for a bgc by smcog'''
    return Region.query.join(Cds) \
                 .join(SmcogHit).join(Smcog) \
                 .filter(Smcog.name.ilike('%{}%'.format(term)))


@register_handler(CLUSTERS)
def clusters_by_asdomain(term):
    '''Return a query for a bgc by asdomain name'''
    return Region.query.join(Cds, Region.region_id == Cds.region_id) \
                 .join(AsDomain).join(AsDomainProfile) \
                 .filter(AsDomainProfile.name.ilike(term))


@register_handler(CLUSTERS)
def clusters_by_asdomainsubtype(term):
    '''Return a query for a BGC by aSDomain subtype'''
    all_subtypes = AsDomainSubtype.query \
            .filter(or_(AsDomainSubtype.subtype == term,
                        AsDomainSubtype.description.ilike('%{}%'.format(term)))) \
            .cte(recursive=True)
    return db.session.query(Region).join(Cds).join(AsDomain) \
           .join(t_rel_as_domain_to_subtype).join(all_subtypes)


@register_handler(CLUSTERS)
def clusters_by_contigedge(term):
    return Region.query.filter(Region.contig_edge.is_(term))


def clusters_by_x_clusterblast(term, algorithm):
    '''Generic query for XClusterBlast hits'''
    return Region.query.join(ClusterblastHit).join(ClusterblastAlgorithm) \
                 .filter(ClusterblastAlgorithm.name == algorithm) \
                 .filter(ClusterblastHit.acc.ilike("%{}%".format(term)))  # TODO: not sure if this shouldn't be straight ilike


@register_handler(CLUSTERS)
def clusters_by_clusterblast(term):
    '''Return a query for a bgc by ClusterBlast hit'''
    return clusters_by_x_clusterblast(term, 'clusterblast')


@register_handler(CLUSTERS)
def clusters_by_knowncluster(term):
    '''Return a query for a bgc by KnownClusterBlast hit'''
    return clusters_by_x_clusterblast(term, 'knownclusterblast')


@register_handler(CLUSTERS)
def clusters_by_subcluster(term):
    '''Return a query for a bgc by SubClusterBlast hit'''
    return clusters_by_x_clusterblast(term, 'subclusterblast')


@register_handler(CLUSTERS)
def clusters_by_resfam(term):
    '''Return a query for a region by Resfam hit'''
    search = "%{}%".format(term)
    return Region.query.join(Cds).join(ResfamDomain).join(Resfam).filter(or_(Resfam.accession.ilike(search), Resfam.name.ilike(search), Resfam.description.ilike(search)))


@register_handler(CLUSTERS)
def clusters_by_pfam(term):
    '''Return a query for a region by Pfam hit'''
    search = "%{}%".format(term)
    query = Region.query.join(Cds).join(PfamDomain).join(Pfam)
    if term.lower().startswith("pfam"):
        return query.filter(Pfam.pfam_id.ilike(search))
    return query.filter(or_(Pfam.pfam_id.ilike(search), Pfam.name.ilike(search), Pfam.description.ilike(search)))


@register_handler(CLUSTERS)
def clusters_by_tigrfam(term):
    '''Return a query for a region by Pfam hit'''
    search = "%{}%".format(term)
    query = Region.query.join(Cds).join(TigrfamDomain).join(Tigrfam)
    if term.lower().startswith("tigrfam"):
        return query.filter(Tigrfam.tigrfam_id.ilike(search))
    return query.filter(or_(Tigrfam.tigrfam_id.ilike(search), Tigrfam.name.ilike(search), Tigrfam.description.ilike(search)))


@register_handler(CLUSTERS)
def clusters_by_tfbs(term):
    """Returns a query for regions containing a match for the given TFBS regulator name"""
    search = "%{}%".format(term)
    return Region.query.join(BindingSite).join(Regulator) \
                 .filter(or_(Regulator.name.ilike(term), Regulator.name.ilike(term)))


@register_handler(CLUSTERS)
def clusters_by_t2pksclass(term):
    '''Return a query for a region with a t2pks of specific product class'''
    search = "%{}%".format(term)
    return Region.query.join(Protocluster).join(T2pk).join(T2pksProductClass).filter(T2pksProductClass.product_class.ilike(search))


@register_handler(CLUSTERS)
def clusters_by_t2pksstarter(term):
    '''Return a query for a region with a t2pks of specific starter'''
    search = "%{}%".format(term)
    return Region.query.join(Protocluster).join(T2pk).join(T2pksStarter).filter(T2pksStarter.name.ilike(search))


@register_handler(CLUSTERS)
def clusters_by_t2pkselongation(term):
    '''Return a query for a region with a t2pks of specific elongation'''
    search = "%d" % term
    return Region.query.join(Protocluster).join(T2pk).join(T2pksStarter).join(T2pksStarterElongation).filter(T2pksStarterElongation.elongation == search)


@register_handler(CLUSTERS)
def clusters_by_modulequery(term):
    '''Return a query for a region containing a module with specific construction'''
    # TODO: handle PKS subtypes
    # TODO: handle substrates
    # TODO: handle aggregate domains


    class QueryPart:
        def __init__(self, operator, section, value, index):
            self.operator = operator
            self.section = section
            self.value = value
            self.matching_domain_ids = None
            self.index = index

        def short(self):
            return "%s %s %s" % (self.section, self.operator, self.value)

        def __repr__(self):
            return "%s %r %s: %s" % (self.section, self.operator, self.value, sorted(self.matching_domain_ids) if self.matching_domain_ids else "not yet matched")


    def match(query_part, absolute_modules, section_modules, aggregate=False, previous_part=None):
        operator = query_part.operator
        section = query_part.section
        value = query_part.value

        def filter_by_existing(current_query):
            # the first part of the section
            if query_part.index == 0:
                assert operator is None, query_part
                # filter by other previous sections' constraints, if they exist
                if absolute_modules:
                    current_query.filter(Module.module_id.in_(absolute_modules))
                return current_query

            # following parts of the section must all have things that matched the earlier parts
            assert section_modules is not None, query_part
            return current_query.filter(Module.module_id.in_(section_modules))

        assert operator != module_query.IGNORE, query_part

        # base query
        matches = Module.query.with_entities(Module.module_id, AsDomain.as_domain_id)

        # limit by section or absolute
        matches = filter_by_existing(matches)
        matches = matches.join(AsDomain).join(ModuleDomainFunction).join(AsDomainProfile)
        # filter by section
        # NONE is a special case and can return early
        if value == module_query.NONE:
            # find all modules in existing that have a hit for that function
            sub = Module.query.with_entities(Module.module_id).join(AsDomain).join(ModuleDomainFunction).filter(ModuleDomainFunction.function == section)
            sub = filter_by_existing(sub)
            # then exclude them
            matches = matches.filter(~Module.module_id.in_(sub))
            module_ids = {i[0] for i in matches}
            assert section_modules is None or module_ids.issubset(section_modules), section_modules.difference(module_ids)
            return module_ids

        matches = matches.filter(ModuleDomainFunction.function == section)

        # match value (skip for ANY, only function presence is required)
        if value != module_query.ANY:
            matches = matches.filter(AsDomainProfile.name == value)

        # modify result set by operator
        # - AND handled as part of the base query limit
        # - OR should never propagate this far
        assert operator != module_query.OR
        # which leaves THEN
        if operator == module_query.THEN:
            # as per AND, but with extra restrictions
            assert previous_part and section_modules is not None
            # if there's nothing that matched the previous part, abort and skip the database interaction
            if previous_part.matching_domain_ids is not None and not previous_part.matching_domain_ids:
                query_part.matching_domain_ids = set()
                return set()

            assert previous_part.matching_domain_ids, previous_part
            prev_domains = AsDomain.query.with_entities(AsDomain.as_domain_id).join(AsDomainProfile)
            # restrict to previous ids in case of chained THEN
            if previous_part.operator == module_query.THEN:
                prev_domains = prev_domains.filter(AsDomain.as_domain_id.in_(previous_part.matching_domain_ids))

            if previous_part.value != module_query.ANY:
                prev_domains = prev_domains.filter(AsDomainProfile.name == previous_part.value)
            matches = matches.filter(AsDomain.follows.in_({i[0] for i in prev_domains.all()}))
        all_matches = list(matches.all())
        query_part.matching_domain_ids = {i[1] for i in all_matches}
        module_ids = {i[0] for i in all_matches}
        assert section_modules is None or module_ids.issubset(section_modules), section_modules.difference(module_ids)
        return module_ids

    attrs = ["starter", "loader", "modification", "carrier_protein", "finalisation", "other"]

    module_query = parse_module_query(term)
    matching_modules = None

    for attr, alternatives in module_query:
        matching_alternatives = None
        for alternative in alternatives:
            if not alternative or set(alternative) == {module_query.IGNORE}:
                continue
            section_modules = None

            previous_part = None
            op = None

            for i, value in enumerate(alternative):
                # don't continue looking if everything is already excluded 
                if section_modules is not None and not section_modules:
                    break
                if i % 2:
                    op = value
                    assert op != module_query.OR  # this should never be in a group, it's what separates the alternatives
                    continue
                if op != module_query.THEN:
                    previous_part = None  # simplify everything
                if value == module_query.IGNORE:
                    continue
                query_part = QueryPart(op, attr, value, i)

                section_modules = match(query_part, matching_modules, section_modules, previous_part=previous_part)  # TODO handle aggregate
                assert section_modules is not None, "%s %s" % (attr, query_part)
                previous_part = query_part

            if previous_part is None:
                continue

            if matching_alternatives is None:
                matching_alternatives = set()
            matching_alternatives.update(section_modules)

        if matching_alternatives is None:
            continue

        if matching_modules is None:
            matching_modules = matching_alternatives
        else:
            matching_modules = matching_modules.intersection(matching_alternatives)
    # in the case where all options are ANY, no results are generated yet
    if matching_modules is None:
        raise InvalidQueryError("no restrictions in query: %s" % term)
    return Region.query.join(Module).filter(Module.module_id.in_(matching_modules)).distinct(Region.region_id)


@register_handler(CLUSTERS)
def clusters_by_crosscdsmodule(term=""):
    """Return a query for regions containing a cross-CDS module"""
    return Region.query.join(Module).filter(Module.multi_gene == True)


@register_handler(CLUSTERS)
def clusters_by_comparippsonmibig(term):
    """Return a query for regions containing a CompaRiPPson hit against the given MIBiG entry"""
    search = f"%{term}%"
    return Region.query.join(ComparippsonHit).join(ComparippsonMibigReference) \
             .filter(or_(
                ComparippsonMibigReference.accession.ilike(search),
                ComparippsonMibigReference.compound.ilike(search),
                ComparippsonMibigReference.product.ilike(search),
            ))
