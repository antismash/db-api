'''Available terms by category searches

This is used for the web UI for typeahead opetions
'''

from sqlalchemy import (
    distinct,
    null,
    or_,
)

from .helpers import (
    register_handler,
    sanitise_string,
)

from api.models import (
    db,
    AsDomainProfile,
    BgcType,
    ClusterblastAlgorithm,
    ClusterblastHit,
    DnaSequence,
    GeneOntology,
    Genome,
    Monomer,
    Profile,
    Resfam,
    Ripp,
    Smcog,
    Substrate,
    T2pksProductClass,
    T2pksProfile,
    T2pksStarter,
    Taxa,
)

AVAILABLE = {}


def available_term_by_category(category, term):
    '''List all available terms by category'''
    cleaned_category = sanitise_string(category)
    cleaned_term = sanitise_string(term)

    if cleaned_category in AVAILABLE:
        query = AVAILABLE[cleaned_category](cleaned_term).limit(50)
        return list(map(lambda x: {'val': x[0], 'desc': x[1]}, query.all()))

    return []


class FakeBooleanQuery:
    """Fake Query class to return boolean value typeaheads."""
    def __init__(self, term, true_help, false_help):
        term = term.casefold()
        if term in {'t', 'tr', 'tru', 'true', 'y', 'ye', 'yes'}:
            self.results = (('true', true_help),)
        elif term in {'f', 'fa', 'fal', 'fals', 'false', 'n', 'no'}:
            self.results = (('false', false_help),)
        else:
            self.results = []

    def limit(self, _):
        return self

    def all(self):
        return self.results


@register_handler(AVAILABLE)
def available_contigedge(term):
    """Generate FakeBooleanQuery for available boolean contigedge options."""
    return FakeBooleanQuery(term, 'Cluster is on a contig edge', 'Cluster is not on a contig edge')


@register_handler(AVAILABLE)
def available_minimal(term):
    """Generate FakeBooleanQuery for available boolean minimal options."""
    return FakeBooleanQuery(term, 'Cluster was found in fast-mode antiSMASH run', 'Cluster was found in a full antiSMASH run')


@register_handler(AVAILABLE)
def available_superkingdom(term):
    '''Generate query for available superkingdoms'''
    return db.session.query(distinct(Taxa.superkingdom), null()) \
             .filter(Taxa.superkingdom.ilike('{}%'.format(term))) \
             .order_by(Taxa.superkingdom)


@register_handler(AVAILABLE)
def available_phylum(term):
    '''Generate query for available phyla'''
    return db.session.query(distinct(Taxa.phylum), null()) \
             .filter(Taxa.phylum.ilike('{}%'.format(term))) \
             .order_by(Taxa.phylum)


@register_handler(AVAILABLE)
def available_class(term):
    '''Generate query for available class'''
    return db.session.query(distinct(Taxa._class), null()) \
             .filter(Taxa._class.ilike('{}%'.format(term))) \
             .order_by(Taxa._class)


@register_handler(AVAILABLE)
def available_order(term):
    '''Generate query for available order'''
    return db.session.query(distinct(Taxa.taxonomic_order), null()) \
             .filter(Taxa.taxonomic_order.ilike('{}%'.format(term))) \
             .order_by(Taxa.taxonomic_order)


@register_handler(AVAILABLE)
def available_family(term):
    '''Generate query for available family'''
    return db.session.query(distinct(Taxa.family), null()) \
             .filter(Taxa.family.ilike('{}%'.format(term))) \
             .order_by(Taxa.family)


@register_handler(AVAILABLE)
def available_genus(term):
    '''Generate query for available genus'''
    return db.session.query(distinct(Taxa.genus), null()) \
             .filter(Taxa.genus.ilike('{}%'.format(term))) \
             .order_by(Taxa.genus)


@register_handler(AVAILABLE)
def available_species(term):
    '''Generate query for available species'''
    return db.session.query(distinct(Taxa.species), null()) \
             .filter(Taxa.species.ilike('{}%'.format(term))) \
             .order_by(Taxa.species)


@register_handler(AVAILABLE)
def available_strain(term):
    '''Generate query for available strain'''
    return db.session.query(distinct(Taxa.strain), null()) \
             .filter(Taxa.strain.ilike('{}%'.format(term))) \
             .order_by(Taxa.strain)


@register_handler(AVAILABLE)
def available_acc(term):
    '''Generate query for available accession'''
    return db.session.query(distinct(DnaSequence.accession), null()) \
             .filter(DnaSequence.accession.ilike('{}%'.format(term))) \
             .order_by(DnaSequence.accession)


@register_handler(AVAILABLE)
def available_assembly(term):
    """Generate query for available accession"""
    return db.session.query(distinct(Genome.assembly_id), null()) \
             .filter(Genome.assembly_id.ilike('{}%'.format(term))) \
             .order_by(Genome.assembly_id)


@register_handler(AVAILABLE)
def available_compoundseq(term):
    '''Generate query for available compound by peptide sequence'''
    return db.session.query(distinct(Ripp.peptide_sequence), null()) \
             .filter(Ripp.peptide_sequence.ilike('{}%'.format(term))) \
             .order_by(Ripp.peptide_sequence)


@register_handler(AVAILABLE)
def available_compoundclass(term):
    '''Generate query for available compound by class'''
    return db.session.query(distinct(Ripp.subclass), null()) \
             .filter(Ripp.subclass.ilike('{}%'.format(term))) \
             .order_by(Ripp.subclass)


@register_handler(AVAILABLE)
def available_substrate(term):
    '''Generate query for available substrates'''
    return db.session.query(distinct(Substrate.name), Substrate.description) \
             .filter(or_(Substrate.name.ilike('{}%'.format(term)), Substrate.description.ilike('{}%'.format(term)))) \
             .order_by(Substrate.name)


@register_handler(AVAILABLE)
def available_monomer(term):
    '''Generate query for available monomers'''
    return db.session.query(distinct(Monomer.name), Monomer.description).join(Substrate) \
             .filter(or_(Monomer.name.ilike('{}%'.format(term)), Substrate.description.ilike('{}%'.format(term)))) \
             .order_by(Monomer.name)


@register_handler(AVAILABLE)
def available_type(term):
    '''Generate query for available type'''
    return db.session.query(distinct(BgcType.term), BgcType.description) \
             .filter(or_(BgcType.term.ilike('{}%'.format(term)), BgcType.description.ilike('{}%'.format(term)))) \
             .order_by(BgcType.term)


@register_handler(AVAILABLE)
def available_profile(term):
    '''Generate query for available asDomain profile'''
    return db.session.query(distinct(Profile.name), Profile.description) \
             .filter(or_(Profile.name.ilike('{}%'.format(term)), Profile.description.ilike('%{}%'.format(term)))) \
             .order_by(Profile.name)


@register_handler(AVAILABLE)
def available_asdomain(term):
    '''Generate query for available asDomain profile'''
    return db.session.query(distinct(AsDomainProfile.name), AsDomainProfile.description) \
             .filter(or_(AsDomainProfile.name.ilike('{}%'.format(term)), AsDomainProfile.description.ilike('%{}%'.format(term)))) \
             .order_by(AsDomainProfile.name)


@register_handler(AVAILABLE)
def available_clusterblast(term):
    '''Generate query for available ClusterBlast hits'''
    return db.session.query(distinct(ClusterblastHit.acc), ClusterblastHit.description) \
             .join(ClusterblastAlgorithm).filter(ClusterblastAlgorithm.name == 'clusterblast') \
             .filter(or_(ClusterblastHit.acc.ilike('{}%'.format(term)), ClusterblastHit.description.ilike('%{}%'.format(term)))) \
             .order_by(ClusterblastHit.acc)


@register_handler(AVAILABLE)
def available_knowncluster(term):
    '''Generate query for available KnownClusterBlast hits'''
    return db.session.query(distinct(ClusterblastHit.acc), ClusterblastHit.description) \
             .join(ClusterblastAlgorithm).filter(ClusterblastAlgorithm.name == 'knownclusterblast') \
             .filter(or_(ClusterblastHit.acc.ilike('{}%'.format(term)), ClusterblastHit.description.ilike('%{}%'.format(term)))) \
             .order_by(ClusterblastHit.acc)


@register_handler(AVAILABLE)
def available_subcluster(term):
    '''Generate query for available SubClusterBlast hits'''
    return db.session.query(distinct(ClusterblastHit.acc), ClusterblastHit.description) \
             .join(ClusterblastAlgorithm).filter(ClusterblastAlgorithm.name == 'subclusterblast') \
             .filter(or_(ClusterblastHit.acc.ilike('{}%'.format(term)), ClusterblastHit.description.ilike('%{}%'.format(term)))) \
             .order_by(ClusterblastHit.acc)


@register_handler(AVAILABLE)
def available_smcog(term):
    """Generate query for available smCoG."""
    return db.session.query(distinct(Smcog.name), Smcog.description) \
             .filter(or_(Smcog.name.ilike('{}%'.format(term)), Smcog.description.ilike('%{}%'.format(term)))) \
             .order_by(Smcog.name)


@register_handler(AVAILABLE)
def available_resfam(term):
    """Generate query for available resfam profiles."""
    search = term + "%"
    return db.session.query(distinct(Resfam.name), Resfam.description) \
             .filter(or_(Resfam.accession.ilike(search),
                         Resfam.name.ilike(search),
                         Resfam.description.ilike("%" + search))
                     ) \
             .order_by(Resfam.name)


@register_handler(AVAILABLE)
def available_t2pksprofile(term):
    """Generate query for available T2PKS profile names/descriptions"""
    search = term + "%"
    return db.session.query(T2pksProfile.name, T2pksProfile.description) \
             .filter(or_(T2pksProfile.name.ilike(search), T2pksProfile.description.ilike("%" + search))) \
             .order_by(T2pksProfile.name)


@register_handler(AVAILABLE)
def available_t2pksproductclass(term):
    """Generate query for available T2PKS product classes"""
    search = "%{}%".format(term)
    return db.session.query(distinct(T2pksProductClass.product_class), null()) \
             .filter(T2pksProductClass.product_class.ilike(search)) \
             .order_by(T2pksProductClass.product_class)


@register_handler(AVAILABLE)
def available_t2pksstarter(term):
    """Generate query for available T2PKS starter units"""
    search = "%{}%".format(term)
    return db.session.query(distinct(T2pksStarter.name), null()) \
             .filter(T2pksStarter.name.ilike(search)) \
             .order_by(T2pksStarter.name)


@register_handler(AVAILABLE)
def available_goterm(term):
    """Generate query for available GO terms."""
    search = term + "%"
    return db.session.query(GeneOntology.identifier, GeneOntology.description) \
             .filter(or_(GeneOntology.identifier.ilike(search),
                         GeneOntology.description.ilike("%" + search))
                     ) \
             .order_by(GeneOntology.identifier)
