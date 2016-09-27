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
    Compound,
    DnaSequence,
    Monomer,
    Profile,
    Taxa,
)

AVAILABLE = {}


def available_term_by_category(category, term):
    '''List all available terms by category'''
    cleaned_category = sanitise_string(category)
    cleaned_term = sanitise_string(term)

    if cleaned_category in AVAILABLE:
        query = AVAILABLE[cleaned_category](cleaned_term)
        return map(lambda x: {'val': x[0], 'desc': x[1]}, query.all())

    return []


@register_handler(AVAILABLE)
def available_superkingdom(term):
    '''Generate query for available superkingdoms'''
    return db.session.query(distinct(Taxa.superkingdom), null()).filter(Taxa.superkingdom.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_phylum(term):
    '''Generate query for available phyla'''
    return db.session.query(distinct(Taxa.phylum), null()).filter(Taxa.phylum.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_class(term):
    '''Generate query for available class'''
    return db.session.query(distinct(Taxa._class), null()).filter(Taxa._class.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_order(term):
    '''Generate query for available order'''
    return db.session.query(distinct(Taxa.taxonomic_order), null()).filter(Taxa.taxonomic_order.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_family(term):
    '''Generate query for available family'''
    return db.session.query(distinct(Taxa.family), null()).filter(Taxa.family.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_genus(term):
    '''Generate query for available genus'''
    return db.session.query(distinct(Taxa.genus), null()).filter(Taxa.genus.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_species(term):
    '''Generate query for available species'''
    return db.session.query(distinct(Taxa.species), null()).filter(Taxa.species.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_strain(term):
    '''Generate query for available strain'''
    return db.session.query(distinct(Taxa.strain), null()).filter(Taxa.strain.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_acc(term):
    '''Generate query for available accession'''
    return db.session.query(distinct(DnaSequence.acc), null()).filter(DnaSequence.acc.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_compoundseq(term):
    '''Generate query for available compound by peptide sequence'''
    return db.session.query(distinct(Compound.peptide_sequence), null()).filter(Compound.peptide_sequence.ilike('{}%'.format(term)))


@register_handler(AVAILABLE)
def available_monomer(term):
    '''Generate query for available monomer'''
    return db.session.query(distinct(Monomer.name), Monomer.description).filter(or_(Monomer.name.ilike('{}%'.format(term)), Monomer.description.ilike('{}%'.format(term))))


@register_handler(AVAILABLE)
def available_type(term):
    '''Generate query for available type'''
    return db.session.query(distinct(BgcType.term), BgcType.description).filter(or_(BgcType.term.ilike('{}%'.format(term)), BgcType.description.ilike('{}%'.format(term))))


@register_handler(AVAILABLE)
def available_profile(term):
    '''Generate query for available asDomain profile'''
    return db.session.query(distinct(Profile.name), Profile.description) \
             .filter(or_(Profile.name.ilike('{}%'.format(term)), Profile.description.ilike('%{}%'.format(term))))


@register_handler(AVAILABLE)
def available_asdomain(term):
    '''Generate query for available asDomain profile'''
    return db.session.query(distinct(AsDomainProfile.name), AsDomainProfile.description) \
             .filter(or_(AsDomainProfile.name.ilike('{}%'.format(term)), AsDomainProfile.description.ilike('%{}%'.format(term))))
