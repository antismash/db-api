from api.location import Location, CompoundLocation
from api.search import helpers


def test_reverse_complement():
    seq = "ATGCCCTGA"
    rc_seq = "TCAGGGCAT"

    assert helpers.reverse_complement(seq) == rc_seq


def test_calculate_sequence():
    seq = "ATGCCCTGA"
    assert helpers.calculate_sequence(Location(0, len(seq), 1), seq) == seq
    assert helpers.calculate_sequence(Location(0, len(seq), -1), seq) == helpers.reverse_complement(seq)
    comp = CompoundLocation([Location(0, 3, 1), Location(6, 9, 1)])
    assert helpers.calculate_sequence(comp, seq) == "ATGTGA"
    comp.strand = -1
    assert helpers.calculate_sequence(comp, seq) == helpers.reverse_complement("ATGTGA")
