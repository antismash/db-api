from api.search import helpers


def test_reverse_complement():
    seq = "ATGCCCTGA"
    rc_seq = "TCAGGGCAT"

    assert helpers.reverse_completement(seq) == rc_seq


def test_calculate_sequence():
    seq = "ATGCCCTGA"
    assert helpers.calculate_sequence('+', seq) == seq
    assert helpers.calculate_sequence('-', seq) == helpers.reverse_completement(seq)
