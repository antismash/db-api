from typing import List


class Location:
    def __init__(self, start: int, end: int, strand=None, parts=None):
        self.start = start
        self.end = end
        self.strand = strand
        self.parts = parts or [self]


class CompoundLocation(Location):
    def __init__(self, parts: List[Location]):
        assert all(part.strand == parts[0].strand for part in parts), "mixed strands detected"
        super().__init__(parts[0].start, parts[-1].end, parts[0].strand, parts)


def location_from_string(data: str) -> Location:
    """ Converts a string, e.g. [<1:6](-), to a FeatureLocation or CompoundLocation
    """
    def parse_position(string: str) -> int:
        """ Converts a positiong from a string into a Position subclass """
        if string[0] == '<':
            return int(string[1:])
        if string[0] == '>':
            return int(string[1:])
        assert string != "UnknownPosition()"
        return int(string)

    def parse_single_location(string: str) -> Location:
        """ Converts a single location from a string to a FeatureLocation """
        start = parse_position(string[1:].split(':', 1)[0])  # [<1:6](-) -> <1
        end = parse_position(string.split(':', 1)[1].split(']', 1)[0])  # [<1:6](-) -> 6

        strand_text = string[-2]  # [<1:6](-) -> -
        if strand_text == '-':
            strand = -1  # type: Optional[int]
        elif strand_text == '+':
            strand = 1
        elif strand_text == '?':
            strand = 0
        elif '(' not in string:
            strand = None
        else:
            raise ValueError("Cannot identify strand in location: %s" % string)

        return Location(start, end, strand=strand)

    assert isinstance(data, str), "%s, %r" % (type(data), data)

    if '{' not in data:
        return parse_single_location(data)

    # otherwise it's a compound location
    # join{[1:6](+), [10:16](+)} -> ("join", "[1:6](+), [10:16](+)")
    operator, combined_location = data[:-1].split('{', 1)

    locations = [parse_single_location(part) for part in combined_location.split(', ')]
    return CompoundLocation(locations)
