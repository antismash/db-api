""" Handlers for searching the database for specific module layouts

Module query terms are strings containing multiple components, e.g.
S=Condensation,Condensation_DCL|L=AMP-binding|M=oMT+cMT>nMT|T=?|F=0|O=*

Special characters are:
    |: separates different sections of the module (e.g. starter and loader)
    =: separates the type of a section from the query for that section
    ,: an OR operator
    +: an AND operator
    >: a THEN operator (effectively an AND with an extra requirement of ordering)
    *: "anything", a wildcard for zero or more domains
    ?: "something", a wildcard for one or more domains
    0: "nothing", no domain allowed (especially relevant for searches like trans-AT modules)

Section types:
    S: Starter
    L: Loader
    M: Modifications
    T: Transport
    F: Finalisation
    O: Other


Example breakdown:
    S=Condensation,Condensation_DCL -> Starter must be Condensation or Condensation_DCL
    L=AMP-binding                   -> Loader must be AMP-binding
    M=oMT+(cMT>nMT)                 -> Modifications must include an oMT, along with a cMT followed immediately by an nMT
    T=?                             -> Transport domain must exist (the "or more" portion isn't relevant to this section)
    F=0                             -> No finalisation domain may exist
    O=*                             -> Other domains can be missing or present


Any section not specified will default to *.

Aggregate domain labels exist and are used to cover multiple domains:
    C*: Any condensation domain (e.g. Condensation and Condensation_DCL)
    KS*: Any PKS_KS domain (e.g. iterative and modular)
    MT*: Any methyltransferase

"""


# TODO: complete/incomplete filtering
# TODO: multiple modules
# TODO: substrate search
# TODO: monomer search

from typing import List

from .helpers import InvalidQueryError

DEFAULT = ("*",)


def group_alternatives(content: List[str]) -> List[List[str]]:
    if not content:
        return []
    if len(content) == 1:
        return [content]
    if len(content) % 2 != 1:
        raise ValueError("invalid query: %s" % "".join(content))

    alternatives = []
    chunk = []
    i = 1
    while i < len(content):
        operator = content[i]
        if not chunk:
            chunk.append(content[i-1])
        if operator in [ModuleQuery.AND, ModuleQuery.THEN]:
            chunk.append(operator)
        elif operator == ModuleQuery.OR:
            alternatives.append(chunk)
            chunk = []
        else:
            raise ValueError("unknown operator: %s" % operator)
        chunk.append(content[i+1])
        i += 2

    if chunk:
        alternatives.append(chunk)

    return alternatives


class ModuleQuery:
    IGNORE = "*"
    NONE = "0"
    ANY = "?"

    AND = "+"
    THEN = ">"
    OR = ","

    def __init__(self) -> None:
        self.starter = None
        self.loader = None
        self.modification = None
        self.transport = None
        self.finalisation = None
        self.other = None

    def set_section(self, section: str, content: List[str]) -> None:
        content = group_alternatives(content)
        if section == "S":
            self.starter = content
        elif section == "L":
            self.loader = content
        elif section == "M":
            self.modification = content
        elif section == "T":
            self.transport = content
        elif section == "F":
            self.finalisation = content
        elif section == "O":
            self.other = content
        else:
            raise ValueError("unknown section: %s" % section)

    def __repr__(self):
        return "ModuleQuery(%s)" % self.__dict__

    def __iter__(self):
        if self.starter:
            yield "starter", self.starter
        if self.loader:
            yield "loader", self.loader
        if self.modification:
            yield "modification", self.modification
        if self.transport:
            yield "carrier_protein", self.transport  # matches database naming
        if self.finalisation:
            yield "finalisation", self.finalisation
        if self.other:
            yield "other", self.other


def _separate_sections(sections: str) -> List[str]:
    return sections.split("|")


def _separate_domains(domains: str) -> List[str]:
    result = []
    operators = ",+>"
    domain = []
    for char in domains:
        if char in operators:
            if not domain:
                raise ValueError("bad syntax in domain chunk: %s" % domains)
            result.append("".join(domain))
            result.append(char)
            domain = []
        else:
            domain.append(char)
    if not domain:
        raise ValueError("domain chunk must end with a domain, not an operator: %s" % domains)
    result.append("".join(domain))
    return result


def parse_module_query(query: str):
    if not query:
        raise InvalidQueryError("module query cannot be empty")
    if "+0" in query or "0+" in query:
        raise InvalidQueryError("incompatible combination: + and 0, will always be false")
    if "+?" in query or "?+" in query:
        raise InvalidQueryError("incompatible combination: + and ?, ? would be ignored")
    if "+*" in query or "*+" in query:
        raise InvalidQueryError("incompatible combination: + and *, * would be ignored")
    if ",*" in query or "*," in query:
        raise InvalidQueryError("incompatible combination: + and ',', * would be ignored")
    if ">*" in query or "*>" in query:
        raise InvalidQueryError("incompatible combination: > and *, * would be ignored")
    if ",?" in query or "?," in query:
        raise InvalidQueryError("incompatible combination: ? and ',', ? would be ignored")
    parsed = ModuleQuery()
    for section in _separate_sections(query):
        section_label, content = section.split("=")
        parsed.set_section(section_label, _separate_domains(content))
    return parsed
