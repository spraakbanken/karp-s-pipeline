"""
A lot of this is copied from Sparv and modified
"""

UD_FALLBACK = "X"


def saldo_to_ud(pos: str) -> str:
    # TODO memoize
    return suc_to_ud(saldo_to_suc(pos))


def saldo_to_suc(pos: str) -> str:
    return _saldo_pos_to_suc[pos]


def suc_to_ud(pos: str) -> str:
    """
    Convert SUC tags to UPOS.

    Args:
        pos: SUC tag.

    Returns:
        UPOS tag.
    """
    pos_dict = {
        "NN": "NOUN",
        "PM": "PROPN",
        "VB": "VERB",  # "AUX" ?
        "IE": "PART",
        "PC": "VERB",  # No ADJ?
        "PL": "PART",  # No ADV, ADP?
        "PN": "PRON",
        "PS": "DET",  # No PRON?
        "HP": "PRON",
        "HS": "DET",  # No PRON?
        "DT": "DET",
        "HD": "DET",
        "JJ": "ADJ",
        "AB": "ADV",
        "HA": "ADV",
        "KN": "CONJ",
        "SN": "SCONJ",
        "PP": "ADP",
        "RG": "NUM",
        "RO": "ADJ",  # ordinal numerals are adjectives
        "IN": "INTJ",
        "UO": "X",
        "MAD": "PUNCT",
        "MID": "PUNCT",
        "PAD": "PUNCT",
    }
    return pos_dict.get(pos.upper(), UD_FALLBACK)


def isof_to_ud(pos: str) -> str:
    """
    Convert isofs internal markup for POS into ud (experimental)
    """
    return _isof_nyord_to_ud.get(pos, UD_FALLBACK)


_saldo_pos_to_suc = {
    "nn": "NN",
    "av": "JJ",
    "vb": "VB",
    "pm": "PM",
    "ab": "AB",
    "in": "IN",
    "pp": "PP",
    "pn": "PN",
    "sn": "SN",
    "kn": "KN",
    "ie": "IE",
    "abh": "AB",
    "nnm": "NN",
    "nna": "NN",
    "avh": "JJ",
    "avm": "JJ",
    "ava": "JJ",
    "vbm": "VB",
    "pmm": "PM",
    "abm": "AB",
    "aba": "AB",
    "pnm": "PN",
    "inm": "IN",
    "ppm": "PP",
    "ppa": "PP",
    "knm": "KN",
    "kna": "KN",
    "snm": "SN",
    # nl and nlm in Saldo is numeral and since we do not now if it is ordinal (SUC:RO), use SUC:RG (cardinal)
    "nl": "RG",  # not RO
    "nlm": "RG",  # not RO
    "al": "DT",
    "pma": "PM",
}

_isof_nyord_to_ud = {
    # combined words (klimatbanta, klimatbantare) get X - unkown
    "substantiv": "NOUN",
    "substantiv, förkortning": "NOUN",
    "substantiv, namn/eponym, teleskopord": "NOUN",
    "substantiv, teleskopord": "NOUN",
    "substantiv, räkneord": "NOUN",
    "substantiv, fras/uttryck": "NOUN",
    "substantiv, namn/eponym": "NOUN",
    "namn/eponym, substantiv": "NOUN",
    # några av dessa passar nog bättre som PROPN (proper noun)
    "förkortning": "NOUN",
    "adjektiv": "ADJ",
    "adjektiv, teleskopord": "ADJ",
    # one of the words with this value cannot be PART, but both can be ADJ
    "adjektiv, förled/efterled": "ADJ",
    "fras/uttryck, adjektiv": "ADJ",
    "namn/eponym, adjektiv": "ADJ",
    "verb": "VERB",
    "verb, förkortning": "VERB",
    "verb, namn/eponym": "VERB",
    "verb, teleskopord": "VERB",
    "namn/eponym, verb": "VERB",
    "fras/uttryck, interjektion": "INTJ",
    "fras/uttryck, substantiv": "X",
    "substantiv, förled/efterled": "PART",
    "förled/efterled": "PART",
    "förled/efterled, substantiv": "PART",
    "pronomen": "PRON",
    "räkneord": "NUM",
    # usually multi-word expressions
    "fras/uttryck": "X",
    "fras/uttryck, substantiv, adjektiv": "X",
    "substantiv, verb": "X",
    "substantiv, verb, adjektiv": "X",
    "substantiv, verb, fras/uttryck": "X",
    "substantiv, adjektiv": "X",
    "adjektiv, substantiv": "X",
    "adjektiv, substantiv, förled/efterled": "X",
    "adjektiv, verb, substantiv": "X",
    "verb, adjektiv": "X",
    "verb, adjektiv, substantiv": "X",
    "verb, substantiv": "X",
    "verb, substantiv, adjektiv": "X",
    "övrigt": "X",
}
