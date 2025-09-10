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
