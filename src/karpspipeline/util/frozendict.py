from typing import Any, Iterator, Mapping


class frozendict(Mapping):
    """
    immutable dict that can be used in sets or other places where a "hashable" type is needed

    does not go through the inital values to look for immutable types,
    so hashing function will fail if self._data resolves to, for example,
    a recursive dict.
    """

    # name instance variables for memory reasons
    __slots__ = ["_data", "_hash"]

    def __init__(self, *args, **kwargs):
        self._data = dict(*args, **kwargs)
        self._hash: int | None = None

    def __hash__(self) -> int:
        """
        Cashes hash in object
        """
        h = self._hash
        if h is None:
            h = hash(frozenset(self._data.items()))
            self._hash = h
        return h

    def __getitem__(self, key: Any) -> Any:
        return self._data[key]

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def _immutable(self, *args, **kwargs):
        raise TypeError("frozendict is immutable")

    # block all mutation methods
    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable
