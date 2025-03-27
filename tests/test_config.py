from karpspipeline import _merge_configs
from karpspipeline.common import Map
from karpspipeline.util import json


def test_merge_simple():
    conf1: Map = {"export": {"karps": {}}, "resource_id": "so2009"}
    conf2: Map = {"export": {"karps": {"lol": "lol"}}}

    newconf: Map = _merge_configs(conf1, conf2)
    assert newconf == {"export": {"karps": {"lol": "lol"}}, "resource_id": "so2009"}


def test_merge():
    conf1: Map = {"export": {"karps": {"lol": "lol"}}, "resource_id": "so2009"}
    conf2: Map = {"export": {"karps": {"will be": "saved"}}}

    newconf: Map = _merge_configs(conf1, conf2)

    json.dumps(newconf)

    assert newconf == {
        "export": {"karps": {"lol": "lol", "will be": "saved"}},
        "resource_id": "so2009",
    }


def test_merge_overwrite():
    conf1: Map = {"export": {"karps": {"will be": "overwritten"}}, "resource_id": "so2009"}
    conf2: Map = {"export": {"karps": {"will be": "saved"}}}

    newconf: Map = _merge_configs(conf1, conf2)

    json.dumps(newconf)

    assert newconf == {
        "export": {"karps": {"will be": "saved"}},
        "resource_id": "so2009",
    }
