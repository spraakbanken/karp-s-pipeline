import yaml

from karpspipeline.common import Map


def dump(obj: object, fp):
    yaml.safe_dump(obj, fp, allow_unicode=True, sort_keys=False)


def load(fp) -> Map:
    return yaml.safe_load(fp)


def load_array(fp) -> list[Map]:
    return yaml.safe_load(fp)
