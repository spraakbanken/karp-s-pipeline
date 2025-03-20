import orjson

from pydantic import BaseModel

from karpspipeline.common import Map


def custom_serializer(obj: object) -> object:
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    raise TypeError(f"Type {type(obj)} not serializable")


def dumps(obj: object) -> str:
    return orjson.dumps(obj, default=custom_serializer).decode()


def loads(str: str) -> Map:
    return orjson.loads(str)
