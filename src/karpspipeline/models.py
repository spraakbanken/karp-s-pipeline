from pydantic import BaseModel

type Entry = dict[str, object]
type EntrySchema = dict[str, Field]


class Field(BaseModel):
    type: str
    collection: bool = False


class ResourceConfig(BaseModel):
    fields: EntrySchema


class PipelineConfig(BaseModel):
    resource_id: str
    label: str | None
    export: dict[str, object]
    resource_config: ResourceConfig | None = None
