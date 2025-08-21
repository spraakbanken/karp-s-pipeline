from collections.abc import Mapping
from pydantic import BaseModel, Field, RootModel, field_validator

type Entry = Mapping[str, object]
type EntrySchema = dict[str, InferredField]


# TODO Does not need validation, transform to data class
class InferredField(BaseModel):
    type: str
    collection: bool = False


# TODO Does not need validation, transform to data class
class FieldConfig(BaseModel):
    fields: EntrySchema


class MultiLang(RootModel[str | dict[str, str]]):
    """
    Model that represents labels that can be be either just the same for all languages:
    "SALDO"
    or multi lang:
    {"eng": "Word list", "swe": "Ordlista"}
    """

    @field_validator("root")
    def validate_label(cls, value):
        if isinstance(value, dict):
            lang_codes = ["swe", "eng"]
            invalid_keys = [key for key in value if key not in lang_codes]
            if invalid_keys:
                raise ValueError(f"label languages allowed: {','.join(lang_codes)}, found: {', '.join(invalid_keys)}")
        return value


class ConfiguredField(BaseModel):
    name: str
    type: str
    collection: bool = False
    label: MultiLang


class PipelineConfig(BaseModel):
    class Config:
        extra = "forbid"

    resource_id: str
    name: MultiLang
    description: MultiLang | None = None
    # the elements of type object will be handled by the exporters models
    export: dict[str, object]
    # the elements of type object will be handled by the importers models
    import_settings: Mapping[str, object] = Field(alias="import", default={})
    # main field list, master order and configuration, new fields may be added or aliased to existing fields
    # entry_word is not in this list and is always the first element, wether used directly or as alias
    fields: list[ConfiguredField]
