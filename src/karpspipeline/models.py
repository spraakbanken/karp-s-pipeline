from pydantic import BaseModel, RootModel, field_validator

type Entry = dict[str, object]
type EntrySchema = dict[str, Field]


# TODO Does not need validation, transform to data class
class Field(BaseModel):
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


class PipelineConfig(BaseModel):
    class Config:
        extra = "forbid"

    resource_id: str
    name: MultiLang
    description: MultiLang | None = None
    # the elements of type object will be handled by the exporters models
    export: dict[str, object]
