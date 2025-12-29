from collections.abc import Mapping
from pathlib import Path
import re
from pydantic import BaseModel, ConfigDict, Field, RootModel, field_serializer, field_validator, computed_field

type Entry = Mapping[str, object]
type EntrySchema = dict[str, InferredField]


# TODO Does not need validation, transform to data class
class InferredField(BaseModel):
    type: str
    collection: bool = False
    extra: dict[str, object] = {}


def MultiLangMinLength(min_length: int = 1) -> type:
    class MultiLangInner(RootModel[str | dict[str, str]]):
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
                    raise ValueError(
                        f"label languages allowed: {','.join(lang_codes)}, found: {', '.join(invalid_keys)}"
                    )
                for item in value.values():
                    if len(item) < min_length:
                        raise ValueError(f"min. length: {min_length}")
            elif isinstance(value, str):
                if len(value) < min_length:
                    raise ValueError(f"min. length: {min_length}")
            return value

        @field_serializer("root", mode="plain")
        def dump(self, value: str | dict[str, str]) -> dict[str, str]:
            if isinstance(value, str):
                return {"swe": value, "eng": value}
            return value

    return MultiLangInner


class MultiLang(MultiLangMinLength(0)):
    pass


class NonEmptyMultiLang(MultiLangMinLength(1)):
    pass


class ConfiguredField(BaseModel):
    name: str
    type: str
    collection: bool = False
    label: MultiLang


CONVERTER_PATTERN = re.compile(
    r"^((['\"](?P<cited_name>([^:]+))['\"])|(?P<uncited_name>([^:\s]+)))"
    r"(?:\:(?P<converter>\w+(?:\.\w+)*))?"
    r"(?:\s+as\s+(?P<target>\w+))?$"
)

NOT_PATTERN = re.compile(r"^not\s(?P<name>\w+)")


class ExportFieldConfig(RootModel[str]):
    @field_validator("root")
    def validate_field_config(cls, value):
        if value == "..." or re.fullmatch(CONVERTER_PATTERN, value) or re.fullmatch(NOT_PATTERN, value):
            return value
        raise ValueError(f"wrongly formatted field str: {value}")

    @computed_field
    @property
    def exclude(self) -> bool:
        return bool(NOT_PATTERN.fullmatch(self.root))

    @computed_field
    @property
    def name(self) -> str:
        m = NOT_PATTERN.fullmatch(self.root)
        if m:
            return m.group("name")

        m = CONVERTER_PATTERN.fullmatch(self.root)
        if m:
            return m.group("cited_name") or m.group("uncited_name")
        raise ValueError("missing field name")

    @computed_field
    @property
    def converter(self) -> str | None:
        m = CONVERTER_PATTERN.fullmatch(self.root)
        return m.group("converter") if m else None

    @computed_field
    @property
    def target(self) -> str:
        m = CONVERTER_PATTERN.fullmatch(self.root)
        return m.group("target") if m else self.name


class ExportConfig(BaseModel):
    # default exporters
    default: list[str] = []
    fields: list[ExportFieldConfig] = []


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    resource_id: str
    name: MultiLang | None = None
    description: MultiLang | None = None
    # the elements of type object will be handled by the exporters models
    export: ExportConfig
    # default installers
    install: list[str] = []
    # the elements of type object will be handled by the importers models
    import_settings: Mapping[str, object] = Field(alias="import", default={})
    # main field list, master order and configuration, new fields may be added or aliased to existing fields
    # entry_word is not in this list and is always the first element, wether used directly or as alias
    fields: list[ConfiguredField]
    workdir: Path

    @property
    def modules(self) -> dict[str, object]:
        return self.__pydantic_extra__ or {}
