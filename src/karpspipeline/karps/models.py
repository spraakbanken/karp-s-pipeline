from pydantic import BaseModel

from karpspipeline.models import MultiLang


class Tag(BaseModel):
    name: MultiLang
    description: MultiLang


class FieldMetadata(BaseModel):
    """
    Used to populate translations and value fields in the Karps backend configs
    """

    name: MultiLang
    description: MultiLang | None = None
    # values are used by enums to validate that the given values are in the set and also for translation
    values: dict[str, MultiLang] = {}


class KarpsConfig(BaseModel):
    output_config_dir: str
    db_database: str
    db_user: str
    db_password: str
    word: str
    word_description: MultiLang
    tags: list[str] = []
    tags_description: dict[str, Tag] = {}
    fields: dict[str, FieldMetadata]
    link: str
