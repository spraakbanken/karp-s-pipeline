from pydantic import BaseModel

from karpspipeline.models import MultiLang


class Tag(BaseModel):
    label: MultiLang
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
    # which field to use as the entry word
    word: str
    # description of the entry word field
    word_description: MultiLang
    # is the entry word field a collection or not
    word_collection: bool = False
    # tags that this resource belong to
    tags: list[str] = []
    # descrption of tags, probably set this in a parent config.yaml
    tags_description: dict[str, Tag] = {}
    # a link for this resource, maybe a home page or repository
    link: str
