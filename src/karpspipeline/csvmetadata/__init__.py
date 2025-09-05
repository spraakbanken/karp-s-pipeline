import csv
from itertools import chain, tee
from typing import Iterable, cast

from karpspipeline.common import create_output_dir
from karpspipeline.models import Entry, PipelineConfig

__all__ = ["export"]


def _get_multi_lang_values(
    multi_lang_obj: None | dict[str, str] | str, fallback_translation: str | None = None
) -> tuple[str, str]:
    if multi_lang_obj:
        if isinstance(multi_lang_obj, str):
            swe_str = eng_str = multi_lang_obj
        else:
            swe_str = cast(dict[str, str], multi_lang_obj)["swe"]
            eng_str = cast(dict[str, str], multi_lang_obj)["eng"]
    else:
        if fallback_translation is not None:
            swe_str = eng_str = fallback_translation
        else:
            raise RuntimeError("missing name in resource")
    return swe_str, eng_str


def export(config: PipelineConfig, entries: Iterable[Entry], fields: list[dict[str, str]]):
    output_dir = create_output_dir()

    fields_data: list[list[object]] = [
        [
            "nyckel",
            "exempelvärden",
            "nyckelförslag",
            "svenska",
            "engelska",
            "fältordning",
            "sekundär information?",
            "kommentar",
        ]
    ]

    # TODO don't consume entries here if they might be needed later
    def get_example(field_name) -> object:
        def stringify(v):
            v = str(v)
            if len(v) > 40:
                return v[0:40] + "..."
            else:
                return v

        for entry in tee(entries, 1)[0]:
            val = entry.get(field_name)
            if val is not None:
                return stringify(val)

        raise RuntimeError(f"csvmetadata: no example for field '{field_name}' found")

    for idx, field in enumerate(fields):
        swe_label, eng_label = _get_multi_lang_values(field.get("label"), fallback_translation=field["name"])
        fields_data.append([field["name"], get_example(field["name"]), "", swe_label, eng_label, idx + 1, "", ""])

    desc_multi_lang_str = config.description.model_dump() if config.description else None
    swe_desc, eng_desc = _get_multi_lang_values(desc_multi_lang_str, fallback_translation="")

    swe_name, eng_name = _get_multi_lang_values(config.name.model_dump())

    karps_config = cast(dict[str, object], config.export["karps"])
    tags = ",".join(cast(dict[str, list[str]], karps_config).get("tags", []))
    entry_word = cast(dict[str, str | dict[str, str]], karps_config["entry_word"])
    word_desc_swe, word_desc_eng = _get_multi_lang_values(entry_word.get("description"))
    resource_data = [
        ["", "", "förslag", "kommentar"],
        ["maskinnamn", config.resource_id, "", ""],
        ["namn: svenska", swe_name, "", ""],
        ["namn: engelska", eng_name, "", ""],
        ["beskrivning: svenska", swe_desc, "", ""],
        ["beskrivning: engelska", eng_desc, "", ""],
        ["taggar/samlingar", tags, "", ""],
        ["entry_word", entry_word["field"], "", ""],
        ["entry_word beskrivning: svenska", word_desc_swe, "", ""],
        ["entry_word beskrivning: engelska", word_desc_eng, "", ""],
    ]

    with open(output_dir / f"{config.resource_id}_metadata.csv", "w") as csvfile:
        wr = csv.writer(csvfile)
        for row in chain(resource_data, [[]], fields_data):
            wr.writerow(row)
