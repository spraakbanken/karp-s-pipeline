from karpspipeline.models import InferredField


PATH = "https://spraakbanken.gu.se/saolhist/bildfiler/"


def page_to_str(page, length=4):
    return str(page).rjust(length, "0")


# templates for creating links to facsimiles
templates = {
    "saol1": lambda page_num: f"SAOL01/SAOL01_{page_to_str(page_num)}.png",
    "saol6": lambda page_num: f"SAOL06/SAOL06_{page_to_str(page_num)}.png",
    "saol7": lambda page_num: f"SAOL07/SAOL07_{page_to_str(page_num)}.png",
    "saol8": lambda page_num: f"SAOL08/SAOL08_{page_to_str(page_num)}.png",
    "saol9": lambda page_num: f"SAOL09/SAOL09_{page_to_str(page_num)}.png",
    "saol10": lambda page_num: f"SAOL10/SAOL10_{page_to_str(page_num)}.png",
    "saol11": lambda page_num: f"SAOL11/SAOL11_{page_to_str(page_num)}.png",
    "saol12": lambda page_num: f"SAOL12/SAOL12_{page_to_str(page_num)}.png",
    "saol13": lambda page_num: f"SAOL13/SAOL13_{page_to_str(page_num)}.png",
    # 5 numbers here
    "saol14": lambda page_num: f"SAOL14/SAOL14_{page_to_str(page_num, length=5)}.png",
}


def create_link_update_schema(_) -> InferredField:
    return InferredField(type="text", extra={"length": 100})


# TODO type
def create_link(resource_id: str, entry):
    # TODO also use sidnr2
    return PATH + templates[resource_id](entry["sidnr1"])
