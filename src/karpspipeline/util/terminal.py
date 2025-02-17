styles_map = {"plain": "0", "no": "2", "bright": "1", "normal": "22"}

ansi_esc = "\x1b"


def fmt(*styles: str) -> str:
    res = "".join([styles_map[style] for style in styles])
    return f"{ansi_esc}[{res}m"


def bold(text: str) -> str:
    return f"{fmt('bright')}{text}{fmt('normal')}"
