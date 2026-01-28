styles_map = {"plain": "0", "bright": "1", "redbg": "41", "greenbg": "42"}

ansi_esc = "\x1b"


def fmt(*styles: str) -> str:
    res = "".join([styles_map[style] for style in styles])
    return f"{ansi_esc}[{res}m"


def bold(text: str) -> str:
    return f"{fmt('bright')}{text}{fmt('plain')}"


def green_box() -> str:
    return color_box("green")


def red_box() -> str:
    return color_box("red")


def color_box(color) -> str:
    return f"{fmt(f'{color}bg')}  {fmt('plain')}"
