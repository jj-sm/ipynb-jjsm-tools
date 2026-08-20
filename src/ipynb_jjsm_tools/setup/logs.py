def info_log(type: str, msg: str) -> None:
    """"
    Print a colored log message to the console.
    """
    LEVELS = {
        "error": ("\033[91m", "ERRO"),
        "warning": ("\033[93m", "WARN"),
        "info": ("\033[94m", "INFO"),
        "ok": ("\033[92m", " OK "),
        "done": ("\033[92m", "DONE"),
    }
    RESET = "\033[0m"

    type_key = type.lower()
    if type_key not in LEVELS:
        raise ValueError(f"Unknown log type '{type}'. Use one of {list(LEVELS)}")

    color, tag = LEVELS[type_key]
    print(f"{color}[{tag}]{RESET}: {msg}")