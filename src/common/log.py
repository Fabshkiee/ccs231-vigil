# Little print helpers so every line says where it came from: [broker], [pub],
# [sub]. Saves you when five terminals are open during the demo and you can't
# tell which window is talking.
MAX_SHOWN = 80

def warn(prefix, reason, offending=""):
    shown = offending if isinstance(offending, str) else repr(offending)
    shown = shown[:MAX_SHOWN]
    tail = f" ({shown})" if shown else ""
    print(f"[{prefix}] warn: {reason}{tail}")

def info(prefix, message):
    """Print one info line."""
    print(f"[{prefix}] {message}")