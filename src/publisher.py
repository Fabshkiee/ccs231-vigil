# The thing we type into during the demo. Reads one event, sends it, prints
# "saved". If severity is 4 or 5 it also sends the same event to alert
# that decision belongs here, not in the broker, because the broker is only
# allowed to route by topic.
import argparse

from common.constants import SEV_MIN, SEV_MAX, ALERT_MIN_SEV
from common.log import warn, info

# roles, only two receives alert
ROLES = {
    "ParanormalReadings": ("readings", False),
    "SightingReports": ("sighting", True),
    "PoltergeistActivity": ("poltergeist", True),
}

SEV_WORD = "severity"

# allow only valid nodes to connect to system
def parse_args(argv=None):
    """Read --as <role> from the command line"""
    parser = argparse.ArgumentParser(description="Paranormal Office publisher")
    # dest="role" because 'as' is a Python keyword and can't be an attribute name
    parser.add_argument("--as", dest="role", required=True, choices=sorted(ROLES))
    return parser.parse_args(argv)

# extract text payload and severity integer
def parse_typed_line(raw):
    """Splits 'chanak sighting, severity 5' into ((kind, sev), None) or (None, reason)"""
    cleaned = raw.strip()
    # validate text
    idx = cleaned.lower().rfind(SEV_WORD) # take last severity instance
    if idx == -1:
        return None, "missing_severity"
    kind = cleaned[:idx].strip().rstrip(",").strip()
    if not kind:
        return None, "empty_text"
    sev_text = cleaned[idx + len(SEV_WORD):].strip()
    # validate severity
    if not (sev_text.isascii() and sev_text.isdigit()):
        return None, "bad_severity"
    sev = int(sev_text)
    if not SEV_MIN <= sev <= SEV_MAX:
        return None, "severity_out_of_range"
    return (kind, sev), None

# fan-out alert from valid roles only
def needs_alert(role, sev):
    """true when a second PUB on the 'alert' topic must be sent"""
    may_alert = ROLES[role][1]
    return may_alert and sev >= ALERT_MIN_SEV

# Prompt loop
def main(argv=None):
    """demo loop: validate typed lines and show what would be sent"""
    role = parse_args(argv).role
    topic = ROLES[role][0]
    info("pub", f"{role} -> topic '{topic}' (validation only, nothing is sent yet)")
    while True:
        try:
            raw = input(f"{role}> ")
        except (EOFError, KeyboardInterrupt):
            print()
            break
        parsed, reason = parse_typed_line(raw)
        if reason:
            # Validate before any socket, rejected line must cause zero traffic.
            warn("pub", reason, raw)
            continue
        kind, sev = parsed
        info("pub", f"would send {topic}: kind={kind!r} sev={sev} alert={needs_alert(role, sev)}")


if __name__ == "__main__":
    main()