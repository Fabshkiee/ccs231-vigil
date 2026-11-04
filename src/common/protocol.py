# The wire format. One format_* and one parse_* per command, nothing else.
# No sockets and no files in here — text in, text out — so the tests can run
# without starting a broker. Whatever you add has to round-trip in
# tests/test_protocol.py, or it doesn't go on the wire.
