CCS231 QA and Fault Test Report
VIGIL Paranormal Publish Subscribe System


Report last updated: November 14, 2026
Code version / commit tested: 51e70e3.
Python version: Python 3.14.6
Operating system: Mac OS




PROTOCOL TESTING (DEVELOPER: NATHAN)
Files that will be tested: tests/test_protocol.py

P-1 (PROTOCOL TEST 1) Supported Messages Round Trip
    Expected: 
        Parsing a formatted message preserves its intended command and field values. Cover SUB, SUB_ACK, PUB, EVENT, ACK, GET_STATS, and error messages as applicable to the agreed parser interfaces. Include PUB_ACK only if the team has agreed its format.
    Input messages / values: 
        SUB: topic sighting, subscriber ID responseteam
        SUB_ACL: topic alert
        PUB_ACK: event ID e-8 
        GET_STATS: topic alert
        PUB: topic sighting, payload (“note”: :a|b”, “sev”: 5)
        EVENT: event ID x, topic alert, payload {}, created_at 1.0
        ACK: Topic sighting, subscriber ID responseteam, event ID e-8
        ERR: code 400, message empty topic
        Dispatcher: SUB with topic sighting, subscriber ID team
    Command to test used: 
	    Python 3 tests/test_protocol.py TestRoundTrip -v
    Observed results: 
	    All nine tests returned ok
    Evidence / log reference: 
	    test_ack (__main__.TestRoundTrip.test_ack) ... ok
        test_dispatcher_matches_direct_parser (__main__.TestRoundTrip.test_dispatcher_matches_direct_parser) ... ok
        test_err (__main__.TestRoundTrip.test_err) ... ok
        test_event (__main__.TestRoundTrip.test_event) ... ok
        test_get_stats (__main__.TestRoundTrip.test_get_stats) ... ok
        test_pub_ack (__main__.TestRoundTrip.test_pub_ack) ... ok
        test_pub_pipe_in_payload (__main__.TestRoundTrip.test_pub_pipe_in_payload) ... ok
        test_sub (__main__.TestRoundTrip.test_sub) ... ok
        test_sub_ack (__main__.TestRoundTrip.test_sub_ack) ... ok
        ----------------------------------------------------------------------
        Ran 9 tests in 0.000s
    Verdict: 
        9 of 9 tests PASSED

P-2 (PROTOCOL TEST 2) INVALID FIELD COUNTS
    Expected: 
        The parser rejects the malformed message with an error result; it does not repair missing fields or raise an uncaught exception. A pipe within a PUB JSON payload is not an extra protocol field.
    Input Values:
        PUB | sighting
        SUB 
    Command To Test used:
        python3 tests/test_protocol.py TestRejects.test_pub_rejects TestRejects.test_dispatcher_rejects -v
    Observed Results:
        Both tests returned ok. Both confirmed that the two missing-field messages returned Err with reason (bad_field_count) and close=False.
    Evidence / Log Reference: 
        pub_rejects TestRejects.test_dispatcher_rejects -v
        test_pub_rejects (__main__.TestRejects.test_pub_rejects) ... ok
        test_dispatcher_rejects (__main__.TestRejects.test_dispatcher_rejects) ... ok
        ----------------------------------------------------------------------
        Ran 2 tests in 0.000s

        OK
    
    Verdict: Partially Passed. Extra field cases and field count checks have not yet been tested.

P-3 (PROTOCOL TEST 3) Empty Topic
    (NO FILE YET- TEST)


P-4 (PROTOCOL TEST 4 ) INVALID PUB PAYLOAD
    Expected: 
        Rejected with an error results
    Input Values:
        {broken — malformed JSON.
        "str" — valid JSON string.
        [1] — valid JSON list.
        {"note":"a|b","sev":5} — valid JSON object.
    Command To Test used:
        python3 tests/test_protocol.py TestRejects.test_pub_rejects TestRoundTrip.test_pub_pipe_in_payload -v
    Observed Results:
        Both Tests Methods returned ok
    Evidence / Log Reference: 
        pub_rejects TestRoundTrip.test_pub_pipe_in_payload -v
        test_pub_rejects (__main__.TestRejects.test_pub_rejects) ... ok
        test_pub_pipe_in_payload (__main__.TestRoundTrip.test_pub_pipe_in_payload) ... ok
        ----------------------------------------------------------------------
        Ran 2 tests in 0.000s

        OK
    Verdict: 
        PASSED

P-5 (PROTOCOL TEST 5) Subscriber Identity Normalization
    Expected: 
        Strip surrounding whitespace, convert to lowercase, and collapse repeated whitespace.
    Input Values:
        "  Response   TEAM "
        Expected normalized ID: "response team".
    Command To Test used:
        python3 tests/test_protocol.py TestIds.test_messy_names_match -v
    Observed Results:
        The test method passed and returned ok, therefore it confirmed that "  Response   TEAM " was normalized to "response team".
    Evidence / Log Reference: 
        test_messy_names_match (__main__.TestIds.test_messy_names_match) ... ok
        ----------------------------------------------------------------------
        Ran 1 test in 0.000s
    Verdict: 
        PASS
P-6 (PROTOCOLO TEST 6) Line framing and message size limit
    Expected:
        Partial data is retained until LF. Complete lines are separated correctly. Input exceeding the limit returns error code 413 with a connection-close instruction.  
    Input Values:
        Partial line: b"SUB|sig" — 7 bytes.
        Multiple lines: b"A\nB\nC\n" — 6 bytes.
        Split message: b"SUB|sighting|" — 13 bytes, followed by b"team\n" — 5 bytes.
        Oversized input: 1,048,577 x bytes. Configured limit: 1,048,576 bytes.
    Command To Test used:
        python3 tests/test_protocol.py TestBuffering -v    
    Observed Results:
        All four tests returned ok. Meaning the partial line remianed bufferd with no complete lines or error. The two chunks produced the complete line b"SUB|sighting|team". The oversized input returned error code 413, reason line_too_long, and close=True. The multiple-line input returned [b"A", b"B", b"C"] with an empty remainder.
    Evidence / Log Reference: 
        test_half_line_kept (__main__.TestBuffering.test_half_line_kept) ... ok
        test_line_across_two_chunks (__main__.TestBuffering.test_line_across_two_chunks) ... ok
        test_oversize_closes (__main__.TestBuffering.test_oversize_closes) ... ok
        test_three_lines_one_chunk (__main__.TestBuffering.test_three_lines_one_chunk) ... ok
            ----------------------------------------------------------------------
        Ran 4 tests in 0.001s

        OK
    Verdict: 
        PASS for the four existing buffering checks. Overall P-07 remains incomplete pending broker integration and remaining coverage.


Protocol stage record
Date and code version: November 14, 2026 — commit 51e70e3.
Unit-test command actually run: python3 tests/test_protocol.py -v
Number of tests run / failures / errors: 
Output or evidence reference: 22 tests / 0 failures / 0 errors.
Checks still blocked or failing: 
No failures occurred in the existing suite. Remaining coverage gaps:
    P-01: Some round-trip tests check only selected command names and fields.
    P-02: Missing-field and extra-field coverage is incomplete.
    P-03: Empty and whitespace-only topic checks have not been run.
    P-07: A complete line followed by an unfinished remainder is not covered. Actual socket closure and connection usability checks await broker integration.
Stage verdict: 
Existing unit-test suite: PASS.
Overall protocol checklist: INCOMPLETE pending the remaining checks.

    


    

    
