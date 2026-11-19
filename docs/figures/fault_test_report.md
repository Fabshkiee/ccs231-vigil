# CCS231 QA and Fault Test Report

## VIGIL Paranormal Publish Subscribe System

**Report last updated:** November 14, 2026

**Code version / commit tested:** 51e70e3.

**Python version:** Python 3.14.6

**Operating system:** Mac OS

---

## PROTOCOL TESTING (DEVELOPER: NATHAN)

**Files that will be tested:** `tests/test_protocol.py`

### P-1 (PROTOCOL TEST 1) Supported Messages Round Trip

**Expected:**

Parsing a formatted message preserves its intended command and field values. Cover SUB, SUB_ACK, PUB, EVENT, ACK, GET_STATS, and error messages as applicable to the agreed parser interfaces. Include PUB_ACK only if the team has agreed its format.

**Input messages / values:**

- SUB: topic `sighting`, subscriber ID `responseteam`
- SUB_ACL: topic `alert`
- PUB_ACK: event ID `e-8`
- GET_STATS: topic `alert`
- PUB: topic `sighting`, payload `(“note”: :a|b”, “sev”: 5)`
- EVENT: event ID `x`, topic `alert`, payload `{}`, created_at `1.0`
- ACK: topic `sighting`, subscriber ID `responseteam`, event ID `e-8`
- ERR: code `400`, message `empty topic`
- Dispatcher: SUB with topic `sighting`, subscriber ID `team`

**Command to test used:**

```bash
Python 3 tests/test_protocol.py TestRoundTrip -v
```

**Observed results:**

All nine tests returned ok.

**Evidence / log reference:**

```text
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
```

**Verdict:**

9 of 9 tests PASSED.

### P-2 (PROTOCOL TEST 2) INVALID FIELD COUNTS

**Expected:**

The parser rejects the malformed message with an error result; it does not repair missing fields or raise an uncaught exception. A pipe within a PUB JSON payload is not an extra protocol field.

**Input Values:**

```text
PUB | sighting
SUB
```

**Command To Test used:**

```bash
python3 tests/test_protocol.py TestRejects.test_pub_rejects TestRejects.test_dispatcher_rejects -v
```

**Observed Results:**

Both tests returned ok. Both confirmed that the two missing-field messages returned `Err` with reason `bad_field_count` and `close=False`.

**Evidence / Log Reference:**

```text
pub_rejects TestRejects.test_dispatcher_rejects -v
test_pub_rejects (__main__.TestRejects.test_pub_rejects) ... ok
test_dispatcher_rejects (__main__.TestRejects.test_dispatcher_rejects) ... ok
----------------------------------------------------------------------
Ran 2 tests in 0.000s

OK
```

**Verdict:**

Partially Passed. Extra field cases and field count checks have not yet been tested.

### P-3 (PROTOCOL TEST 3) Empty Topic

(NO FILE YET- TEST)

### P-4 (PROTOCOL TEST 4) INVALID PUB PAYLOAD

**Expected:**

Rejected with an error result.

**Input Values:**

- `{broken` — malformed JSON.
- `"str"` — valid JSON string.
- `[1]` — valid JSON list.
- `{"note":"a|b","sev":5}` — valid JSON object.

**Command To Test used:**

```bash
python3 tests/test_protocol.py TestRejects.test_pub_rejects TestRoundTrip.test_pub_pipe_in_payload -v
```

**Observed Results:**

Both test methods returned ok.

**Evidence / Log Reference:**

```text
pub_rejects TestRoundTrip.test_pub_pipe_in_payload -v
test_pub_rejects (__main__.TestRejects.test_pub_rejects) ... ok
test_pub_pipe_in_payload (__main__.TestRoundTrip.test_pub_pipe_in_payload) ... ok
----------------------------------------------------------------------
Ran 2 tests in 0.000s

OK
```

**Verdict:**

PASSED.

### P-5 (PROTOCOL TEST 5) Subscriber Identity Normalization

**Expected:**

Strip surrounding whitespace, convert to lowercase, and collapse repeated whitespace.

**Input Values:**

```text
Input: "  Response   TEAM "
Expected normalized ID: "response team"
```

**Command To Test used:**

```bash
python3 tests/test_protocol.py TestIds.test_messy_names_match -v
```

**Observed Results:**

The test method passed and returned ok, confirming that `"  Response   TEAM "` was normalized to `"response team"`.

**Evidence / Log Reference:**

```text
test_messy_names_match (__main__.TestIds.test_messy_names_match) ... ok
----------------------------------------------------------------------
Ran 1 test in 0.000s
```

**Verdict:**

PASS.

### P-6 (PROTOCOLO TEST 6) Line Framing and Message Size Limit

**Expected:**

Partial data is retained until LF. Complete lines are separated correctly. Input exceeding the limit returns error code 413 with a connection-close instruction.

**Input Values:**

- Partial line: `b"SUB|sig"` — 7 bytes.
- Multiple lines: `b"A\nB\nC\n"` — 6 bytes.
- Split message: `b"SUB|sighting|"` — 13 bytes, followed by `b"team\n"` — 5 bytes.
- Oversized input: 1,048,577 x bytes.
- Configured limit: 1,048,576 bytes.

**Command To Test used:**

```bash
python3 tests/test_protocol.py TestBuffering -v
```

**Observed Results:**

All four tests returned ok:

- The partial line remained buffered with no complete lines or error.
- The two chunks produced the complete line `b"SUB|sighting|team"`.
- The oversized input returned error code `413`, reason `line_too_long`, and `close=True`.
- The multiple-line input returned `[b"A", b"B", b"C"]` with an empty remainder.

**Evidence / Log Reference:**

```text
test_half_line_kept (__main__.TestBuffering.test_half_line_kept) ... ok
test_line_across_two_chunks (__main__.TestBuffering.test_line_across_two_chunks) ... ok
test_oversize_closes (__main__.TestBuffering.test_oversize_closes) ... ok
test_three_lines_one_chunk (__main__.TestBuffering.test_three_lines_one_chunk) ... ok
----------------------------------------------------------------------
Ran 4 tests in 0.001s

OK
```

**Verdict:**

PASS for the four existing buffering checks. Overall P-07 remains incomplete pending broker integration and remaining coverage.

### Protocol Stage Record

**Date and code version:** November 14, 2026 — commit `51e70e3`.

**Unit-test command actually run:**

```bash
python3 tests/test_protocol.py -v
```

**Number of tests run / failures / errors:**

- Tests run: 22
- Failures: 0
- Errors: 0

**Checks still blocked or failing:**

No failures occurred in the existing suite. Remaining coverage gaps:

- **P-01:** Some round-trip tests check only selected command names and fields.
- **P-02:** Missing-field and extra-field coverage is incomplete.
- **P-03:** Empty and whitespace-only topic checks have not been run.
- **P-07:** A complete line followed by an unfinished remainder is not covered. Actual socket closure and connection usability checks await broker integration.

**Stage verdict:**

- Existing unit-test suite: **PASS**.
- Overall protocol checklist: **INCOMPLETE** pending the remaining checks.

---

## STORAGE TESTING (DEVELOPER: JOHN MARK)

**Files that will be tested:** `src/common/storage.py`

### S-1 (STORAGE TEST 1) SAVE AND RELOAD EVENTS

**Expected:**

The same accepted event IDs, topics, payloads, and order are recovered.

**Input Values:**

**Save Tests:**

- Event ID `12`, topic `sighting`, payload `{}`, created_at `2400`.
- Event ID `13`, topic `readings`, payload `{}`, created_at `1000`.

**Load Test:**

A log containing event ID `4be5feca1c964528a343661b336d8c09`, topic `sighting`, payload `{"text": "chants", "severity": 2}`, created_at `1730`, and an empty `alert` list.

**Command To Test used:**

```bash
PYTHONPATH=tests python3 -m unittest test_storage.TestAppendEvents.test_ae_to_new_file test_storage.TestAppendEvents.test_ae_to_existing_file test_storage.TestLoadEvents.test_load_valid_events_file -v
```

**Observed Results:**

All three tests returned ok:

- Saving to a new file produced `{'sighting":[event]}` with the expected event values.
- Appending events under different topics preserved both events and their values.
- Loading the prepared log returned data equal to the original prepared data.
- The append tests found no remaining `.tmp` files.

**Evidence / Log Reference:**

```text
test_ae_to_new_file (test_storage.TestAppendEvents.test_ae_to_new_file) ... ok
test_ae_to_existing_file (test_storage.TestAppendEvents.test_ae_to_existing_file) ... ok
test_load_valid_events_file (test_storage.TestLoadEvents.test_load_valid_events_file) ... ok

----------------------------------------------------------------------
Ran 3 tests in 0.002s

OK
```

**Verdict:**

Partially Passed. All three checks passed. Saving multiple events to the same topic, reloading them through `load_events()`, and verifying their order remains untested.

### S-2 (STORAGE TEST 2) Atomic Write and Storage Failure

**Expected:**

Uses the following sequence:

1. Write a temporary file.
2. Run `fsync(file)`.
3. Replace the original file.
4. Run `fsync(directory)`.

A failed save preserves previously committed data and reports the error.

**Input Values:**

The test first saved `["old_data"]`. It then mocked `os.replace()` to raise `OSError("Disk full")` while attempting to save `["new_data"]`.

**Command To Test used:**

```bash
PYTHONPATH=tests python3 -m unittest test_storage.TestAtomicWrite -v
```

**Observed Results:**

All four tests were successful. Their assertions confirmed that:

- Successful writes saved the expected JSON.
- A normal overwrite replaced `[1, 2, 3]` with `[4, 5, 6]`.
- The simulated replacement failure raised `OSError`.
- After that failure, the original file still contained `["old_data"]`.
- No `.tmp` files remained after the tested overwrite and failure.
- The mocked directory-open, `fsync`, and close functions were called.

**Evidence / Log Reference:**

```text
test_aw_failure_cleanup (test_storage.TestAtomicWrite.test_aw_failure_cleanup) ... ok
test_aw_overwrite (test_storage.TestAtomicWrite.test_aw_overwrite) ... ok
test_aw_success (test_storage.TestAtomicWrite.test_aw_success) ... ok
test_aw_unix_directory_sync (test_storage.TestAtomicWrite.test_aw_unix_directory_sync) ... ok

----------------------------------------------------------------------
Ran 4 tests in 0.003s

OK
```

**Verdict:**

Existing atomic-write tests all **PASS 4/4**.

Overall verification of S-2 remains **INCOMPLETE** pending directory-sync failure handling, sequence verification, and broker integration.

### S-3 (STORAGE TEST 3) CORRUPT STORE LOG

**Expected:**

Raises `StoreError` rather than silently returning an empty log. Once integrated, the broker refuses to start with the corrupt log.

**Input Values:**

The test wrote `[{bad json content or smth]` into a temporary file, then called `load_events()` on that file.

**Command To Test used:**

```bash
PYTHONPATH=tests python3 -m unittest test_storage.TestLoadEvents.test_load_corrupted_events_file -v
```

**Observed Results:**

The test returned ok. Its `assertRaises(StoreError)` assertion confirmed that loading the corrupt file raised `StoreError`.

**Evidence / Log Reference:**

```text
test_load_corrupted_events_file (test_storage.TestLoadEvents.test_load_corrupted_events_file) ... ok

----------------------------------------------------------------------
Ran 1 test in 0.001s

OK
```

**Verdict:**

PASS for the storage-function check. Broker startup verification remains **BLOCKED** until integration.

### S-4 (STORAGE TEST 4) ACK OFFSETS AND PRUNING

**Expected:**

Offsets remain independent and recover correctly. If pruning shifts indices, stored positions remain correct and never skip an unread event.

**Input Values:**

No event logs were used in this test.

Offset values included:

- `researcharchive`:
  - `sighting=8`
  - `alert=2`
- `responseteam`:
  - `sighting=7`
  - `alert=1`

An update advanced `researcharchive`’s `sighting` offset to `15`, while preserving its `alert` offset at `2`.

**Prune Status:**

No pruning implementation is present in the supplied `storage.py`. Retention policy awaits team confirmation. Pruning was not tested.

**Command To Test used:**

```bash
PYTHONPATH=tests python3 -m unittest test_storage.TestSaveOffset test_storage.TestLoadOffsets -v
```

**Observed Results:**

All six tests returned ok. The assertions confirmed that:

- Saving preserved independent subscriber and topic entries.
- Updating one topic preserved the other topic’s offset.
- Loading a prepared offsets file returned the expected values.
- Missing or corrupt offsets files returned `{}`.
- A mocked storage failure raised `OSError` and left no `.tmp` files.

**Evidence / Log Reference:**

```text
test_so_to_existing_file (test_storage.TestSaveOffset.test_so_to_existing_file) ... ok
test_so_to_new_file (test_storage.TestSaveOffset.test_so_to_new_file) ... ok
test_so_when_disk_full (test_storage.TestSaveOffset.test_so_when_disk_full) ... ok
test_load_corrupted_offsets_file (test_storage.TestLoadOffsets.test_load_corrupted_offsets_file) ... ok
test_load_missing_offsets_file (test_storage.TestLoadOffsets.test_load_missing_offsets_file) ... ok
test_load_valid_offsets_file (test_storage.TestLoadOffsets. test_load_valid_offsets_file) ... ok

----------------------------------------------------------------------
Ran 6 tests in 0.003s

OK
```

**Verdict:**

Existing offset tests **PASS 6/6**. Combined save/reload verification remains untested. Pruning verification awaits confirmation of the retention policy and implementation.

### Storage Stage Record

**Date and code version:** November 15, 2026 — commit `728dfcc`.

**Unit-test command actually run:**

```bash
PYTHONPATH=tests python3 -m unittest test_storage -v
```

**Number of tests run / failures / errors:**

- Tests run: 16
- Failures: 0
- Errors: 0

**Output or evidence reference:**

```text
test_ae_to_existing_file (test_storage.TestAppendEvents.test_ae_to_existing_file) ... ok
test_ae_to_new_file (test_storage.TestAppendEvents.test_ae_to_new_file) ... ok
test_ae_when_disk_full (test_storage.TestAppendEvents.test_ae_when_disk_full) ... ok
test_aw_failure_cleanup (test_storage.TestAtomicWrite.test_aw_failure_cleanup) ... ok
test_aw_overwrite (test_storage.TestAtomicWrite.test_aw_overwrite) ... ok
test_aw_success (test_storage.TestAtomicWrite.test_aw_success) ... ok
test_aw_unix_directory_sync (test_storage.TestAtomicWrite.test_aw_unix_directory_sync) ... ok
test_load_corrupted_events_file (test_storage.TestLoadEvents.test_load_corrupted_events_file) ... ok
test_load_missing_events_file (test_storage.TestLoadEvents.test_load_missing_events_file) ... ok
test_load_valid_events_file (test_storage.TestLoadEvents.test_load_valid_events_file) ... ok
test_load_corrupted_offsets_file (test_storage.TestLoadOffsets.test_load_corrupted_offsets_file) ... ok
test_load_missing_offsets_file (test_storage.TestLoadOffsets.test_load_missing_offsets_file) ... ok
test_load_valid_offsets_file (test_storage.TestLoadOffsets.test_load_valid_offsets_file) ... ok
test_so_to_existing_file (test_storage.TestSaveOffset.      test_so_to_existing_file) ... ok
test_so_to_new_file (test_storage.TestSaveOffset.test_so_to_new_file) ... ok
test_so_when_disk_full (test_storage.TestSaveOffset.test_so_when_disk_full) ... ok

----------------------------------------------------------------------
Ran 16 tests in 0.007s

OK
```

**Checks still blocked or failing:**

No failures occurred in the existing code. Remaining gaps and questions:

- **S-01:** Save multiple events to the same topic, reload them using `load_events()`, and verify their values and order.
- **S-02:** Verify the atomic-write call order and test directory-sync failures. The supplied implementation silently ignores directory-sync errors.
- **S-02 — Broker integration:** Verify that storage failure produces error `503`, no fan-out, and no `PUB_ACK`.
- **S-03 — Broker integration:** Verify that the broker refuses to start with a corrupt event log.
- **S-04:** Save offsets using `save_offset()`, then reload the same data using `load_offsets()`.
- **S-04:** Confirm the retention policy and whether pruning will be implemented.

**Stage verdict:**

Overall unit-test suite: **PASSED 16/16**.

Overall storage checklist: **INCOMPLETE**, with outstanding checks and integration verification.
````