# CCS231 QA and Fault Test Report

## VIGIL Paranormal Publish Subscribe System

**Report last updated:** November 24, 2026

**Code version / commit tested:** 5812d86.

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


### P-1 (PROTOCOL TEST 1) Supported Messages Round Trip Retest

**Expected:**

Parsing a formatted message preserves its command name and all field values.

**Input messages / values:**

- SUB: topic `sighting`, subscriber ID `responseteam`.
- SUB_ACK: topic `alert`.
- PUB_ACK: event ID `e-8`.
- GET_STATS: topic `alert`.
- PUB: topic `sighting`, payload `{"note":"a|b","sev":5}`.
- EVENT: event ID `x`, topic `alert`, payload `{}`, created_at `1.0`.
- ACK: topic `sighting`, subscriber ID `responseteam`, event ID `e-8`.
- ERR: code `400`, message `empty_topic`.
- Dispatcher: SUB with topic `sighting`, subscriber ID `team`.

**Command to test used:**

```bash
python3 tests/test_protocol.py TestRoundTrip -v
```

**Observed results:**

All nine tests returned `ok`. The updated assertions confirmed the command names and complete returned fields. The dispatcher comparison also passed.

**Evidence / log reference:**

```text
test_ack (__main__.TestRoundTrip.test_ack) ... ok
test_dispatcher_matches_direct_parser (__main__.TestRoundTrip.test_dispatcher_matches_direct_parser) ... ok
test_err (__main__.TestRoundTrip.test_err) ... ok
test_event (__main__.TestRoundTrip.test_event) ... ok
test_get_stats (__main__.TestRoundTrip.test_get_stats) ... ok
test_pub_ack (__main__.TestRoundTrip.test_pub_ack) ... ok
test_pub_pipe_in_payload (__main__.TestRoundTrip.test_pub_pipe_in_payload)... ok
test_sub (__main__.TestRoundTrip.test_sub) ... ok
test_sub_ack (__main__.TestRoundTrip.test_sub_ack) ... ok

----------------------------------------------------------------------
Ran 9 tests in 0.000s

OK
```

**Verdict:**

**PASSED 9/9.** The previously identified command-name and field-assertion coverage gap is resolved for the tested inputs.


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



### P-2 (PROTOCOL TEST 2) Invalid Field Counts Retest

**Expected:**

Messages with missing fields or forbidden extra fields return `Err` with reason `bad_field_count` and `close=False`. Invalid JSON containing an additional pipe in PUB or EVENT returns `bad_json`.

**Input messages / values:**

Missing-field and extra-field inputs:

- SUB: `SUB`, `SUB|a`, `SUB|a|b|c`.
- SUB_ACK: `SUB_ACK`, `SUB_ACK|a|b`.
- PUB_ACK: `PUB_ACK`, `PUB_ACK|a|b`.
- GET_STATS: `GET_STATS`, `GET_STATS|a|b`.
- ACK: `ACK`, `ACK|a`, `ACK|a|b`, `ACK|a|b|c|d`.
- PUB: `PUB`.
- EVENT: `EVENT`.
- ERR: `ERR`, `ERR|400`.

Invalid JSON inputs containing an additional pipe:

- `PUB|sighting|{}|x`
- `EVENT|{}|x`

**Command to test used:**

```bash
python3 tests/test_protocol.py TestRejects.test_missing_and_extra_fields TestRejects.test_extra_pipe_in_pub_and_event_is_payload -v
```

**Observed results:**

Both test methods returned `ok`. Their passing assertions confirmed that all 17 field-count inputs returned `Err` with reason `bad_field_count` and `close=False`.

The two invalid JSON inputs returned `Err` with reason `bad_json` and `close=False`. No uncaught exception occurred.

**Evidence / log reference:**

```text
test_missing_and_extra_fields (__main__.TestRejects.test_missing_and_extra_fields) ... ok
test_extra_pipe_in_pub_and_event_is_payload (__main__.TestRejects.test_extra_pipe_in_pub_and_event_is_payload) ... ok

----------------------------------------------------------------------
Ran 2 tests in 0.000s

OK
```

**Verdict:**

**PASSED 2/2 test methods.** All 19 input cases passed their assertions. The previously identified field-count coverage gap is resolved for the tested inputs.





### P-3 (PROTOCOL TEST 3) Empty Topic

**Expected:**

Empty and whitespace-only topics are rejected with `Err`, reason `empty_topic`, and `close=False`.

**Input messages / values:**

Each command was tested with an empty topic, spaces-only topic, and tab-only topic:

```text
SUB||team
SUB|   |team
SUB|\t|team

SUB_ACK|
SUB_ACK|   
SUB_ACK|\t

PUB||{}
PUB|   |{}
PUB|\t|{}

ACK||s|e
ACK|   |s|e
ACK|\t|s|e

GET_STATS|
GET_STATS|   
GET_STATS|\t
```

`\t` represents an actual tab character in the test inputs.

**Command to test used:**

```bash
python3 tests/test_protocol.py TestRejects.test_empty_and_whitespace_topic -v
```

**Observed results:**

The test method returned `ok`. Its passing assertions confirmed that all 15 inputs returned `Err` with reason `empty_topic` and `close=False`. No uncaught exception occurred.

**Evidence / log reference:**

```text
test_empty_and_whitespace_topic (__main__.TestRejects.test_empty_and_whitespace_topic) ... ok

----------------------------------------------------------------------
Ran 1 test in 0.000s

OK
```

**Verdict:**

**PASSED 1/1 test method.** All 15 input cases passed their assertions. The previously identified empty-topic coverage gap is resolved for the tested inputs.


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


### P-6 (PROTOCOL TEST 6) Line Framing and Message Size Limit Retest

**Expected:**

When one chunk contains a complete line followed by an unfinished line, the complete line is returned and the unfinished portion stays buffered. The next chunk completes the buffered message without losing data.

**Input messages / values:**

- Initial buffer: `b""`.
- First chunk: `b"SUB|a|b\nPUB|sig"`.
- Second chunk: `b"hting|{}\n"`.

**Command to test used:**

```bash
python3 tests/test_protocol.py TestBuffering.test_complete_line_then_unfinished_line_in_one_chunk -v
```

**Observed results:**

The test returned `ok`. Its passing assertions confirmed:

- After the first chunk, the returned lines were `[b"SUB|a|b"]`, the remainder was `b"PUB|sig"`, and the error was `None`.
- After the second chunk, the returned lines were `[b"PUB|sighting|{}"]`, the remainder was `b""`, and the error was `None`.

**Evidence / log reference:**

```text
test_complete_line_then_unfinished_line_in_one_chunk (__main__.TestBuffering.test_complete_line_then_unfinished_line_in_one_chunk) ... ok

----------------------------------------------------------------------
Ran 1 test in 0.000s

OK
```

**Verdict:**

**PASSED 1/1.** The complete-line-plus-unfinished-remainder coverage gap is resolved. Actual connection behavior remains pending broker integration.


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


## INTEGRATION TESTING (DEVELOPERS: JOHN MARK AND MJ)

**Files that will be tested:** `src/broker.py`, `src/publisher.py`, and `src/subscriber.py`


### I-1 (INTEGRATION TEST 1) Delivery to Matching Subscribers

**Expected:**

Both subscribers of `sighting` receive the published event. PublicShield, subscribed to `alert`, receives none during the observation window. The event is persisted, and routing follows the topic.

**Date and code version:**

November 23, 2026 — commit `3777526`..

**Input messages / values:**

Publisher role: `SightingReports`.

Typed input:

```text
QA fanout check 002, severity 2
```

Subscribers:

- ResponseTeam — ID `responseteam`, topics `sighting` and `poltergeist`.
- ResearchArchive — ID `researcharchive`, topics `readings`, `sighting`, and `poltergeist`.
- PublicShield — ID `publicshield`, topic `alert`.

**Command to test used:**

Broker startup from a temporary working folder:

```bash
project_dir="$PWD"
test_dir="$(mktemp -d -t vigil-i01)"
cd "$test_dir"
python3 "$project_dir/src/broker.py"
```

Each client was started from the project folder in a separate terminal:

```bash
python3 src/subscriber.py --as ResponseTeam
python3 src/subscriber.py --as ResearchArchive
python3 src/subscriber.py --as PublicShield
python3 src/publisher.py --as SightingReports
```

**Observation window:**

[Enter the actual duration observed after publishing. Enter five seconds only if measured or observed.]

**Observed results:**

Publisher:

```text
Event faf56ce9 saved (sighting)
```

ResponseTeam:

```text
Event faf56ce9 — sighting: QA fanout check 002, severity 2
```

ResearchArchive:

```text
Event faf56ce9 — sighting: QA fanout check 002, severity 2
```

PublicShield remained quiet during the reported check. Its last displayed subscription message was:

```text
[sub] subscribed to alert
```

**Persistence / routing evidence:**

The saved log contained the matching event:

```json
{
  "event_id": "faf56ce95a8c4285b99fcd2a163c3dd7",
  "topic": "sighting",
  "payload": {
    "kind": "QA fanout check 002",
    "sev": 2,
    "source": "SightingReports"
  },
  "created_at": 1795429263.975425
}
```

Actual log location:

```text
src/common/data/event_log.json
```

The configuration selected this project data folder despite the broker's temporary working directory.

Review of the supplied broker code shows `storage.append_event(event)` is called before subscriber delivery, and subscribers are selected using `subscriptions.get(topic, [])`. The terminal results demonstrate delivery to both matching subscribers. The saved log confirms persistence.

**Evidence / log reference:**

Publisher and subscriber terminal outputs, plus the matching event entry read from `event_log.json`.

**Verdict:**

Matching-subscriber delivery and persistence checks PASSED.

Complete the non-matching-subscriber check by recording the actual observation duration. Persistence-before-delivery and topic-based subscriber selection are supported by review of the supplied broker code.


## RECONNECT AND REPLAY TESTING (DEVELOPERS: JOHN MARK AND MJ)

**Files that will be tested:** `src/broker.py`, `src/subscriber.py`, `src/publisher.py`, and `src/common/storage.py`

### R-1 (RECONNECT TEST 1) Three Events Published While Subscriber Is Offline


**Expected:**

A subscriber reconnecting with the same ID receives its three missed events in log order. Its stored offset remains unchanged while offline and advances after it processes and acknowledges the replayed events.

**Date and code version:**

November 23, 2026 — commit `d666ef6`.

**Input messages / values:**

- Subscriber role: `ResponseTeam`.
- Subscriber ID: `responseteam`.
- Topic under test: `sighting`.
- Starting acknowledged offset: `1`.

Published while ResponseTeam was stopped:

```text
QA replay check 001, severity 2
QA replay check 002, severity 2
QA replay check 003, severity 2
```

**Command to test used:**

Broker:

```bash
python3 src/broker.py
```

Subscriber, stopped with Control + C before publishing and restarted afterward:

```bash
python3 src/subscriber.py --as ResponseTeam
```

Publisher:

```bash
python3 src/publisher.py --as SightingReports
```

Offset checks before disconnecting, while offline after publishing, and after replay:

```bash
PYTHONPATH=src python3 -c 'import json; from common.storage import load_offsets; print(json.dumps(load_offsets().get("responseteam", {}), indent=2))'
```

Saved event and log-position check:

```bash
PYTHONPATH=src python3 -c 'import json; from common.storage import load_events; events = load_events().get("sighting", []); print(json.dumps([{"index": i, **e} for i, e in enumerate(events) if e.get("payload", {}).get("kind", "") in ("QA replay check 001", "QA replay check 002", "QA replay check 003")], indent=2))'
```

**Observed results:**

The publisher confirmed all three events were saved while ResponseTeam was offline.

ResponseTeam's `sighting` offset remained at `1` after publication.

After restarting with the same subscriber ID, ResponseTeam printed all three missed events in the same order as the saved log:

- Index `2`: `8760c92679fe4ccdbcb95d40d12dcaa0` — QA replay check 001.
- Index `3`: `ca8646e9888141ffa387a982d0290ce1` — QA replay check 002.
- Index `4`: `2881c21e9fc44bb680d2b8582c3b05d5` — QA replay check 003.

Its stored `sighting` offset then advanced to `4`.

No duplicate prints or previously acknowledged events appeared in the supplied replay output. Duplicate suppression was not exercised by this run because no duplicate delivery was observed.

**Evidence / log reference:**

Publisher output:

```text
Event 8760c926 saved (sighting)
Event ca8646e9 saved (sighting)
Event 2881c21e saved (sighting)
```

Offset before disconnecting and while offline after publication:

```json
{
  "sighting": 1
}
```

ResponseTeam replay output:

```text
Event 8760c926 — sighting: QA replay check 001, severity 2
Event ca8646e9 — sighting: QA replay check 002, severity 2
Event 2881c21e — sighting: QA replay check 003, severity 2
[sub] subscribed to sighting, poltergeist
```

Offset after replay:

```json
{
  "sighting": 4
}
```

Saved events were verified through `load_events()`. Their full IDs and positions matched the replay order.

**Verdict:**

**PASS for the tested offline replay scenario.** All three missed events were recovered in log order, and the stored offset advanced from `1` to `4` after processing and acknowledgment.




## AUTOMATED FAULT TESTING (TEST ENGINEER: EARL)

**Files that will be tested:** `src/broker.py`, `src/subscriber.py`, and `src/common/storage.py`

**Test harness:** `src/tools/fault_inject.py`

**Command Pattern:**

```bash
python3 src/tools/fault_inject.py --case F-01
```

Replace the case argument with `F-02` through `F-08` for the other tests.

### F-1 (FAULT TEST 1) BROKER KILLED MID APPEND

**Expected:**

The committed log remains valid JSON and previously acknowledged events remain durable. Record whether the interrupted publish committed.

**Date and Code Version:**

November 24, 2026 — commit `9c8f9b`.

**Input Values:**

The initial log contained one acknowledged event:

- Event ID: `8451fcf34342483186cf582bef16fd17`
- Topic: `sighting`
- Payload: `{"kind":"F-01 baseline","sev":2,"source":"fault_inject"}`

**Command To Test Used:**

```bash
python3 src/tools/fault_inject.py --case F-01
```

**Port / Temporary Data Folder:**

Port `64135`. The harness used an automatically created temporary directory, which was removed after the test. Its path was not included in this run's output.

**Fault Injection Method:**

The harness performed the real `fsync()` on the temporary event log, paused before `os.replace()`, and signaled the parent process. The parent sent SIGKILL to the test broker.

**Observed Results:**

- The broker exited with code `-9`, confirming SIGKILL.
- The committed log remained valid JSON and byte-for-byte unchanged.
- The acknowledged baseline event survived.
- The interrupted second publish was absent from the committed log at this crash point.
- After restarting with the same data, the broker replayed the baseline event with unchanged fields.

**Evidence / Log Reference:**

```text
Baseline acknowledged: 8451fcf34342483186cf582bef16fd17
Crash checkpoint reached
Test broker killed with SIGKILL
Committed log is valid JSON
Acknowledged baseline event preserved
Replayed event: 8451fcf34342483186cf582bef16fd17
Baseline replay verified after broker restart
F-01 test processes stopped
F-01 result: PASS
```

Selected values from the JSON evidence:

```json
{
  "crashed_broker_exit_code": -9,
  "committed_log_valid": true,
  "committed_log_unchanged": true,
  "replay_matches_baseline": true,
  "broker_stopped": true,
  "temporary_directory_removed": true,
  "error": null
}
```

**Verdict:**

**PASS** for the tested crash point: after temporary-file fsync and before replacement.

**Issue and Owner, If Needed:**

None identified in this run.

**Teardown Result:**

The restarted broker stopped successfully, and the temporary directory was removed.

### F-2 (FAULT TEST 2) SUBSCRIBER KILLED BEFORE ACKNOWLEDGMENT

**Expected:**

The unacknowledged event is delivered again after reconnecting. Its stored offset remains unchanged before the crash and advances after the replay is acknowledged.

**Date and Code Version:**

November 24, 2026 — commit `9c8f9b`.

**Input Values:**

- Subscriber ID: `responseteam`
- Topic: `sighting`
- Target event ID: `77a1cf0cf8ef4f8983da55d79097a71d`
- Event description: `F-02 unacknowledged event`
- Severity: `2`

**Command To Test Used:**

```bash
python3 src/tools/fault_inject.py --case F-02
```

**Port / Temporary Data Folder:**

Port `50190`. The harness used an automatically created temporary directory, which was removed after the test. Its path was not included in this run's output.

**Fault Injection Method:**

The harness intercepted the subscriber's ACK-send operation and paused before transmitting it. After receiving the checkpoint signal, the parent killed the subscriber with SIGKILL. The recorded exit code was `-9`.

**Stored Offsets Before Kill and Reconnect:**

- Before delivery: `-1`.
- At the ACK checkpoint: `-1`.
- After the subscriber was killed, before reconnecting: `-1`.
- After acknowledging the replay: `0`.

**Observed Results:**

The subscriber initially printed:

```text
Event 77a1cf0c — sighting: F-02 unacknowledged event, severity 2
```

A harness client then reconnected using the same subscriber ID and topic. It received event `77a1cf0cf8ef4f8983da55d79097a71d` with fields identical to the saved event. Its ACK advanced the offset to `0`.

**Evidence / Log Reference:**

```text
Subscriber paused before ACK
Subscriber killed with SIGKILL
Unacknowledged event replayed: 77a1cf0cf8ef4f8983da55d79097a71d
Replay ACK advanced the offset to 0
F-02 test processes stopped
F-02 result: PASS
```

Selected values from the JSON evidence:

```json
{
  "offset_before_delivery": -1,
  "offset_at_ack_checkpoint": -1,
  "subscriber_exit_code": -9,
  "offset_after_subscriber_kill": -1,
  "offset_after_replay_ack": 0,
  "replay_matches_saved_event": true,
  "broker_stopped": true,
  "subscriber_stopped": true,
  "temporary_directory_removed": true,
  "error": null
}
```

**Verdict:**

**PASS.** The unacknowledged event replayed unchanged after reconnecting, and the offset advanced only after the replay ACK.

**Issue and Owner, If Needed:**

None identified in this run.

**Teardown Result:**

The subscriber was killed as intended, the broker stopped successfully, and the temporary directory was removed.

### F-3 (FAULT TEST 3) ACK LOST ON THE WIRE

**Expected:**

The unacknowledged event is delivered again. The subscriber retains its seen-ID set across reconnect, suppresses the duplicate display, and acknowledges the replay so its offset advances.

**Date and Code Version:**

November 24, 2026 — commit `9c8f9b`, assuming no code changes since F-2. Record any local changes separately.

**Input Values:**

- Subscriber ID: `responseteam`
- Topic: `sighting`
- Event ID: `399004ca11244b5993c69ddc973b5087`
- Event description: `F-03 lost ACK`
- Severity: `2`

**Command To Test Used:**

```bash
python3 src/tools/fault_inject.py --case F-03
```

**Port / Temporary Data Folder:**

Port `50273`. The harness used an automatically created temporary directory, which was removed after the test. Its path was not included in this run's output.

**Fault Injection Method:**

The harness intercepted the first ACK before socket transmission and suppressed it. After the parent verified the unchanged offset, the hook closed the connection and raised a `LinkError` to trigger reconnect.

**Subscriber Process Continuity:**

The subscriber handled the connection failure through its normal reconnect loop. It retried after one second using the same process and retained its in-memory seen-ID set.

**Observed Results:**

The captured delivery records contained two identical copies of the target event. The subscriber printed the event once:

```text
Event 399004ca — sighting: F-03 lost ACK, severity 2
```

The replayed copy was suppressed from display and acknowledged.

**Stored Offsets Before and After ACK:**

- Before delivery: `-1`.
- After the first ACK was suppressed: `-1`.
- After the replay ACK reached the broker: `0`.

**Evidence / Log Reference:**

```text
First ACK suppressed; offset remains -1
Replay acknowledged; offset advanced to 0
Same event received twice
Same event printed once
Duplicate suppression verified
F-03 test processes stopped
F-03 result: PASS
```

Selected values from the JSON evidence:

```json
{
  "offset_before_delivery": -1,
  "offset_after_dropped_ack": -1,
  "offset_after_replay_ack": 0,
  "delivery_count": 2,
  "print_count": 1,
  "subscriber_exit_code": 0,
  "subscriber_stopped": true,
  "broker_stopped": true,
  "temporary_directory_removed": true,
  "error": null
}
```

**Verdict:**

**PASS.** The event was delivered twice across reconnect, printed once, and successfully acknowledged on replay.

**Issue and Owner, If Needed:**

None identified in this run.

**Teardown Result:**

The subscriber exited with status `0`, the broker stopped successfully, and the temporary directory was removed.

### F-4 (FAULT TEST 4) TWO PUBLISHERS SAME TOPIC

**Expected:**

Subscriber delivery order matches the broker's committed log order. Messages remain complete and parseable, and acknowledged events are not lost.

**Date and Code Version:**

November 24, 2026 — commit `9c8f9b`. Record any local changes to the tested files separately.

**Input Values:**

- Topic: `sighting`.
- Subscriber ID: `f04-check`.
- Publishers: `publisher-a` and `publisher-b`.
- Concurrent events: 10 per publisher, 20 total.
- One additional warmup event established subscriber readiness.

Publisher event labels:

- Publisher A: `F-04 publisher-a event 0` through `F-04 publisher-a event 9`.
- Publisher B: `F-04 publisher-b event 0` through `F-04 publisher-b event 9`.

Each payload included severity `2`, its publisher name, and a note containing repeated `a|b` text.

**Command To Test Used:**

```bash
python3 src/tools/fault_inject.py --case F-04
```

**Port / Temporary Data Folder:**

Port `50355`. The harness used an automatically created temporary directory, which was removed after the test. Its path was not included in this run's output.

**Observed Broker Log Order:**

The 20 concurrent events appeared in the following order, shown using their eight-character ID prefixes:

```text
ce0728f1
7e813776
fc6bfff5
36e608d9
c95bd4a4
0a9e1807
58c0c5a1
5ee2b6ed
6357e7ec
cff9ad8a
e6a49414
d8a70dda
ff60fc81
ac4ef346
c11c446d
2078b297
e7990c85
50df53fb
dadc97d5
8a9f9545
```

The committed log was the ordering reference.

**Observed Results:**

The subscriber received all 20 events in exactly the same order as the committed log. The harness compared the complete received event objects with the stored entries.

All 20 acknowledged event IDs were unique, and all payloads matched their published values. No parsing errors occurred during the checks.

**Evidence / Log Reference:**

```text
Both publishers completed 10 events
All 20 events received and stored
Delivery order matches the saved log
All payloads preserved
F-04 test processes stopped
F-04 result: PASS
```

Selected values from the JSON evidence:

```json
{
  "events_per_publisher": 10,
  "test_port": 50355,
  "received_count": 20,
  "stored_concurrent_count": 20,
  "delivery_matches_log_order": true,
  "all_payloads_preserved": true,
  "broker_stopped": true,
  "temporary_directory_removed": true,
  "error": null
}
```

**Verdict:**

**PASS** for the tested 20-event concurrent run. All events arrived intact and in committed log order.

**Issue and Owner, If Needed:**

None identified in this run.

**Teardown Result:**

Publisher and subscriber sockets closed, the broker stopped successfully, and the temporary directory was removed.

### F-5 (FAULT TEST 5) BROKER RESTART WITH POPULATED LOG

**Expected:**

Committed events and offsets recover from disk. Pending events replay in order without skipping unread events.

**Date and Code Version:**

November 24, 2026 — commit `9c8f9b`. Record any local changes to the tested files separately.

**Input Values:**

- Subscriber ID: `f05-check`.
- Topic: `sighting`.

Events were saved in this order:

1. `58b60f6ec4104165a5658f58c904b124` — F-05 restart event 1.
2. `a971c2589cec4060885fe0a292bfb3e5` — F-05 restart event 2.
3. `affd0e9f67be4f5f829cf60ad8be4afa` — F-05 restart event 3.

Only the first event was acknowledged before the crash. The stored offset was `0`.

**Command To Test Used:**

```bash
python3 src/tools/fault_inject.py --case F-05
```

**Port / Temporary Data Folder:**

Port `50461`. The harness used an automatically created temporary directory, which was removed after the test. Its path was not included in this run's output.

**Stop / Restart Method:**

The harness sent SIGKILL to the broker after saving the events and offset. The recorded exit code was `-9`.

It then started a new broker process using the same port, event log, and offsets file.

**Observed Results:**

- The committed event log remained byte-for-byte unchanged across restart.
- The subscriber's recovered offset remained `0`.
- Reconnecting with the same subscriber ID replayed only these pending events, in order:

```text
a971c2589cec4060885fe0a292bfb3e5
affd0e9f67be4f5f829cf60ad8be4afa
```

Both replayed event objects matched their saved entries. The previously acknowledged first event was skipped. Replay ACKs advanced the offset through `1` to `2`.

**Evidence / Log Reference:**

```text
First event acknowledged; offset is 0
Broker killed with SIGKILL
Broker restarted with saved events and offsets
Only the two pending events replayed in order
Replay ACKs advanced the offset to 2
F-05 test processes stopped
F-05 result: PASS
```

Selected values from the JSON evidence:

```json
{
  "offset_before_crash": 0,
  "offsets_before_crash": {
    "f05-check": {
      "sighting": 0
    }
  },
  "crashed_broker_exit_code": -9,
  "offsets_after_restart": {
    "f05-check": {
      "sighting": 0
    }
  },
  "offset_after_replay_ack": 2,
  "event_log_preserved": true,
  "saved_offsets_preserved": true,
  "replay_matches_pending_events": true,
  "broker_stopped": true,
  "temporary_directory_removed": true,
  "error": null
}
```

**Verdict:**

**PASS.** Saved events and offsets survived the crash, and only the two pending events replayed in log order.

**Issue and Owner, If Needed:**

None identified in this run.

**Teardown Result:**

The restarted broker stopped successfully, test connections closed, and the temporary directory was removed.

### F-6 (FAULT TEST 6) ALL SUBSCRIBERS OFFLINE WITH KEEP-ALL RETENTION

**Expected:**

Under the agreed keep-all policy, all events remain stored while subscribers are offline. Each subscriber replays its unread events in log order using its own offset.

**Date and Code Version:**

November 24, 2026 — commit `9c8f9b`. Record any local changes to the tested files separately.

**Input Values:**

- `f06-slow`: topic `sighting`, acknowledged offset `0`.
- `f06-fast`: topic `sighting`, acknowledged offset `2`.

Five events were used:

1. `0fca483436c0409c84b9483e58abe689` — retained event 1.
2. `076f698ef3874413ac77b90782f9cfe0` — retained event 2.
3. `70522caf751b4a15ad015b23c0514fff` — retained event 3.
4. `53ad73a3077f43f782bf19acd65f0cf9` — retained event 4.
5. `fae2cbf34ec34b3191cda39d8f75feed` — retained event 5.

Events 4 and 5 were published while both subscribers were offline.

**Retention Policy:**

Keep all events in the log. The test exercised event retention and per-subscriber replay while both subscribers were offline.

**Prune Status:**

No pruning is implemented or attempted. The harness checked that publishing additional events preserved the existing log entries and offline subscriber offsets.

**Command To Test Used:**

```bash
python3 src/tools/fault_inject.py --case F-06
```

**Port / Temporary Data Folder:**

Port `50520`. The harness used an automatically created temporary directory, which was removed after the test. Its path was not included in this run's output.

**Observed Results:**

All five events remained stored in their original order.

After publishing the additional events, the stored offsets remained unchanged:

- `f06-slow`: `0`.
- `f06-fast`: `2`.

After reconnecting:

- `f06-slow` received events 2, 3, 4, and 5 in order.
- `f06-fast` received events 4 and 5 in order.
- Both replay sequences matched the complete saved event objects.
- Both subscribers' ACKs advanced their offsets to `4`.

**Evidence / Log Reference:**

```text
Slow subscriber offset: 0
Fast subscriber offset: 2
Both subscribers disconnected
All five events retained
Offline subscriber offsets unchanged
Slow subscriber recovered four unread events
Fast subscriber recovered two unread events
Both subscriber offsets advanced to 4
F-06 test processes stopped
F-06 result: PASS
```

Selected values from the JSON evidence:

```json
{
  "retention_policy": "Keep all events; no pruning",
  "offsets_before_offline_publish": {
    "f06-slow": {
      "sighting": 0
    },
    "f06-fast": {
      "sighting": 2
    }
  },
  "offsets_after_offline_publish": {
    "f06-slow": {
      "sighting": 0
    },
    "f06-fast": {
      "sighting": 2
    }
  },
  "final_offsets": {
    "f06-slow": {
      "sighting": 4
    },
    "f06-fast": {
      "sighting": 4
    }
  },
  "all_events_retained": true,
  "broker_stopped": true,
  "temporary_directory_removed": true,
  "error": null
}
```

**Verdict:**

**PASS** for offline retention and replay under the keep-all policy. Pruning safety was not exercised because pruning is not implemented.

**Issue and Owner, If Needed:**

None identified within the tested retention policy.

**Teardown Result:**

Test connections closed, the broker stopped successfully, and the temporary directory was removed.

### F-7 (FAULT TEST 7) DUPLICATE SUB WITH THE SAME SUBSCRIBER ID

**Expected:**

The new registration replaces the old connection and closes its socket. The replacement receives the event once and updates the subscriber's offset through ACK.

**Date and Code Version:**

November 24, 2026 — commit `9c8f9b`. Record any local changes to the tested files separately.

**Input Values:**

- Old connection ID: `f07-check`.
- New connection ID: `f07-check`.
- Normalized identity: `f07-check`.
- Topic: `sighting`.
- Published event ID: `704d8678277045189dc8f81e752ff889`.

Published payload:

```json
{
  "kind": "F-07 replacement delivery",
  "sev": 2,
  "source": "fault_inject"
}
```

**Command To Test Used:**

```bash
python3 src/tools/fault_inject.py --case F-07
```

**Port / Temporary Data Folder:**

Port `50577`. The harness used an automatically created temporary directory, which was removed after the test. Its path was not included in this run's output.

**Connection Details:**

Both clients connected to `127.0.0.1:50577`.

- Old subscriber used client port `50581`.
- Replacement subscriber used client port `50582`.

The old subscriber first received and acknowledged a warmup event, establishing stored offset `0`.

**Observed Results:**

The harness confirmed that the old socket closed after the second connection registered with the same ID.

The replacement received the published event with fields identical to its saved log entry. No extra data arrived during the following 0.3-second observation window.

**Observed Stored Offset:**

- Before replacement: `0`.
- After the replacement acknowledged the new event: `1`.

**Evidence / Log Reference:**

```text
Old subscriber registered and acknowledged warmup
Old subscriber connection closed
New subscriber received the published event once
Replacement ACK advanced the offset to 1
F-07 test processes stopped
F-07 result: PASS
```

Selected values from the JSON evidence:

```json
{
  "subscriber_id": "f07-check",
  "offset_before_replacement": 0,
  "old_socket_closed": true,
  "offset_after_replacement_ack": 1,
  "extra_data_observation_seconds": 0.3,
  "replacement_received_one_copy": true,
  "broker_stopped": true,
  "temporary_directory_removed": true,
  "error": null
}
```

**Verdict:**

**PASS** for the tested replacement scenario. The old socket closed, the replacement received one copy, and its ACK advanced the stored offset.

**Issue and Owner, If Needed:**

None identified in this run.

**Teardown Result:**

Test connections closed, the broker stopped successfully, and the temporary directory was removed.




### F-8 (FAULT TEST 8) CLEAN SHUTDOWN UNDER LOAD

**Expected:**

The broker stops accepting connections, attempts to drain in-flight sends within the configured 2-second budget, and exits with status `0`. Stored files remain valid, acknowledged publishes remain durable, and interrupted publishes are recorded separately.

**Date and Code Version:**

November 24, 2026 — commit `9c8f9b`. Record any local changes to the tested files separately.

**Input Values:**

- Two concurrent publishers: `publisher-a` and `publisher-b`.
- Subscriber: `responseteam`, receiving `sighting` events.
- One warmup event established subscriber readiness.
- Each publisher received PUB_ACK for five load events before shutdown.
- Both publishers were active when SIGINT was sent.
- Each publisher's next publish was interrupted during shutdown.

**Command To Test Used:**

```bash
python3 src/tools/fault_inject.py --case F-08
```

**Port / Temporary Data Folder:**

Port `50637`. The harness used an automatically created temporary directory, which was removed after the test. Its path was not included in this run's output.

**Shutdown Timing:**

The exact wall-clock time of SIGINT was not recorded.

Shutdown duration was measured using a monotonic clock:

- Configured budget: `2` seconds.
- Measured duration: `0.0056411249679513276` seconds, approximately `5.64` milliseconds.

**Observed Results:**

- The broker exited with status `0` within the configured budget.
- Two publishes were interrupted before receiving PUB_ACK:
  - `F-08 publisher-a event 5`.
  - `F-08 publisher-b event 5`.
- Both publisher errors were `ConnectionError: Broker closed before replying`.
- The subscriber detected a connection reset and entered its reconnect loop before test cleanup stopped it.
- Completion of every in-flight handler was not directly verified.
- Refusal of new connections during shutdown was not directly tested.

**File Validity and Acknowledged-Event Recovery:**

- The event log and offsets file remained valid JSON.
- All 10 load events acknowledged through PUB_ACK remained stored with their expected payloads.
- The acknowledged warmup event also remained stored, giving 11 stored events in total.
- No stored events lacked a corresponding PUB_ACK in the captured evidence.
- The subscriber's saved `sighting` offset was `9`, within the valid log range of `0` through `10`.

**Evidence / Log Reference:**

```text
Both publishers active; sending SIGINT
Broker exited with status 0
Shutdown duration: 0.0056411249679513276
All acknowledged events preserved
Saved log and offsets are valid JSON
Interrupted publishes: 2
F-08 test processes stopped
F-08 result: PASS
```

Selected values from the JSON evidence:

```json
{
  "shutdown_budget_seconds": 2,
  "active_publishers_at_sigint": 2,
  "shutdown_duration_seconds": 0.0056411249679513276,
  "broker_exit_code": 0,
  "publisher_a_acknowledged": 5,
  "publisher_b_acknowledged": 5,
  "subscriber_exit_code": 0,
  "saved_offsets": {
    "responseteam": {
      "sighting": 9
    }
  },
  "stored_event_count": 11,
  "acknowledged_events_preserved": true,
  "saved_files_valid_json": true,
  "stored_without_pub_ack": [],
  "broker_stopped": true,
  "subscriber_stopped": true,
  "temporary_directory_removed": true,
  "error": null
}
```

**Verdict:**

**PASS** for the implemented shutdown-timing, exit-status, file-validity, and acknowledged-publish durability checks.

Full in-flight handler draining and refusal of new connections during shutdown remain unverified.


**Teardown Result:**

The broker stopped, the subscriber exited with status `0`, publisher connections closed, and the temporary directory was removed.



## 7 FINAL VERIFICATION AND SUBMISSION RECORD

**Final Code Version:**

[INPUT — run `git rev-parse --short HEAD` after committing the final changes.]

**Clean-Clone Machine / Environment:**

Previous testing used macOS and Python 3.14.

Final clean-clone verification: NOT RUN.

**README Instructions Followed and Result:**

NOT RUN from a clean clone. Record the instructions followed and their actual results during final verification.

**Full-Suite Command:**

```bash
python3 -m unittest discover -s tests
```

**Actual Full-Suite Output / Test Counts:**

The previously recorded full-suite run completed successfully:

```text
Ran 102 tests in 0.908s

OK
```

- Tests run: 102.
- Failures: 0.
- Errors: 0.

This result predates the latest changes. Final verification against the submission version remains pending.

**Protocol Stage Verdict:**

The recorded protocol tests and coverage-gap retests passed. Confirm the completed protocol checklist against the final code version.

**Storage Stage Verdict:**

The previously recorded storage suite passed 16/16 tests.

Additional broker tests were supplied for storage-failure behavior and corrupt-log startup refusal, but their execution results have not yet been recorded.

Other outstanding storage checks must be reconciled with the storage stage record before declaring the entire stage PASS.

**Fan-Out Stage Verdict:**

Matching subscribers received the published sighting event. No corresponding delivery was observed on the subscriber registered for alert.

The final record must include the actual observation window and evidence for the persistence-before-delivery requirement.

**Reconnect Replay Stage Verdict:**

PASS for the recorded replay scenario. Three events published while the subscriber was offline replayed in order after reconnection. The stored offset advanced from `1` to `4`.

**Fault Cases Passed:**

7 / 8.

F-1, F-2, F-3, F-4, F-5, F-6, and F-7 passed their documented checks.

F-6 was tested under the agreed keep-all retention policy; no pruning was implemented or tested.

**Fault Cases Failed, Blocked, or Not Run:**

F-8: FAIL.

The expanded shutdown test confirmed that the listener closed and new connections were refused. However, the broker exited before a paused in-flight delivery could finish.

A broker fix and successful retest remain outstanding.

**Known Limitations and Evidence References:**

- F-1 covers one controlled crash point: after temporary-file fsync and before replacement.
- F-6 verifies keep-all retention and replay, not pruning safety.
- F-8 failed on November 24, 2026, at 08:09:40 +08:00, using test port `51337`.
- F-8 reported `ConnectionError: Broker exited before pending work finished`.
- Its evidence recorded `new_connection_refused: true` and `broker_alive_before_release: false`.
- The harness cleanup-reporting flag requires correction: `temporary_directory_removed: false` on the failure path does not establish that the directory remained.
- Final clean-clone verification and full-suite testing remain pending.

**Overall Conclusion Supported by the Results:**

Seven of eight automated fault cases passed their documented checks. F-8 identified an unresolved shutdown-draining issue.

The numerical fault-case target is met. Overall submission readiness remains pending confirmation of phases 1–4, final verification, and the remaining submission items.

**Fault Tolerance and Recovery Section Supplied for SDD:**

[INPUT — confirm whether the section was supplied and include the unresolved F-8 limitation.]

**Report Exported as fault-test-report.pdf and Checked:**

[INPUT — export the final report and verify its contents and layout.]

**Reviewer and Review Date:**

[INPUT — reviewer name, date, and review outcome.]

**Completion Target from the Implementation Stack:**

Phases 1–4 pass and at least 7 of 8 fault cases pass, with failures documented honestly.

Recorded fault-case outcome: **7 PASS / 1 FAIL**.

Overall completion is not yet confirmed.