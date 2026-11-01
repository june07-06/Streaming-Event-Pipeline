# Streaming-Event-Pipeline
A Data Engineering project that transforms clickstream and application events into user sessions using event-time gaps, with support for out-of-order events, duplicate handling, data validation, late-arriving events, and reliable processing.
# Clickstream Sessionization Pipeline

A reproducible batch pipeline that validates clickstream CSVs, quarantines invalid or duplicate events, builds deterministic user sessions, and publishes results to DuckDB. The project includes synthetic sample data, expected answers, automated tests, reconciliation reports, seeded large-data generation, and performance evidence.

## Quick start

Requires Python 3.10 or newer. From the project root, install the dependencies and run the sample pipeline:

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# macOS/Linux:       source .venv/bin/activate
python -m pip install pandas numpy duckdb pytest nbformat
python -m pytest -q
python -m pipeline.run
python -m pipeline.report
```

The default run reads `data/raw/*.csv` and `data/ref/users.csv`, writes `output/warehouse.duckdb`, and prints a run summary. The report command runs reconciliation checks and prints quality and session summaries. `python -m pipeline.report` exits nonzero if a check fails.

To create a notebook and a fresh project ZIP from the checked-in files, run `python build_notebook.py --notebook sessionization_submission.ipynb --zip sessionization_project.zip`. The notebook is generated; it is not included in this source ZIP.

## Architecture

```text
CSV event batches + users reference
                 |
                 v
       ingest.py — read raw strings, attach file/line/run lineage
                 |
                 v
       validate.py — schema and business rules, duplicate handling
                 |                         |
          clean events                 rejected rows
                 |                         |
                 +-------> publish.py <----+
                           one DuckDB transaction
                                  |
             +--------------------+------------------+
             v                    v                  v
       events_clean           sessions       rejected_events
                                  |
                            run_log.py
                                  |
                       reports.py / report.py
```

`pipeline.run` coordinates the stages. `config.py` holds defaults and parameters; `sessionize.py` applies the session rule; `synth.py` and `generate.py` create reproducible datasets; `evidence.py` compares results and measures performance.

## Configuration and commands

| Option | Default | Purpose |
|---|---|---|
| `--raw-dir DIR` | `data/raw` | Read every `*.csv` in this folder, sorted by filename |
| `--users FILE` | `data/ref/users.csv` | User reference data |
| `--db FILE` | `output/warehouse.duckdb` | DuckDB target database |
| `--gap-minutes N` | `30` | Session boundary threshold |
| `--fail-at POINT` | none | Inject a failure at `after_sessions`, `after_events`, or `before_commit` to demonstrate rollback |

Example using a separate database:

```bash
python -m pipeline.run --raw-dir data/raw --users data/ref/users.csv --db output/sample.duckdb --gap-minutes 30
python -m pipeline.report --db output/sample.duckdb --gap-minutes 30
```

A run returns exit code 0 on success and 1 on failure. Each run receives a unique `run_id`; pass a `run_id` to `run_pipeline()` in Python when a fixed identifier is useful for a controlled job.

## Data dictionary

### Input files

| File | Required columns | Meaning |
|---|---|---|
| `data/raw/*.csv` | `event_id`, `user_id`, `event_ts`, `event_type`, `page_url` | One row per clickstream event. `page_url` may be blank. Timestamps are ISO-8601 strings. |
| `data/ref/users.csv` | `user_id` | Reference keys used to validate event users. The sample also has `signup_ts` and `country`; the pipeline requires `user_id`. Reference `user_id` values are expected to be unique. |

Ingestion preserves event fields as strings and adds `source_file`, physical `line_no` (header is line 1), UTC `ingested_at`, and `run_id`. Missing input columns or a missing users file fail the run. Extra event columns are ignored. A raw directory with no CSV files fails clearly; a header-only CSV is a valid empty batch.

### DuckDB output tables

| Table | Key / contents |
|---|---|
| `events_clean` | One accepted event per `event_id`, normalized UTC `event_ts`, source lineage, and assigned `session_id` |
| `sessions` | One row per session: `session_id`, `user_id`, `session_start`, `session_end`, `duration_seconds`, `event_count`, `entry_page`, `exit_page`, and `run_id` |
| `rejected_events` | Raw event values, `source_file`, `line_no`, `reason_code`, `reason_detail`, and `run_id` for rejected rows |
| `pipeline_runs` | `run_id`, start/end timestamps, status, input/clean/rejected/already-loaded counts, sessions/users affected, and `error_message` |

Tables are created without DuckDB primary-key constraints. The pipeline deduplicates event IDs and rebuilds affected users' sessions; reconciliation reports check event/session key uniqueness and event-to-session integrity.

## Validation and session rules

The validator assigns the first matching reason in this order: `MISSING_EVENT_ID`, `MISSING_USER_ID`, `MISSING_TIMESTAMP`, `INVALID_TIMESTAMP`, `FUTURE_TIMESTAMP`, `UNKNOWN_USER`, `INVALID_EVENT_TYPE`, then `DUPLICATE_EXACT` or `DUPLICATE_CONFLICT`. Blank timestamps are rejected, not replaced with ingestion time. Timestamps are normalized to UTC; timezone-naive values are interpreted as UTC. Events more than one day after ingestion are rejected. Allowed event types are `page_view`, `click`, `add_to_cart`, and `purchase` (case-sensitive).

For duplicate `event_id` values in the incoming batch, the earliest row by `(ingested_at, source_file, line_no)` is kept among rows that pass the other rules. Later rows are quarantined as exact or conflicting duplicates based on the event payload. If an event ID already exists in the database, the stored event wins and the incoming row is counted as `rows_already_loaded`.

Events are ordered by `(user_id, event_ts, event_id)`. A new session starts when the gap from the previous event is **greater than** 30 minutes: exactly 30:00 stays in the same session; 30:01 starts another. Sessions do not split at midnight or at batch boundaries; a one-event session has duration zero. Session IDs are the first 16 hexadecimal characters of SHA-256 over `user_id + "|" + session_start` in ISO format.

## Retry, restart, and late data

Publishing each batch is transactional. An injected or unexpected publish failure rolls back the data changes, records a `FAILED` run and its failure reason outside that transaction, then raises the error. Correct the cause and rerun the batch; a fresh run ID is generated automatically. Do not reuse an existing run ID.

Rerunning the same files against the same database does not add existing events: they are counted as already loaded. Rejects are replaced for each source filename so a corrected rerun can clear old rejects. Reconciliation is `rows_read = rows_clean + rows_rejected + rows_already_loaded`.

For late-arriving events, point `--raw-dir` at the late batch and reuse the same users file and database. The pipeline rebuilds sessions for affected users from all stored and new events in one transaction, allowing a late event to merge sessions or move a session start. Session IDs depend on session start, so a moved start changes that session ID. This is batch processing, not streaming; DuckDB supports one writer at a time, so concurrent writers are not handled.

## Reproducible sample and larger run

The hand-built sample in `data/raw`, `data/ref`, and `data/expected` is synthetic and has known expected sessions, reject rows, and run counts. Run it with the quick-start commands. For the included sample, expected results are 22 input rows, 13 clean events, 9 rejected rows covering all nine reason codes, and 6 sessions.

Generate a larger synthetic dataset with a fixed seed (default 42):

```bash
python -m pipeline.generate --events 100000 --users 2000 --seed 42 --out data/large
python -m pipeline.run --raw-dir data/large/raw --users data/large/ref/users.csv --db output/large.duckdb
python -m pipeline.run --raw-dir data/large/late --users data/large/ref/users.csv --db output/large.duckdb
python -m pipeline.report --db output/large.duckdb
```

The generator writes `data/large/manifest.json` and `expected_sessions.csv` alongside its input files. It injects duplicate exact/conflicting events, missing and malformed values, invalid event types, unknown users, out-of-order events, timestamp ties, session-gap boundaries, and a separate late batch. Expected-answer data is kept separate from raw input. The ZIP includes the manifest and previously captured reports; regenerate the dataset to create the full large input and expected-answer files.

## Correctness evidence and tests

`python -m pytest -q` runs module-level tests covering configuration, ingestion and file errors, validation and reason ordering, duplicate behavior, timestamp normalization/range, session boundaries and ordering, idempotent reruns, late events, transaction rollback and retry, reconciliation reports, seeded generation, and sample expected answers.

`python -m pipeline.report --db output/large.duckdb` reports ten checks. These include run-count reconciliation, event and session key uniqueness, event/session referential integrity, session aggregates, both directions of the gap rule, and known reject codes; each check reports its violation count, with zero meaning PASS. `pipeline.evidence.compare_sessions` compares produced sessions with the generator's expected sessions.

Recorded seed-42 evidence in the included reports: the 100,000-event scenario produced 100,000 clean events, 3,300 rejects, and 25,079 sessions matching the generator's expected answer. All ten reconciliation checks passed. Re-running both batches produced no additional rows and left the table fingerprints unchanged.

## Performance and trade-offs

The recorded 100,000-event run took about 4.5–5.8 seconds for the main batch and 1.3–1.8 seconds for the late batch in Google Colab; machine and run variation apply. Validation was the slowest measured stage. The measured optimization bulk-parses canonical UTC timestamps and sorts only repeated event IDs when classifying duplicates: validation improved from 2.276 seconds to 1.698 seconds (1.34x on the measured 101,300 input rows), with clean and rejected outputs asserted identical. Whole-run speedup is not claimed.

For late data, rebuilding every session for each affected user is simple and correct, but cost grows with those users' stored history. Session IDs can change when late data moves a session start. Validation and sessionization both sort clean events, retaining the current output ordering at the cost of one redundant sort. There is no concurrent-writer protection.

## Project contents

```text
pipeline/       ingestion, validation, sessionization, publishing, run log, reports, generators
 tests/         automated tests by module and behavior
 data/raw/      small synthetic event sample
 data/ref/      sample user reference
 data/expected/ sample expected rejects, sessions, and run summary
 data/large/    generator manifest and expected large-run evidence
 docs/          design and implementation decisions
 output/        captured sample and large-run reports
```
