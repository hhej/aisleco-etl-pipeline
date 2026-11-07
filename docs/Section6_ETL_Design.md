# 6. ETL Design

This section describes how data moves from the CRM into the data warehouse of Section 5. The pipeline
is built and running: the code is in the `etl/` folder, the warehouse schema in
`WeddingOrganizer_DW_v7_dbdiagram.sql`, and `AisleCo_ETL_Pipeline_Demo.ipynb` runs every step shown
here on the Appendix B story and on a simulated business year.

## 6.1 Overview

The pipeline reads the CRM once a day, at 02:00 Bangkok time, and updates the warehouse. It works in
three stages:

| Stage | What happens | Where the data sits afterwards |
|---|---|---|
| Extract | Rows that changed since the last run are copied out of the CRM into CSV files | Landing folder, one per run (Bronze zone) |
| Stage | The files are checked, bad rows are set aside, the rest is merged into a cleaned copy of the CRM tables | Schema `stg` in the warehouse (Silver zone) |
| Transform and load | SQL builds each dimension and fact from `stg` and writes only the rows that changed | Schemas `dim`, `fact`, and the KPI views in `mart` (Gold zone) |

The source (database `aisleco_crm`) and the target (`aisleco_dw`) are separate databases. Nothing is
calculated inside the CRM. The transformations run inside the warehouse in SQL after the data has been
loaded into staging, so strictly the design is **ELT**. This was chosen because the warehouse already
has the join and date functions the rules need, and because keeping the transformations as SQL files
makes each business rule readable on its own.

Two schema changes support the pipeline:

- **OLTP v5** adds `UpdatedAt` to every table. It is set on insert by a default and on every update by a
  trigger, so the application cannot forget it. The nine tables the pipeline reads also get an index on
  `UpdatedAt`.
- **DW v7** keeps the star schema of DW v6, made leaner after review, and adds `stg` (nine staging
  tables), `etl` (seven control tables) and `mart` (eleven views: one base inquiry view and ten KPI views).
  The review changes:
  - **Ratios are not stored.** `UtilizationRate` (weekly workload) and `QuoteShareOfBudget` (inquiry)
    were removed from the facts, because a dashboard that sums or averages a stored ratio gives a
    wrong total. The facts keep the parts (`TotalAssignedHours` and `StandardWeeklyHours`;
    `QuotedAmount` and the new `BudgetMidpoint` in baht), and the `mart` views divide them.
  - **Outcome labels have their own dimension.** FactInquiry used to carry the CRM status, the
    furthest stage, who closed it, the lost reason, and the lost and open flags, which overlapped.
    `InquiryStatus`, `IsLost` and `IsOpen` were dropped. Outcome (Booked / Lost / Cancelled / Open),
    furthest stage, closed-by and lost reason moved to **DimInquiryOutcome**, a junk dimension of
    about 20 rows (one per combination). FactInquiry holds one `OutcomeKey`. Dashboard filters
    therefore show a fixed list of values, and a label is renamed in one place.

## 6.2 Extract

| Topic | Design |
|---|---|
| What is read | 9 of the 12 CRM tables, and only the columns a KPI needs. Customer, CustomerContact and CustomerChannelAccount are never read, and `CustomerID` is not read from Inquiry or Event, so no personal data leaves the CRM (Thai PDPA). |
| Incremental extraction | Each table has a **watermark** in `etl.Watermark`: the highest `UpdatedAt` already loaded. A run reads only rows with a later `UpdatedAt`. A full refresh (`--full-refresh`) reads everything. |
| Late commits | A row written by a slow transaction that commits after the previous snapshot can carry a timestamp up to 5 minutes before that snapshot. Each run therefore re-reads from 5 minutes before the **previous snapshot time** (`etl.Watermark.ExtractedAt`), never later than the watermark. Re-reading is harmless because the load step only writes rows that changed. Anchoring the overlap to the snapshot rather than the watermark matters when a backlog is drained: the watermark is then far behind the snapshot, and nothing is re-read, so each run stays within its cap. |
| Deleted rows | A timestamp cannot show a row that no longer exists, so every run also extracts the full list of primary keys (small and cheap). Staging rows whose key has disappeared are deleted. |
| Consistency | The nine tables are read in parallel by separate connections, but all of them use **one shared database snapshot** (`pg_export_snapshot`). A child row (e.g. an assignment) can never arrive without the version of its parent (the event) that it points to. |
| Load on the CRM | Bulk `COPY ... TO STDOUT` straight into CSV files, read through the `UpdatedAt` index. One extract of the ×10 test volume takes 0.07 s. |
| Batch cap (backlog queue) | `ETL_MAX_ROWS_PER_RUN` limits the changed transaction rows (inquiries, messages, events, assignments) one incremental run takes, oldest first, so a run stays inside the 2-minute target (6.7). Changes after the cutoff wait in the CRM behind the watermark: that backlog is the queue, and the next run continues from it. A CRM transaction is never split; a taken row pulls its parent rows into the same batch (so nothing is rejected as an orphan); the key file marks queued keys `Pending` so reconciliation does not count them as missing; and the watermark moves exactly to the cutoff, so every run makes progress. A run that leaves a backlog writes a warning alert with the queue size. Full refreshes are not capped. |
| Output | `data/landing/run_NNNNN/`: one CSV per table, one key file per table, and `manifest.json` with the row count, the highest `UpdatedAt` and a SHA-256 checksum per file. |

## 6.3 Staging and data quality

The stage step first checks every landed file against the manifest. A file whose checksum or row count
does not match stops the run before anything is written. The files are then bulk-loaded (`COPY`) into
landing tables, checked, and merged into `stg`. A staging row is written only when one of its values
changed and the incoming version is not older than the one already held. A replayed old landing
folder therefore cannot overwrite newer data. Every staging row carries the run that last wrote it
(`_RunID`), for lineage.

The data-quality rules are stored as data in `etl.DqRule` (27 rules), not hidden in code. Every check
of every run is recorded in `etl.DqResult`.

| Kind | When it is checked | Severity and effect | Examples |
|---|---|---|---|
| Row (16 rules) | Each extracted row, before staging | **Reject** (14): the row goes to the dead letter `etl.RejectedRow` with its full content and is not loaded. **Warn** (2): logged, row loaded. | A staff message without the staff member; an event without a date; hours outside 0–1000; an assignment whose event or employee is not in staging; a budget range that cannot be read (warn) |
| Set (4 rules) | The whole staging table, in the `dq_gate` step | **Block** (3): the run stops before any dimension or fact is touched. **Warn** (1). | No message or assignment may point to a missing parent; more than 5 % of one table's new rows rejected (a sign of an upstream change, not of one bad record) |
| Reconciliation (7 rules) | After the load, in the `reconcile` step | **Block** (6), **Warn** (1) | Every staged inquiry and event has exactly one fact row; staff hours per event equal the assignments; spreading hours over weeks neither creates nor loses hours; staging holds exactly the CRM's keys; frozen weeks that no longer match the CRM (warn) |

A rejected row is not lost. When someone corrects it in the CRM, its `UpdatedAt` moves, the next run
extracts it again, and the dead-letter row is marked with the run that finally loaded it. A rejected
parent takes its children with it: an assignment that points to a rejected event is rejected too, so
the hours never land in the warehouse without their wedding date.

## 6.4 Transform and load

The transformation rules are the ones defined in Sections 2.1 and 5 and are not repeated here. Each
target table has one SQL file in `sql/transform/`. The file builds the complete set of rows the table
should hold now (in schema `work`), and a generic load step then compares it with the table.

| Target | Transform (from `stg`) | Load strategy |
|---|---|---|
| DimDate | Calendar 2025–2028, high season, Buddhist Lent, auspicious level from AuspiciousDate | Upsert by DateKey; only changed flags rewritten |
| DimPackage, DimEmployee | Package price versions; employee + role | Type 1 upsert by OLTP ID; keys never change |
| DimInquiryOutcome + FactInquiry | Response time and first responder from ChatMessage (bot ignored), outcome and furthest stage, stage days, budget midpoint. New outcome combinations are added to DimInquiryOutcome in the same step (keys never change) | **Merge with change detection** by InquiryID: insert new, update only rows whose values changed, delete rows whose inquiry is gone |
| FactEventBooking | Staff count, planning / wedding-day / total hours per wedding | Merge with change detection by EventID |
| FactEmployeeWeeklyWorkload | Hours split (≤ 12 h wedding day, rest spread over the planning weeks), employee × week grid, overtime, active events (utilization is calculated in `mart.v_weekly_workload`) | Merge by employee and week **from the as-of week onwards only**. Weeks that have ended are inserted if missing and otherwise frozen (Section 5.4). |

Merging instead of truncating means surrogate keys stay the same from run to run, and each step reports
how many rows it inserted, updated, deleted or left unchanged. For the weekly fact the load also counts
rows of ended weeks whose **hours** no longer match the CRM: a late change, for example hours added to
an assignment after the week was over. Those rows stay frozen. The count is recorded every run, and a
warning is raised when it goes up. A contract change is not a late change: the old standard hours in
past weeks are the intended history and raise no warning.

Two guards protect the frozen history. A run whose as-of date is earlier than the last successful one
is refused, because it would reopen weeks that are already frozen. A failed run can be resumed only
while it is still the latest run.

## 6.5 Orchestration

The pipeline has 11 steps. Each does one job and runs in its own database transaction:

```
start_run → extract → stage → dq_gate → dim_date ─┬→ fact_inquiry → fact_event_booking ─┬→ reconcile → finish
                                        dim_package ┤                                      │
                                        dim_employee┴→ fact_weekly_workload ───────────────┘
```

The same steps can be run from the command line (`python -m etl run --asof 2027-02-01`) or by Apache
Airflow (DAG `aisleco_daily_etl`). Airflow adds the daily schedule (02:00 Bangkok), parallel execution
of independent steps, two retries with exponential backoff, at most one run at a time, a failure
callback for alerting, and a web view of every run and task log. A run can be replayed for a given
date or as a full rebuild by triggering the DAG with `{"asof": "2027-02-01", "full_refresh": true}`.

## 6.6 Error handling and recovery

| Situation | What the pipeline does |
|---|---|
| CRM or warehouse temporarily unreachable | Up to 4 connection attempts with exponential backoff (1, 2, 4 s); Airflow retries the task twice more |
| Source still down | The step fails, the run is marked failed, an error alert is written to `etl.Alert`. The watermark does not move, so the next run extracts the same window again. |
| A bad record | Rejected to the dead letter; the rest of the load continues; warning alert |
| A systematic data problem | The quality gate blocks the run before the warehouse changes |
| Damaged or partial landing file | Refused by the checksum check before staging |
| Crash in the middle of a step | The step's transaction is rolled back, so nothing is half-written. `etl.StepRun` records every attempt and is the **checkpoint**: `python -m etl resume RUN_ID` skips the steps that succeeded, reuses the landed files and finishes the run. |
| The same run executed twice | **Idempotent**: every write is an upsert or merge with change detection, so a second run with the same data writes nothing |
| More changes than one run may take | Batch cap: the oldest changes are loaded, the rest wait in the backlog queue and the next runs drain it (6.2); warning alert with the queue size |
| Two loads at the same time | Airflow allows one active run; every step also takes a database lock, so two steps never write the warehouse at the same time |

The watermarks move forward only in the last step, after the reconciliation has passed. A run that
fails anywhere therefore leaves the next run to pick up exactly where this one should have.

## 6.7 Testing

| Test (Chapter 6 category) | What was checked | Result |
|---|---|---|
| Transformation logic | 14 small hand-made cases: bot greeting ignored, staff message before the couple's first message ignored, only-bot reply = "Not logged", walk-in = "In person", quote share, unreadable budget, hours assigned in / after the wedding week, missing assignment date, ≤ 12 h assignments, cancelled events | All pass |
| End-to-end | Appendix B replayed through four incremental runs | Every Appendix B number reproduced (Beam 109 %, Arm 120 %, Fah 120 %, Beam 82 % / Fah 60 %, Nina 67 % on 36 h, response times 11 / 745 / 7 min, quote share 12.7 %) |
| Source-to-target | The simulated business year (619 inquiries, 2,652 messages, 345 assignments) through the pipeline, compared value by value with the facts the v3 notebook computes independently in pandas | 28,638 values, 0 differences |
| Incremental = full | Four incremental runs compared with one full rebuild of the same final data | Identical, except Nina's ended weeks, which correctly keep 44 h |
| Idempotency | A second run on unchanged data | 0 rows inserted, updated or deleted |
| Error handling | Source unavailable, bad row → dead letter → corrected, rejected parent with child, blocking gate, crash + resume, damaged landing file, resuming an old run, as-of date moving backwards | All behave as in 6.6 |
| Batch cap | A burst larger than the cap is queued and drained over several runs; a new child pulls its new parent into the batch | Each run takes at most the cap; no dead letters; the drained warehouse equals a full refresh |
| Performance: history | The simulated year copied up to 1,000 times (4.25 M CRM rows, about 1,000 years of Aisle & Co. at today's volume); one full and one small incremental run per size | Incremental run: 0.5 s at ×10, 9.3 s at ×100, **50.6 s at ×1000**, within the 2-minute target. Full (initial) load: 105 s at 2.1 M rows, 219 s at 4.25 M, so above about 2.4 M rows it is a one-off outside the target. Repeated runs vary by a few seconds (laptop) |
| Performance: burst | At ×100 history, up to 326,000 changes committed as 1,000-row CRM transactions, no cap | 10.1 s for 326,000 changes (≈ 32,000 changes/s) |
| Performance: overflow | The same 326,000 changes with the batch cap of 100,000 and the production 5-minute overlap | Drained in 4 runs of 5–8 s; queue 206 k → 106 k → 14 k → 0 |

The 33 automated tests are in `tests/` and run with `pytest`. The performance numbers come from
`scripts/benchmark.py --stress` on a laptop (results and chart in `docs/stress_test.txt` / `.png`).
**Capacity, as designed:** one run must finish within 2 minutes. Measured: an incremental run stays
within it up to at least 4.25 M CRM rows of history, and one run absorbs at least 326,000 changes in
about 10 s. Above that, the batch cap (100,000 changes per run) holds each run at about 7 s and the
excess waits in the backlog queue. A linear fit puts the limits near 10 M rows of history and 8 M
changes per run, but that is an extrapolation beyond what was measured.

## 6.8 Known limits

- The transform steps recompute every fact row from staging and then write only the changed ones, so
  a run's time grows with history (about 11 s per million CRM rows measured), not only with the changes. At
  80–120 weddings a year this takes well under a second. Near 10 M rows the transforms would need
  limiting to the inquiries and employees touched by the changed rows.
- The batch cap bounds the changes per run, not the history. A backlog drains one run at a time; with
  the daily schedule that means one batch a day, so after a large burst the DAG should be triggered
  again until the backlog alert clears.
- The 5-minute overlap assumes no CRM transaction stays open longer than 5 minutes.
- Late changes to weeks that have ended are reported but not applied. Correcting them needs a
  deliberate full rebuild, which also applies today's standard hours to past weeks (Section 5.4).
- Rejected rows wait in the dead letter until someone corrects them in the CRM. The daily alert list
  is the prompt to do so.
- Alerts are written to `etl.Alert` and the Airflow log. Sending them by e-mail or chat needs only a
  notifier in the Airflow failure callback.

---

### Appendix A, new row

| Revision | Changes |
|---|---|
| 10 → 11 | ETL pipeline built (Section 6). OLTP v5: `UpdatedAt` on every table, kept by a trigger, indexed on the 9 extracted tables. DW v7: DW v6 star schema without stored ratios (utilization and quote share are calculated in views) and with the inquiry outcome labels moved to the new DimInquiryOutcome junk dimension (InquiryStatus, IsLost, IsOpen dropped), plus staging (`stg`, 9 tables), ETL control (`etl`, 7 tables: Run, StepRun, Watermark, DqRule, DqResult, RejectedRow, Alert) and views (`mart`, 11 views: base inquiry view + 10 KPI views). Facts are merged with change detection instead of truncated, so surrogate keys are stable. |

### Correction to Appendix B.5

B.5 says Nina's week of 11 January stays at "24 h / 44". The freeze is right, but her hours that week
are **14 h** (12 wedding-day hours for Ploy and Ton's wedding on 16 January, plus 1 planning hour for
each of the two 14 February weddings), so the row reads 14 h / 44 = 32 %. Both the DW v6 SQL and the
pipeline give 14 h.
