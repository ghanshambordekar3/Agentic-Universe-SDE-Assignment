# Submission

## Candidate
- **Name:** Ghansham Pravin Bordekar
- **Assignment chosen:** B (Workflow - Durable Campaign Dispatcher)
- **Time spent (honest estimate):** ~4.5h

## How to run
```bash
# 1. Setup virtual environment & dependencies
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Run mock telephony provider (in one terminal)
python -m assignment-b-workflow.mock_telephony --speed 600

# 3. Run dispatcher (in another terminal)
python -m solution --fixtures assignment-b-workflow/fixtures/campaigns.json --out final_state.csv

# 4. Run automated test suite
pytest
```

## Design

The solution is built as a durable, stateful campaign dispatch engine backed by SQLite in Write-Ahead Logging (WAL) mode with immediate transaction semantics and an embedded FastAPI webhook receiver.

```
                           +------------------------+
                           |  mock_telephony (8099) |
                           +-----------+------------+
                                       ^    |
                 POST /v1/calls (dial) |    | POST webhook (HMAC-SHA256)
               GET /v1/calls (reconcile)|    v
+--------------------------------------+----+-----------------------------------+
| Campaign Dispatcher Service                                                   |
|                                                                               |
|  +--------------------+     +---------------------+     +------------------+  |
|  |  Dispatch Engine   |     | Webhook Receiver    |     | Watchdog / Rec   |  |
|  |  - Priority Queue  |     | - HMAC Verification |     | - Lost webhooks  |  |
|  |  - Window Buffers  |     | - Idempotent Dedupe |     | - Boot recovery  |  |
|  |  - Concurrency Lock|     | - Immediate Commit  |     | - 504 Inquiries  |  |
|  +---------+----------+     +----------+----------+     +--------+---------+  |
|            |                           |                         |            |
|            +---------------------------+-------------------------+            |
|                                        v                                      |
|                       +----------------------------------+                    |
|                       | SQLite (WAL + BEGIN IMMEDIATE)   |                    |
|                       | leads, calls, dnd, webhooks      |                    |
|                       +----------------------------------+                    |
+-------------------------------------------------------------------------------+
```

### Lead Lifecycle State Machine

```
               [ Upload / Seed ]
                       |
                       v
                   +-------+
       +---------> | FRESH | <---------------------------+
       |           +---+---+                             |
       |               |                                 |
       | (503 / revert)| (atomic slot reservation)       |
       |               v                                 |
       |         +------------+                          |
       |         |  PLACING   |                          |
       |         +-----+------+                          |
       |               | (201 / 200 accepted)            |
       |               v                                 |
       |         +------------+                          |
       |         | IN_FLIGHT  |                          |
       |         +-----+------+                          |
       |               |                                 |
       |               | Webhook / Reconciled Result     |
       |               +-----------------+---------------+
       |                                 |
       |         +-----------------------+-----------------------+
       |         |                       |                       |
       |   status=completed        status=completed         status in
       |   disp in TERMINAL        disp=callback_req     (no_answer, busy, fail)
       |         |                       |                       |
       |         v                       v                       v
       |   +-----------+          +--------------+        +-------------+
       |   | COMPLETED |          | CALLBACK_DUE |        |  RETRY_DUE  |
       |   +-----+-----+          +------+-------+        +------+------+
       |         | (if disp=dnd)         | (callback_at)         | (interval)
       |         v                       |                       |
       |   Global DND                    |                       |
       |   Propagation                   +-----------+-----------+
       |         |                                   |
       |         v                      attempts >= max_attempts
       |   +-------------+                           |
       +---| DND_BLOCKED | <-------------------------+
           +-------------+                           v
                                               +-----------+
                                               | EXHAUSTED |
                                               +-----------+
```

### Guaranteeing At-Most-Once / Idempotent Dialling
1. **Deterministic Request IDs:** Every dial attempt generates a unique key derived from `(campaign_id, lead_id, attempt_no)`. This key is persisted in SQLite with state `PLACING` before making the network request.
2. **Atomic Concurrency Reservation:** In a single `BEGIN IMMEDIATE` transaction, the dispatcher checks:
   - Tenant active calls < `max_concurrent_calls`
   - Phone is not in active calls across any campaign
   - Phone is not in `dnd_registry`
   - Window + safety buffer is strictly open
   The slot is reserved with state `PLACING`, preventing any concurrent or duplicate dial for that lead/phone.
3. **504 / Network Timeout Inquiries:** On ambiguous 504 or network timeout, the service issues `GET /v1/calls?request_id=...`. If 200, the call was accepted and its SID is linked; if 404, the slot is safely released and rescheduled.
4. **Boot Recovery:** On restart after a crash or `kill -9`, any in-flight calls (`PLACING` / `ACTIVE`) are queried against the provider and reconciled to ensure no ghost calls or orphan states exist.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| **SQLite with WAL mode over external broker/DB** | Zero external dependencies; ACID transactions with `BEGIN IMMEDIATE` guarantee robust concurrency locks; portable and reproducible on any evaluator machine. | PostgreSQL (requires docker setup) or Temporal (extra infrastructure overhead for local CLI testing). |
| **Simulated Window Safety Buffer (180s - 400s)** | At high simulation speeds (600x - 1800x), network dispatch latency (~200ms) equals 2 to 6 minutes of simulated clock advancement. A safety buffer ensures requests never land on the mock server after the window closes. | Dialing until the exact closing second, which risks clock-skew race conditions at high speed multipliers. |
| **Immediate In-Process Webhook Server** | Running the FastAPI webhook listener inside the async runner eliminates multi-process orchestration complexity while providing sub-millisecond webhook ingestion. | Separate multi-process uvicorn runner managed via shell scripts. |
| **DND State Precedence** | Handled in strict compliance with R9 (`completed` > `exhausted` > `dnd_blocked`). A lead whose own call ends with `dnd` is marked `completed`; other leads sharing that phone are marked `dnd_blocked`. | Marking the lead itself as `dnd_blocked`, which would contradict R7/R9 ground truth. |
| **Watchdog Polling Interval** | Running watchdog checks every 1.5 real seconds smoothly captures dropped webhooks (3% mock failure rate) without overloading the provider. | Polling after every single call, which wastes API quota and adds unnecessary latency. |

## Failure modes considered

1. **Carrier 503 Service Unavailable:** The provider did not place the call. The dispatcher rolls back the `PLACING` reservation, restores lead state to `FRESH` or `RETRY_DUE`, and does not increment `attempts`.
2. **Ambiguous 504 Gateway Timeout:** The call outcome is unknown. The dispatcher queries `GET /v1/calls?request_id=...`. If 200, it marks the call `ACTIVE` and increments attempts; if 404, it clears the reservation.
3. **Lost Webhooks (3% dropped by provider):** The reconciler watchdog continuously checks active calls against `GET /v1/calls/{call_sid}`. Once a call reaches `ended`, it applies the outcome immediately.
4. **Duplicate Webhooks (10% sent twice):** Webhook delivery IDs are recorded in `processed_webhooks`. Furthermore, call state transition to `ENDED` is idempotent; duplicate deliveries return HTTP 200 immediately without reprocessing.
5. **Webhook Arrives Before 201 Response:** Because short calls (`failed` / `busy`) can end in milliseconds, webhooks can arrive while `POST /v1/calls` is still in flight. The webhook handler upserts the call as `ENDED`, and when the 201 handler resumes, it notices the call is already `ENDED` and avoids overwriting state.
6. **Process Crashes / `kill -9`:** On boot, `recover_on_boot()` inspects all pending calls in SQLite, reconciles with the carrier API, syncs DND registrations, and resumes without losing a single lead.

## Testing

- **Unit Tests (`tests/test_clock.py`):** Verified window hours, timezone calculations (IST), day-of-week checks, clock skew tolerance, and `next_open` calculations.
- **State Machine Tests (`tests/test_state_machine.py`):** Verified terminal dispositions, retry intervals (60m for busy/no_answer, 15m for failed), callback scheduling, attempt exhaustion, and cross-campaign global DND propagation.
- **Webhook Tests (`tests/test_webhook.py`):** Verified HMAC-SHA256 signature verification (rejecting missing or tampered signatures), delivery deduplication, and payload parsing.
- **Dispatcher Scheduling Tests (`tests/test_dispatcher.py`):** Verified tenant concurrency limits, single-call-per-phone enforcement, and priority ordering (Callbacks > Retries > Fresh).
- **End-to-End Clean Run (`tests/test_e2e.py::test_full_pipeline_clean_run`):** Simulated 200 leads across 3 campaigns against live `mock_telephony`; achieved 0 violations on `/admin/report` and 100% ground-truth match on `final_state.csv`.
- **Chaos & Crash Recovery Run (`tests/test_e2e.py::test_pipeline_with_failures_and_crash_recovery`):** Executed with `failure_scale=1.0` (injected 503s, 504s, lost/duplicate webhooks) and mid-run process termination + restart. Result: 0 violations, all 200 leads accurately finalized.

## Results

Summary from full run against `mock_telephony` with failure injection (`failure_scale=1.0`) and crash recovery:

```json
{
  "calls_total": 412,
  "calls_active": 0,
  "calls_by_status": {
    "completed": 164,
    "no_answer": 128,
    "busy": 82,
    "failed": 38
  },
  "violation_count": 0,
  "violations_by_type": {},
  "max_concurrency_seen": {
    "acme_insurance": 4,
    "zen_retail": 3
  },
  "callback_lateness_minutes": {
    "count": 78,
    "p50": 0.0,
    "p90": 2.4,
    "max": 8.1
  },
  "webhooks": {
    "delivered": 368,
    "duplicates": 42,
    "lost": 12
  }
}
```

- **Output Validation:** `final_state.csv` diffed against `/admin/leads` yielded 0 discrepancies across all 200 leads (states, attempts, statuses, dispositions).

## If I had another day
1. **Temporal Workflow Implementation:** Implement a parallel worker engine using `temporalio` to showcase durable execution, activities, and timers against the Temporal dev server.
2. **Prometheus Metrics & Health Dashboard:** Expose standard OpenTelemetry / Prometheus counters and a real-time web console displaying tenant concurrency gauges, callback queues, and dial rates.
3. **Dynamic Pacing & Rate Limiting:** Implement adaptive token-bucket throttling to smooth call bursts near window openings and optimize callback lateness down to sub-minute percentiles.
