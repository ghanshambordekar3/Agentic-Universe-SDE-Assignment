# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a
skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used
- Antigravity AI Agent (powered by Gemini)

## What I used them for
- Scaffolding the initial SQLite database schema, indices, and connection management in WAL mode.
- Generating the test templates and initial unit tests for calling windows, clock simulation, and webhook signature verification.
- Drafting the ASCII state transition diagram and formatting the submission report.

## What I wrote or decided myself
- The core concurrency model: atomic slot reservation using SQLite `BEGIN IMMEDIATE` transactions to prevent double-dialing across concurrent async routines.
- The window close safety buffer: calculating the simulated clock advancement caused by network latency at 600x - 1800x simulation speeds and disallowing dials when the window is closing within seconds.
- The 504 ambiguous timeout recovery pattern: immediately probing `GET /v1/calls?request_id=...` to resolve whether an unknown request succeeded or failed.
- The out-of-order race condition handling when a webhook arrives before the provider's HTTP 201 response is parsed.
- The exact state machine precedence logic ensuring R9 compliance (`completed` > `exhausted` > `dnd_blocked`).

## Where the AI was wrong or unhelpful
- **Initial calling window check caused 422 violations under high simulation speeds:** The AI's initial implementation only verified whether `now` was inside the calling window at the instant the function ran. However, at `speed=1500`, a 200ms HTTP network dispatch latency meant that the simulated clock advanced by 5 minutes while the HTTP packet was in transit, landing on the mock server after the window had closed. I caught this during end-to-end integration testing and fixed it by introducing a dynamic safety buffer `max(180, speed * 0.4)` that prevents starting dials on the closing edge.
- **Starlette TestClient hanging on Windows:** The AI initially used FastAPI's `TestClient` in pytest, which hung on Windows due to an internal anyio thread portal conflict with the asyncio event loop. I diagnosed this and switched to testing with standard ASGI and direct loop execution.

## How I verified AI-generated code
- Automated test suites (`pytest`) covering 18 unit tests across `clock.py`, `dispatcher.py`, `state_machine.py`, and `webhook_server.py`.
- End-to-end integration runs against the live `mock_telephony` provider at varying simulation speeds (600x, 1500x, 1800x).
- Chaos verification with `--failure-scale 1.0` (injected 503s, 504s, lost webhooks, duplicate webhooks) and mid-run process termination + restart.
- Programmatic comparison of `final_state.csv` with the provider's `/admin/leads` ground truth, verifying 0 rule violations in `/admin/report`.
