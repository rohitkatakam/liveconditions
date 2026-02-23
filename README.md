# Lotus Health — FHIR Condition MCP Server

A CQRS-inspired event-sourcing system that ingests FHIR Condition data, reconciles patient state in real time, and exposes it to RAG agents via FastMCP tools.

## Prerequisites

- Python 3.13+
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) (`pip install uv` or `brew install uv`)

## Setup

```bash
uv sync --all-extras
```

This installs all runtime and test dependencies in one step.

---

## Quick Demo

Runs the full scenario end-to-end: out-of-order data arrival, TB correction, and audit-trail verification.

```bash
uv run python run_demo.py
```

**What it does:**

1. Loads `conditions.json` and splits it into two halves.
2. Ingests Day 1 (first half) — queries live representation immediately, no waiting.
3. Ingests Day 2 (second half, reversed order to simulate out-of-order arrival) — live state merges correctly.
4. Finds tuberculosis conditions in the current view and issues a user correction ("I don't have TB").
5. Re-queries — TB is excluded from results.
6. Verifies audit trail: original ingestion events and correction events are both preserved; no records deleted.

---

### MCP Inspector (interactive browser UI)

The server auto-seeds 105 conditions from `conditions.json` into the live store on startup (patient ID `demo-patient-001`), so the inspector has real data to query immediately.

```bash
uv run mcp dev src/mcp_server.py
```

Open `http://localhost:5173` and click **Connect**.

---

**Test `query_conditions`** (read tool)

1. In the **Tools** panel, click **List Tools** and select **query_conditions**.
2. Set the following field:
   - `patient_id` → `demo-patient-001`
3. Leave all other fields blank (optional filters).
4. Click **Run Tool**.

Expected: a JSON response with `total_count: 105`, a `conditions` array, and `has_corrections: false`.

---

**Test `issue_correction`** (mutation tool)

First note a TB condition ID from the `query_conditions` response above (look for `"code_text": "Latent tuberculosis"`), or use one of these real IDs from the dataset:
- `6323ca84-c7c5-46b0-8ece-1b9efe2b0a9b`
- `e36bce71-1e82-4bb8-8b6d-58d034a0b935`

1. Select **issue_correction**.
2. Set the following fields:
   - `patient_id` → `demo-patient-001`
   - `condition_ids` → `["6323ca84-c7c5-46b0-8ece-1b9efe2b0a9b"]` (or just put this id in a form field)
   - `reason` → `Patient confirmed no TB diagnosis`
3. Click **Run Tool**.

Expected: `status: "success"`, `conditions_masked: 1`, and `updated_condition_count: 104`.

Re-run `query_conditions` after — `total_count` will be `104` and `has_corrections` will be `true`. The original record is still in the audit log; it is only masked from the live view.

## Test MCP Tools via Pytest

Validate that both required tools are correctly registered, return the expected response contract, and correctly handle the TB correction mutation.

```bash
# Both tools are registered on the server
uv run pytest tests/test_e2e_flow.py::TestMCPTools::test_mcp_server_registers_both_required_tools -v

# query_conditions returns correct shape (status, conditions, total_count, metadata, has_corrections)
uv run pytest tests/test_e2e_flow.py::TestMCPTools::test_query_conditions_returns_correct_shape -v

# issue_correction masks TB conditions in the live view
uv run pytest tests/test_e2e_flow.py::TestMCPTools::test_issue_correction_via_tool_masks_tb -v

# Full MCP-focused test suites
uv run pytest tests/test_e2e_flow.py::TestFastMCPTools tests/test_e2e_flow.py::TestMCPTools -v
```

**Expected outcomes:**
- `test_mcp_server_registers_both_required_tools` — server exposes both `query_conditions` and `issue_correction`.
- `test_query_conditions_returns_correct_shape` — read tool returns all required response keys for RAG callers.
- `test_issue_correction_via_tool_masks_tb` — correction tool appends a masking event; TB is excluded from subsequent queries; `has_corrections` flips to `True`.

### Full test suite

```bash
uv run pytest
```

---

## Architecture Summary

This is a summary of how this system works, and some testcases that verify certain points made here.

### Write Path — Event-First Ingestion

All incoming FHIR Condition payloads — including malformed ones — are written as immutable rows to `condition_ingestion_events` before any validation result reaches the read model (`ConditionStore.ingest_raw_batch`). Pydantic validates each payload against a typed FHIR `Condition` model; failures are persisted as `failed_validation` events so the system can always answer "what did it ingest and what did it die on." Valid payloads are then projected into `extracted_conditions_staging`.

> **Verify (failed events tracked):** `uv run pytest tests/test_handout_requirements.py::TestHandoutRequirements::test_requirement_monitoring_raw_event_tracing -v`
> Injects one malformed payload alongside the real dataset and asserts `failed_validation == 1`, `parsed == 105`, and the malformed record is absent from the live view.

Ingestion can run synchronously or via an opt-in background worker (`start_background_worker` / `enqueue_raw_batch`). If the worker is unavailable or the queue is full, the call falls back to synchronous ingestion transparently — no data loss. This isn't necessary at all for this dataset size, this was more a scalability consideration.

### Read Path — Decoupled Live Representation

The read state is a set of DuckDB SQL views over the staging tables. `current_conditions` joins extracted conditions with the correction mask to instantly return the current patient truth. Because reads are view queries rather than derived computations, they return in under 10 ms regardless of whether background ingestion is still processing — fully decoupling read latency from write throughput.

> **Verify (reads are fast):** `uv run pytest tests/test_e2e_flow.py::TestLatency::test_reads_are_fast_after_ingestion -v`
> Ingests all 105 conditions then times a single `query_current_conditions` call; asserts it completes in under 100 ms.

> **Verify (background ingestion doesn't block reads):** `uv run pytest tests/test_e2e_flow.py::TestLatency::test_background_worker_does_not_block_reads -v`
> Enqueues a full batch on the background worker and immediately queries without flushing; asserts the read returns in under 200 ms.

Reconciliation deduplicates conditions by condition ID and respects FHIR `derived-from` extension lineage tracked in a `condition_lineage` table. Data quality issues (vague onset periods, missing clinical status, non-clinical admin entries, SNOMED/ICD-10 coding gaps) surface as `validation_flags` on each condition rather than causing silent drops.

> **Verify (data inconsistencies flagged, not dropped):** `uv run pytest tests/test_handout_requirements.py::TestHandoutRequirements::test_requirement_accuracy_data_inconsistencies -v`
> Asserts zero parse failures while confirming flags for same-day onset, missing clinical status, non-clinical entries, and lineage relationships.

### Corrections Path — Append-Only Mutations

User corrections (e.g., "I don't have TB") are appended to `correction_events` via `ConditionStore.issue_correction`. The `correction_mask_current` view aggregates active correction events per condition ID, and `current_conditions` left-joins against this mask to exclude any masked condition. Original clinician records in `condition_ingestion_events` and `extracted_conditions_staging` are never modified or deleted — full audit trail is preserved.

> **Verify (original data preserved after correction):** `uv run pytest tests/test_e2e_flow.py::TestUserCorrections::test_original_data_preserved_after_correction -v`
> Issues a correction and asserts the raw `condition_ingestion_events` count is unchanged before and after.

> **Verify (TB removed from live view):** `uv run pytest tests/test_handout_requirements.py::TestHandoutRequirements::test_requirement_tuberculosis_correction -v`

### MCP Interface — FastMCP Tools

Two FastMCP tools are registered in `src/mcp_server.py`:

- **`query_conditions`** — read tool; queries `current_conditions` for a given `patient_id` with optional filters (`status`, `code_system`, `code`). Returns conditions, count, quality-flag metadata, and a `has_corrections` flag. Hits the live view directly with zero dependency on ingestion state.
- **`issue_correction`** — mutation tool; accepts `patient_id` and `condition_ids`, appends a correction event, and the change is immediately visible in the next `query_conditions` call.

> **Verify (both tools registered, correct response shape, TB correction):** `uv run pytest tests/test_e2e_flow.py::TestMCPTools -v`

### Monitoring

- `get_ingestion_event_stats(patient_id)` — per-patient breakdown of `total_attempted`, `parsed`, `failed_validation`, and `conditions_flagged` from the raw event log.
- `get_background_ingestion_status()` — worker health snapshot: `worker_running`, `queue_depth`, `jobs_enqueued`, `jobs_processed`, `jobs_failed`, `last_error`, `last_processed_at`.
- All ingestion and correction paths emit structured JSON log events at `INFO` and `WARNING` levels.

> **Verify (monitoring fields present and accurate):** `uv run pytest tests/test_handout_requirements.py::TestHandoutRequirements::test_requirement_monitoring tests/test_handout_requirements.py::TestBackgroundObservability -v`
