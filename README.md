# Lotus Health — FHIR Condition MCP Server

## Setup

```bash
uv sync
```

## Run the MCP server (stdio transport)

```bash
uv run mcp-conditions
```

## Run the demo

```bash
uv run python run_demo.py
```

## Run tests

```bash
uv run pytest
```

---

## Architecture

**Write path** — `ConditionStore.ingest_raw_batch(patient_id, raw_conditions)`

Every incoming payload is written to the append-only `condition_ingestion_events` table first. Valid payloads are then projected into `extracted_conditions_staging` for fast reads. Malformed payloads are stored with `ingestion_outcome='failed_validation'` and never appear in query results — providing a full audit trail of what was attempted vs what was accepted.

**Read path** — `ConditionStore.query_current_conditions(patient_id)`

Reads directly from the `current_conditions` view, which joins the staging table against the correction mask. Queries are instant and fully decoupled from ingestion.

**Corrections** — `ConditionStore.issue_correction(patient_id, condition_ids, reason)`

Corrections are appended to `correction_events`. The `current_conditions` view filters them out automatically. Original clinical records are never modified or deleted.

**Background ingestion** — opt-in for latency-sensitive callers

```python
store.start_background_worker()
store.enqueue_raw_batch(patient_id, raw_conditions)   # returns immediately
store.flush_background_ingestion(timeout=10.0)        # wait if needed
status = store.get_background_ingestion_status()      # worker health
store.stop_background_worker()
```

Pass `fallback_to_sync=True` (default) to `enqueue_raw_batch` to fall back to synchronous ingestion if the worker is not running.

**Monitoring** — `ConditionStore.get_ingestion_event_stats(patient_id)`

Returns `total_attempted`, `parsed`, `failed_validation`, and `conditions_flagged` — directly from the event log.

---

## FastMCP tools

| Tool | Purpose |
|------|---------|
| `query_conditions` | RAG tool — returns the cleaned, currently active patient state |
| `issue_correction` | Mutation tool — appends a correction that masks specified conditions |
