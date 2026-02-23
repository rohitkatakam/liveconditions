# Lotus Health — FHIR Condition MCP Server

## Setup

```bash
uv sync
```

## Run the MCP server (stdio transport)

```bash
uv run mcp-conditions
```

## Run the demo script

```bash
uv run python run_demo.py
```

## Run tests

```bash
uv run pytest
```

## Ingestion Paths

`store.ingest_raw_batch(patient_id, raw_conditions)` is the **canonical path**: it accepts raw
FHIR Condition dicts, validates each one internally with Pydantic, and persists every attempt
(valid or malformed) as an append-only event before projecting only valid conditions into the live
view — providing complete audit traceability at zero extra caller effort.

`store.ingest_batch(batch_id, patient_id, extracted_conditions, raw_events)` is a **compatibility
shim** for callers that pre-parse with `ConditionParser`. New callers should prefer
`ingest_raw_batch`.

Use `store.get_ingestion_event_stats(patient_id)` to query monitoring data: `total_attempted`,
`parsed`, `failed_validation`, and `conditions_flagged`.

## Async Background Ingestion

`store.start_background_worker()` enables background ingestion mode. Use `store.enqueue_raw_batch()` to submit batches without blocking the current thread.

```python
store = ConditionStore()
store.start_background_worker()
store.enqueue_raw_batch(patient_id, raw_conditions)  # returns immediately
store.flush_background_ingestion(timeout=10.0)       # wait for all jobs to finish
store.stop_background_worker()
```

**Fallback to sync:** Pass `fallback_to_sync=True` (default) to `enqueue_raw_batch`. If the worker is not running or the queue is full, the batch is ingested synchronously without data loss.

**Worker health:** Use `store.get_background_ingestion_status()` for queue depth, job counters, and last error. Use `get_ingestion_event_stats(patient_id)` to verify per-patient payload accounting.

**Rollback:** Call `stop_background_worker()` at any time to drain and stop the worker. Subsequent `enqueue_raw_batch(fallback_to_sync=True)` calls fall back to synchronous ingestion transparently.
