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

## Dynamic Reconciliation

After ingesting conditions, call `store.reconcile_conditions(patient_id)` to compute deduplication
and merging groups for a patient. The reconciliation engine:

1. Finds all pairs of conditions that share the same medical code (SNOMED or ICD-10).
2. Clusters overlapping conditions into groups using union-find.
3. Selects a **canonical** condition per group using this precedence:
   - **Lineage depth** — the deepest `derived-from` descendant wins.
   - **Clinical status rank** — Active > In remission > Inactive > Resolved.
   - **Extraction timestamp** — most recently ingested condition wins.
   - **Alphabetical tiebreak** — stable fallback; marks the group as ambiguous.
4. Persists group metadata in `condition_reconciliation_groups` and member membership in
   `condition_reconciliation_members`.

```python
store = ConditionStore()
store.ingest_raw_batch(patient_id, raw_conditions)
store.reconcile_conditions(patient_id)   # must call to refresh live reconciled view
conditions = store.query_current_conditions(patient_id)   # returns reconciled result
```

> **Important:** Until `reconcile_conditions()` is called for a patient,
> `query_current_conditions` returns the unreconciled result (all unmasked conditions).
> Call it again after each new ingestion batch to keep the live view current.

### Observability stat keys (from `get_statistics`)

| Key | Meaning |
|-----|---------|
| `reconciliation_groups_total` | Total number of reconciliation groups computed for this patient. |
| `reconciliation_groups_merged` | Groups that contain more than one condition (actual merges). |
| `reconciliation_conditions_deduped` | Total number of redundant conditions collapsed (sum of `member_count - 1` for merged groups). |
| `reconciliation_ambiguous_groups` | Merged groups where canonical selection fell through to alphabetical tiebreak (no lineage, status, or timestamp distinguishes members). |
| `reconciliation_lineage_overrides` | Groups where canonical was selected by FHIR `derived-from` lineage depth. |

### Interaction with correction masks

Correction events (from `issue_correction`) mask conditions **before** reconciliation runs.
Masked conditions are excluded from candidate sets, so they cannot become canonical members of
any group. Reconciliation always operates on the current unmasked view — re-running
`reconcile_conditions` after a correction will re-compute groups without the masked condition.
