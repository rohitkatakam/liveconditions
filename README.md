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
