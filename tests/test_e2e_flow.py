"""End-to-end tests for HANDOUT.md requirements.

Covers: live representation, unordered batches, TB correction, accuracy flags,
monitoring (died-on traceability), latency, and FastMCP tools.
"""
import json
import time
import pytest
from src.storage import ConditionStore
from src.mcp_tools import ConditionTools
from src.mcp_server import create_mcp_server


PATIENT = "0e3158f6-383d-40c6-acbf-62f187852934"


@pytest.fixture(scope="module")
def conditions_data():
    with open("conditions.json") as f:
        return json.load(f)


@pytest.fixture
def store():
    s = ConditionStore()
    yield s
    s.close()


@pytest.fixture
def tools(store):
    return ConditionTools(store)


# ---------------------------------------------------------------------------
# Live representation & unordered batches
# ---------------------------------------------------------------------------

class TestLiveRepresentation:

    def test_unordered_batches_produce_correct_state(self, store, conditions_data):
        """Half arrives day-1, other half day-2 in reversed order — full state is correct."""
        mid = len(conditions_data) // 2
        day1, day2 = conditions_data[:mid], list(reversed(conditions_data[mid:]))

        store.ingest_raw_batch(PATIENT, day1)
        after_day1 = store.query_current_conditions(PATIENT)
        assert len(after_day1) > 0

        store.ingest_raw_batch(PATIENT, day2)
        after_day2 = store.query_current_conditions(PATIENT)
        assert len(after_day2) == len(conditions_data)

    def test_tuberculosis_correction_masks_live_view(self, store, conditions_data):
        """User says 'I don't have TB' — TB must disappear from live view."""
        store.ingest_raw_batch(PATIENT, conditions_data)

        tb_before = [
            c for c in store.query_current_conditions(PATIENT)
            if "tubercul" in (c.get("code_text") or "").lower()
        ]
        assert len(tb_before) > 0, "TB conditions must exist before correction"

        store.issue_correction(
            patient_id=PATIENT,
            condition_ids=[c["condition_id"] for c in tb_before],
            reason="Patient confirmed no TB",
        )

        tb_after = [
            c for c in store.query_current_conditions(PATIENT)
            if "tubercul" in (c.get("code_text") or "").lower()
        ]
        assert len(tb_after) == 0
        # Other conditions remain
        assert len(store.query_current_conditions(PATIENT)) == len(conditions_data) - len(tb_before)

    def test_correction_preserves_original_events(self, store, conditions_data):
        """Append-only audit trail: ingestion event count must not change after correction."""
        store.ingest_raw_batch(PATIENT, conditions_data)
        conditions = store.query_current_conditions(PATIENT)

        raw_count_before = store.conn.execute(
            "SELECT COUNT(*) FROM condition_ingestion_events WHERE patient_id = ?",
            [PATIENT],
        ).fetchone()[0]

        store.issue_correction(
            patient_id=PATIENT,
            condition_ids=[conditions[0]["condition_id"]],
        )

        raw_count_after = store.conn.execute(
            "SELECT COUNT(*) FROM condition_ingestion_events WHERE patient_id = ?",
            [PATIENT],
        ).fetchone()[0]

        assert raw_count_before == raw_count_after


# ---------------------------------------------------------------------------
# Accuracy — data inconsistencies must be detected
# ---------------------------------------------------------------------------

class TestAccuracy:

    def test_all_real_conditions_parse_without_failures(self, conditions_data):
        """All 105 real conditions must parse; quality issues flagged, not dropped."""
        from src.models import ConditionValidator

        results = [ConditionValidator.validate(c) for c in conditions_data]
        assert all(r.is_valid for r in results)
        assert any(r.flags for r in results), "Expect at least some quality flags"

    def test_expected_inconsistency_types_are_detected(self, conditions_data):
        """Vague time ranges, wrong status, non-clinical entries, lineage must all be found."""
        from src.models import ConditionValidator

        all_flags = []
        for c in conditions_data:
            all_flags.extend(ConditionValidator.validate(c).flags)

        joined = " ".join(all_flags)
        assert "ambiguous_onset_period" in joined
        assert "missing_clinical_status" in joined
        assert "non_clinical_entry" in joined
        assert "has_lineage" in joined


# ---------------------------------------------------------------------------
# Monitoring — system must report what was ingested and what died
# ---------------------------------------------------------------------------

class TestMonitoring:

    def test_failed_payloads_are_recorded_not_projected(self, store, conditions_data):
        """Malformed payloads must appear in event store but NOT in live view."""
        malformed = [{"resourceType": "Condition", "id": "bad-001"}]  # missing required fields
        result = store.ingest_raw_batch(PATIENT, malformed + list(conditions_data))

        assert result["conditions_failed"] == 1
        assert result["conditions_ingested"] == len(conditions_data)

        stats = store.get_ingestion_event_stats(PATIENT)
        assert stats["total_attempted"] == len(conditions_data) + 1
        assert stats["failed_validation"] == 1
        assert stats["parsed"] == len(conditions_data)

        live = store.query_current_conditions(PATIENT)
        assert not any(c["condition_id"] == "bad-001" for c in live)

    def test_ingestion_stats_keys_present(self, store, conditions_data):
        """get_ingestion_event_stats must expose all required monitoring keys."""
        store.ingest_raw_batch(PATIENT, conditions_data)
        stats = store.get_ingestion_event_stats(PATIENT)
        for key in ("total_attempted", "parsed", "failed_validation", "conditions_flagged"):
            assert key in stats, f"Missing monitoring key: {key}"

    def test_statistics_reflect_live_state(self, store, conditions_data):
        """get_statistics must reflect masking and status breakdown."""
        store.ingest_raw_batch(PATIENT, conditions_data)
        stats = store.get_statistics(PATIENT)
        assert stats["total_conditions_raw"] == len(conditions_data)
        assert stats["total_conditions_current"] == len(conditions_data)
        assert isinstance(stats["by_status"], dict)

        # After one correction, current count drops
        cid = store.query_current_conditions(PATIENT)[0]["condition_id"]
        store.issue_correction(patient_id=PATIENT, condition_ids=[cid])
        stats2 = store.get_statistics(PATIENT)
        assert stats2["total_conditions_current"] == len(conditions_data) - 1
        assert stats2["total_conditions_raw"] == len(conditions_data)


# ---------------------------------------------------------------------------
# Latency — reads must be fast and non-blocking
# ---------------------------------------------------------------------------

class TestLatency:

    def test_reads_are_fast_after_ingestion(self, store, conditions_data):
        """Read latency must be well under 100 ms after a full dataset is ingested."""
        store.ingest_raw_batch(PATIENT, conditions_data)

        t0 = time.perf_counter()
        results = store.query_current_conditions(PATIENT)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        assert len(results) > 0
        assert elapsed_ms < 100, f"Read took {elapsed_ms:.1f} ms (target <100 ms)"

    def test_background_worker_does_not_block_reads(self, store, conditions_data):
        """Reads must return instantly while background ingestion is still processing."""
        store.start_background_worker()
        try:
            store.enqueue_raw_batch(PATIENT, list(conditions_data))

            t0 = time.perf_counter()
            result = store.query_current_conditions(PATIENT)
            elapsed_ms = (time.perf_counter() - t0) * 1000

            assert elapsed_ms < 200, (
                f"query_current_conditions blocked for {elapsed_ms:.1f} ms during background ingestion"
            )
            assert isinstance(result, list)
        finally:
            store.stop_background_worker()

    def test_background_worker_health_is_observable(self, store):
        """get_background_ingestion_status must expose required health fields."""
        store.start_background_worker()
        try:
            status = store.get_background_ingestion_status()
            for key in ("mode", "worker_running", "queue_depth", "jobs_enqueued",
                        "jobs_processed", "jobs_failed"):
                assert key in status, f"Missing status key: {key}"
            assert status["worker_running"] is True
        finally:
            store.stop_background_worker()

    def test_sync_fallback_ingests_and_returns_immediately(self, store, conditions_data):
        """enqueue_raw_batch with no worker started must fall back to sync ingestion."""
        result = store.enqueue_raw_batch(PATIENT, list(conditions_data[:5]), fallback_to_sync=True)
        assert result["status"] == "sync_fallback"
        live = store.query_current_conditions(PATIENT)
        assert len(live) > 0


# ---------------------------------------------------------------------------
# FastMCP tools
# ---------------------------------------------------------------------------

class TestMCPTools:

    def test_query_conditions_returns_correct_shape(self, tools, store, conditions_data):
        """query_conditions must return status, conditions, total_count, metadata."""
        store.ingest_raw_batch(PATIENT, conditions_data)
        result = tools.query_conditions(PATIENT)

        assert result["status"] == "success"
        assert result["patient_id"] == PATIENT
        assert isinstance(result["conditions"], list)
        assert result["total_count"] == len(conditions_data)
        assert "metadata" in result
        assert isinstance(result["has_corrections"], bool)

    def test_issue_correction_via_tool_masks_tb(self, tools, store, conditions_data):
        """issue_correction tool must mask conditions and update live count."""
        store.ingest_raw_batch(PATIENT, conditions_data)
        before = tools.query_conditions(PATIENT)

        tb_ids = [
            c["condition_id"] for c in before["conditions"]
            if "tubercul" in (c.get("code_text") or "").lower()
        ]
        assert tb_ids, "Need TB conditions for this test"

        correction = tools.issue_correction(
            patient_id=PATIENT,
            condition_ids=tb_ids,
            reason="Patient portal correction",
        )
        assert correction["status"] == "success"
        assert correction["conditions_masked"] == len(tb_ids)

        after = tools.query_conditions(PATIENT)
        assert after["total_count"] == before["total_count"] - len(tb_ids)
        assert after["has_corrections"] is True

    def test_mcp_server_registers_both_required_tools(self):
        """FastMCP server must expose query_conditions and issue_correction."""
        app, _ = create_mcp_server()
        tool_names = set(app._tool_manager._tools.keys())
        assert "query_conditions" in tool_names
        assert "issue_correction" in tool_names
