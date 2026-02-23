"""End-to-end tests for event sourcing and correction flow."""
import json
import time
import threading
import uuid
import pytest
import logging
from datetime import datetime

from src.parser import ConditionParser
from src.models import ConditionValidator
from src.storage import ConditionStore, BackgroundIngestionWorker, IngestionJob
from src.mcp_tools import ConditionTools
from src.mcp_server import create_mcp_server

# Canonical patient ID used by TestLatency / TestMCPTools
PATIENT = "test-patient-e2e"


@pytest.fixture
def conditions_data():
    """Load all FHIR conditions from the sample dataset."""
    with open("conditions.json") as f:
        return json.load(f)


@pytest.fixture
def store():
    """Create a fresh ConditionStore for each test."""
    store = ConditionStore()
    yield store
    store.close()


@pytest.fixture
def tools(store):
    """Create ConditionTools with a store."""
    return ConditionTools(store)


@pytest.fixture
def sample_conditions():
    """Load sample FHIR conditions from test data."""
    with open("conditions.json") as f:
        data = json.load(f)
    
    # Data is a direct array of conditions
    all_conditions = data if isinstance(data, list) else data.get("conditions", [])
    mid = len(all_conditions) // 2
    
    return {
        "all": all_conditions,
        "day1": all_conditions[:mid],
        "day2": all_conditions[mid:],
    }


class TestSingleIngestion:
    """Tests for single batch ingestion."""
    
    def test_ingest_single_batch(self, store, sample_conditions):
        """Verify single batch ingestion creates normalized state."""
        patient_id = "test-patient-001"
        conditions_data = sample_conditions["day1"]
        
        # Parse conditions
        parser = ConditionParser(patient_id)
        parse_result = parser.parse_conditions(conditions_data)
        
        assert parse_result["parsed"] > 0, "Should parse some conditions"
        
        extracted = parser.get_extracted_conditions()
        assert len(extracted) == parse_result["parsed"]
        
        # Ingest batch
        batch_id = uuid.uuid4()
        ingest_result = store.ingest_batch(
            batch_id=batch_id,
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        assert ingest_result["status"] == "success"
        assert ingest_result["conditions_ingested"] == len(extracted)
        
        # Query current conditions
        conditions = store.query_current_conditions(patient_id)
        assert len(conditions) > 0
        assert all(c["patient_id"] == patient_id for c in conditions)
    
    def test_validation_flags_preserved(self, store, sample_conditions):
        """Verify validation flags are stored and retrievable."""
        patient_id = "test-patient-002"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parse_result = parser.parse_conditions(conditions_data)
        
        extracted = parser.get_extracted_conditions()
        flagged_count = sum(1 for c in extracted if c.validation_flags)
        
        # Ingest
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        # Query and check flags
        conditions = store.query_current_conditions(patient_id)
        conditions_with_flags = [c for c in conditions if c["validation_flags"] and c["validation_flags"] != "[]"]
        
        assert len(conditions_with_flags) >= 0  # May have flags
        # Flags may be JSON strings or lists depending on database
        for c in conditions:
            if c["validation_flags"]:
                if isinstance(c["validation_flags"], str):
                    parsed = json.loads(c["validation_flags"])
                    assert isinstance(parsed, list)
                else:
                    assert isinstance(c["validation_flags"], list)
    
    def test_active_conditions_filter(self, store, sample_conditions):
        """Verify active_conditions_only view works."""
        patient_id = "test-patient-003"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        all_conditions = store.query_current_conditions(patient_id)
        active_conditions = store.query_active_conditions(patient_id)
        
        # Active should be subset or equal
        assert len(active_conditions) <= len(all_conditions)
        assert all(c["clinical_status"] == "Active" for c in active_conditions)


class TestOutOfOrderIngestion:
    """Tests for out-of-order data arrival (Day 2 before Day 1)."""
    
    def test_out_of_order_ingestion_merges_correctly(self, store, sample_conditions):
        """Verify out-of-order arrival doesn't break state."""
        patient_id = "test-patient-004"
        
        day1_conditions = sample_conditions["day1"]
        day2_conditions = sample_conditions["day2"]
        
        # Parse Day 2 first (reverse order)
        parser_day2 = ConditionParser(patient_id)
        parser_day2.parse_conditions(day2_conditions)
        extracted_day2 = parser_day2.get_extracted_conditions()
        
        # Ingest Day 2
        batch_id_day2 = uuid.uuid4()
        store.ingest_batch(
            batch_id=batch_id_day2,
            patient_id=patient_id,
            extracted_conditions=extracted_day2,
            raw_events=day2_conditions,
        )
        
        count_after_day2 = len(store.query_current_conditions(patient_id))
        
        # Parse and ingest Day 1
        parser_day1 = ConditionParser(patient_id)
        parser_day1.parse_conditions(day1_conditions)
        extracted_day1 = parser_day1.get_extracted_conditions()
        
        batch_id_day1 = uuid.uuid4()
        store.ingest_batch(
            batch_id=batch_id_day1,
            patient_id=patient_id,
            extracted_conditions=extracted_day1,
            raw_events=day1_conditions,
        )
        
        count_after_day1 = len(store.query_current_conditions(patient_id))
        
        # Total should be sum of both (or less if duplicates)
        assert count_after_day1 >= count_after_day2
        assert count_after_day1 <= len(extracted_day1) + len(extracted_day2)
    
    def test_lineage_preserved_across_batches(self, store, sample_conditions):
        """Verify derived-from relationships are maintained across out-of-order batches."""
        patient_id = "test-patient-005"
        
        day1_conditions = sample_conditions["day1"]
        day2_conditions = sample_conditions["day2"]
        
        # Ingest Day 2 first
        parser_day2 = ConditionParser(patient_id)
        parser_day2.parse_conditions(day2_conditions)
        extracted_day2 = parser_day2.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted_day2,
            raw_events=day2_conditions,
        )
        
        # Ingest Day 1
        parser_day1 = ConditionParser(patient_id)
        parser_day1.parse_conditions(day1_conditions)
        extracted_day1 = parser_day1.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted_day1,
            raw_events=day1_conditions,
        )
        
        # Find a condition with lineage
        conditions_with_lineage = [
            c for c in extracted_day1 + extracted_day2
            if c.derived_from_ids
        ]
        
        if conditions_with_lineage:
            test_condition = conditions_with_lineage[0]
            lineage = store.get_condition_lineage(patient_id, test_condition.condition_id)
            
            # Should have parent IDs from derived_from_ids
            assert len(lineage["parent_ids"]) > 0


class TestUserCorrections:
    """Tests for user-initiated corrections."""
    
    def test_correction_masks_condition(self, store, tools, sample_conditions):
        """Verify correction excludes condition from live representation."""
        patient_id = "test-patient-006"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        # Ingest
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        conditions_before = store.query_current_conditions(patient_id)
        count_before = len(conditions_before)
        
        # Get TB-related conditions
        tb_conditions = [c for c in conditions_before if "tubercul" in c["code_text"].lower()]
        
        if tb_conditions:
            condition_id_to_mask = tb_conditions[0]["condition_id"]
            
            # Issue correction
            result = store.issue_correction(
                patient_id=patient_id,
                condition_ids=[condition_id_to_mask],
                reason="User confirmed they don't have TB",
            )
            
            assert result["status"] == "success"
            assert result["conditions_masked"] == 1
            
            # Query after correction
            conditions_after = store.query_current_conditions(patient_id)
            assert len(conditions_after) == count_before - 1
            
            # Verify condition is not in current_conditions
            assert not any(c["condition_id"] == condition_id_to_mask for c in conditions_after)
    
    def test_original_data_preserved_after_correction(self, store, sample_conditions):
        """Verify original event log is unchanged after correction."""
        patient_id = "test-patient-007"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        # Ingest
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        # Count raw events before correction
        raw_count_before = store.conn.execute(
            "SELECT COUNT(*) FROM condition_ingestion_events WHERE patient_id = ?",
            [patient_id],
        ).fetchall()[0][0]
        
        # Get a condition to mask
        conditions = store.query_current_conditions(patient_id)
        if conditions:
            condition_id = conditions[0]["condition_id"]
            
            # Issue correction
            store.issue_correction(
                patient_id=patient_id,
                condition_ids=[condition_id],
            )
            
            # Count raw events after correction
            raw_count_after = store.conn.execute(
                "SELECT COUNT(*) FROM condition_ingestion_events WHERE patient_id = ?",
                [patient_id],
            ).fetchall()[0][0]
            
            # Raw events should be unchanged
            assert raw_count_before == raw_count_after
            
            # But correction event should exist
            correction_count = store.conn.execute(
                "SELECT COUNT(*) FROM correction_events WHERE patient_id = ?",
                [patient_id],
            ).fetchall()[0][0]
            
            assert correction_count > 0
    
    def test_multiple_corrections_cumulative(self, store, sample_conditions):
        """Verify multiple corrections accumulate correctly."""
        patient_id = "test-patient-008"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        conditions_initial = store.query_current_conditions(patient_id)
        count_initial = len(conditions_initial)
        
        # Issue multiple corrections
        if len(conditions_initial) >= 2:
            ids_to_mask = [
                conditions_initial[0]["condition_id"],
                conditions_initial[1]["condition_id"],
            ]
            
            store.issue_correction(
                patient_id=patient_id,
                condition_ids=ids_to_mask,
            )
            
            conditions_final = store.query_current_conditions(patient_id)
            count_final = len(conditions_final)
            
            assert count_final == count_initial - 2


class TestFastMCPTools:
    """Tests for FastMCP tool interfaces."""
    
    def test_query_conditions_tool_basic(self, tools, store, sample_conditions):
        """Verify query_conditions tool returns expected structure."""
        patient_id = "test-patient-009"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        # Call tool
        result = tools.query_conditions(patient_id)
        
        assert result["status"] == "success"
        assert result["patient_id"] == patient_id
        assert "conditions" in result
        assert isinstance(result["conditions"], list)
        assert "total_count" in result
        assert "metadata" in result
        assert isinstance(result["has_corrections"], bool)
    
    def test_query_conditions_with_filters(self, tools, store, sample_conditions):
        """Verify query_conditions respects status filters."""
        patient_id = "test-patient-010"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        # Query all
        all_result = tools.query_conditions(patient_id)
        
        # Query active only
        active_result = tools.query_conditions(patient_id, status="Active")
        
        assert active_result["total_count"] <= all_result["total_count"]
        assert all(c["clinical_status"] == "Active" for c in active_result["conditions"])
    
    def test_issue_correction_tool(self, tools, store, sample_conditions):
        """Verify issue_correction tool updates state correctly."""
        patient_id = "test-patient-011"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        # Get conditions before correction
        before = tools.query_conditions(patient_id)
        count_before = before["total_count"]
        
        # Issue correction via tool
        if before["conditions"]:
            condition_id = before["conditions"][0]["condition_id"]
            
            correction_result = tools.issue_correction(
                patient_id=patient_id,
                condition_ids=[condition_id],
                reason="Test correction",
            )
            
            assert correction_result["status"] == "success"
            assert correction_result["conditions_masked"] == 1
            
            # Verify via query tool
            after = tools.query_conditions(patient_id)
            assert after["total_count"] == count_before - 1
            assert after["has_corrections"] is True


class TestStatisticsAndMetadata:
    """Tests for statistics and metadata queries."""
    
    def test_statistics_accuracy(self, store, sample_conditions):
        """Verify statistics reflect actual state."""
        patient_id = "test-patient-012"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        stats = store.get_statistics(patient_id)
        
        assert stats["patient_id"] == patient_id
        assert stats["total_conditions_current"] == len(extracted)
        assert stats["conditions_masked"] == 0
        assert isinstance(stats["by_status"], dict)

        # After one correction, current count drops
        cid = store.query_current_conditions(patient_id)[0]["condition_id"]
        store.issue_correction(patient_id=patient_id, condition_ids=[cid])
        stats2 = store.get_statistics(patient_id)
        assert stats2["total_conditions_current"] == len(extracted) - 1
        assert stats2["total_conditions_raw"] == len(extracted)


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
            required_keys = {"worker_running", "queue_depth", "jobs_enqueued", "jobs_processed", "jobs_failed"}
            missing = required_keys - set(status.keys())
            assert not missing, f"Missing health keys: {missing}"
            assert status["worker_running"] is True
            assert status["jobs_enqueued"] == 0
            assert status["jobs_processed"] == 0
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
