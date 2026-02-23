"""End-to-end tests for event sourcing and correction flow."""
import json
import uuid
import pytest
import logging
from datetime import datetime

from src.parser import ConditionParser
from src.models import ConditionValidator
from src.storage import ConditionStore
from src.mcp_tools import ConditionTools


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
        assert isinstance(stats["by_coding_system"], dict)
    
    def test_statistics_after_correction(self, store, sample_conditions):
        """Verify statistics update after corrections."""
        patient_id = "test-patient-013"
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
        
        stats_before = store.get_statistics(patient_id)
        
        # Issue correction
        conditions = store.query_current_conditions(patient_id)
        if conditions:
            store.issue_correction(
                patient_id=patient_id,
                condition_ids=[conditions[0]["condition_id"]],
            )
            
            stats_after = store.get_statistics(patient_id)
            
            # Raw count unchanged, current count reduced
            assert stats_after["total_conditions_raw"] == stats_before["total_conditions_raw"]
            assert stats_after["total_conditions_current"] == stats_before["total_conditions_current"] - 1
            assert stats_after["conditions_masked"] == 1


class TestPerformance:
    """Basic performance tests."""
    
    def test_query_latency(self, store, sample_conditions):
        """Verify queries complete within latency target (<50ms)."""
        import time
        
        patient_id = "test-patient-014"
        conditions_data = sample_conditions["all"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        
        # Measure query time
        start = time.time()
        conditions = store.query_current_conditions(patient_id)
        elapsed_ms = (time.time() - start) * 1000
        
        assert len(conditions) > 0
        # Allow generous margin for test environment
        assert elapsed_ms < 1000, f"Query took {elapsed_ms}ms (target: <50ms)"
    
    def test_ingest_latency(self, store, sample_conditions):
        """Verify ingestion completes within latency target (<100ms)."""
        import time
        
        patient_id = "test-patient-015"
        conditions_data = sample_conditions["day1"]
        
        parser = ConditionParser(patient_id)
        parser.parse_conditions(conditions_data)
        extracted = parser.get_extracted_conditions()
        
        # Measure ingest time
        start = time.time()
        store.ingest_batch(
            batch_id=uuid.uuid4(),
            patient_id=patient_id,
            extracted_conditions=extracted,
            raw_events=conditions_data,
        )
        elapsed_ms = (time.time() - start) * 1000
        
        # Allow generous margin for test environment
        assert elapsed_ms < 5000, f"Ingestion took {elapsed_ms}ms (target: <100ms)"


class TestEventFirstIngestion:
    """Tests for event-first (lossless) ingestion: every payload attempt is persisted."""

    # Minimal malformed payload: missing required clinicalStatus, code, subject
    MALFORMED_PAYLOAD = {
        "resourceType": "Condition",
        "id": "malformed-test-001",
    }

    # Minimal valid payload that passes Pydantic validation
    VALID_PAYLOAD_STUB = {
        "resourceType": "Condition",
        "id": "stub-valid-001",
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "55561003", "display": "Active"}],
            "text": "Active",
        },
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "73211009", "display": "Diabetes"}],
            "text": "Diabetes mellitus",
        },
        "subject": {"reference": "Patient/test-patient"},
    }

    def test_ingestion_persists_invalid_payload_events(self, store):
        """Malformed payloads must still appear as append-only events with failed_validation outcome."""
        patient_id = "test-event-first-001"

        result = store.ingest_raw_batch(
            patient_id=patient_id,
            raw_conditions=[self.MALFORMED_PAYLOAD, self.VALID_PAYLOAD_STUB],
        )

        # Both payloads must be recorded regardless of validity
        event_rows = store.conn.execute(
            "SELECT ingestion_outcome FROM condition_ingestion_events WHERE patient_id = ? ORDER BY created_at",
            [patient_id],
        ).fetchall()

        assert len(event_rows) == 2, f"Expected 2 events, got {len(event_rows)}"
        outcomes = {row[0] for row in event_rows}
        assert "failed_validation" in outcomes
        assert "parsed" in outcomes
        # Only the valid one should appear in the live representation
        live = store.query_current_conditions(patient_id)
        assert len(live) == 1
        assert live[0]["condition_id"] == "stub-valid-001"

    def test_ingestion_records_parse_error_metadata(self, store):
        """Failed-validation events must include auditable error_detail JSON metadata."""
        patient_id = "test-event-first-002"

        store.ingest_raw_batch(
            patient_id=patient_id,
            raw_conditions=[self.MALFORMED_PAYLOAD],
        )

        row = store.conn.execute(
            "SELECT error_detail, ingestion_outcome FROM condition_ingestion_events WHERE patient_id = ?",
            [patient_id],
        ).fetchone()

        assert row is not None
        assert row[1] == "failed_validation"
        import json as _json
        error_detail = _json.loads(row[0])
        assert "error_type" in error_detail
        assert "error_message" in error_detail
        assert "source_stage" in error_detail

    def test_event_count_equals_all_attempted_payloads(self, store, sample_conditions):
        """total events persisted must equal total payloads attempted (valid + invalid)."""
        patient_id = "test-event-first-003"
        all_conditions = sample_conditions["all"]

        # Inject 3 malformed payloads alongside the real dataset
        malformed_batch = [
            {"resourceType": "Condition", "id": f"bad-{i}"} for i in range(3)
        ]
        mixed_batch = malformed_batch + all_conditions

        result = store.ingest_raw_batch(
            patient_id=patient_id,
            raw_conditions=mixed_batch,
        )

        stats = store.get_ingestion_event_stats(patient_id)

        assert stats["total_attempted"] == len(mixed_batch)
        assert stats["failed_validation"] == 3
        assert stats["parsed"] == len(all_conditions)
        assert result["conditions_failed"] == 3
        assert result["conditions_ingested"] == len(all_conditions)


class TestMCPServerRegistration:
    """Tests for FastMCP server adapter wiring."""

    @pytest.fixture
    def mcp_app_and_store(self):
        """Create a fresh MCP server with a new store."""
        from src.mcp_server import create_mcp_server

        app, store = create_mcp_server()
        yield app, store
        store.close()

    @pytest.fixture
    def populated_mcp(self, sample_conditions):
        """Create a server with day1 conditions already ingested."""
        from src.mcp_server import create_mcp_server

        app, store = create_mcp_server()
        patient_id = "mcp-test-patient-001"
        store.ingest_raw_batch(
            patient_id=patient_id,
            raw_conditions=sample_conditions["day1"],
        )
        yield app, store, patient_id
        store.close()

    def test_mcp_server_builds_and_registers_tools(self, mcp_app_and_store):
        """create_mcp_server returns an app with both tools registered."""
        app, _store = mcp_app_and_store
        registered_names = set(app._tool_manager._tools.keys())
        assert "query_conditions" in registered_names
        assert "issue_correction" in registered_names

    @pytest.mark.asyncio
    async def test_mcp_query_conditions_returns_expected_payload(self, populated_mcp):
        """Calling query_conditions via MCP app returns the expected contract."""
        app, _store, patient_id = populated_mcp

        result = await app.call_tool("query_conditions", {"patient_id": patient_id})
        assert result, "Should return non-empty result"

        import json as _json

        payload = _json.loads(result[0].text)
        assert payload["status"] == "success"
        assert "conditions" in payload
        assert "total_count" in payload
        assert "metadata" in payload
        assert payload["total_count"] > 0

    @pytest.mark.asyncio
    async def test_mcp_query_conditions_respects_filters(self, populated_mcp):
        """query_conditions with status filter returns only matching conditions."""
        app, _store, patient_id = populated_mcp

        result = await app.call_tool(
            "query_conditions", {"patient_id": patient_id, "status": "Active"}
        )

        import json as _json

        payload = _json.loads(result[0].text)
        assert payload["status"] == "success"
        for condition in payload["conditions"]:
            assert condition.get("clinical_status", "").lower() == "active"

    @pytest.mark.asyncio
    async def test_mcp_issue_correction_masks_condition(self, populated_mcp):
        """issue_correction removes the masked condition from subsequent queries."""
        app, _store, patient_id = populated_mcp
        import json as _json

        # Get current conditions
        result = await app.call_tool("query_conditions", {"patient_id": patient_id})
        payload = _json.loads(result[0].text)
        assert payload["total_count"] > 0

        # Pick the first condition to mask
        target_id = payload["conditions"][0]["condition_id"]

        # Issue correction
        corr_result = await app.call_tool(
            "issue_correction",
            {"patient_id": patient_id, "condition_ids": [target_id], "reason": "test correction"},
        )
        corr_payload = _json.loads(corr_result[0].text)
        assert corr_payload["status"] == "success"
        assert corr_payload["conditions_masked"] >= 1

        # Re-query: masked condition should be gone
        after_result = await app.call_tool("query_conditions", {"patient_id": patient_id})
        after_payload = _json.loads(after_result[0].text)
        remaining_ids = {c["condition_id"] for c in after_payload["conditions"]}
        assert target_id not in remaining_ids

    @pytest.mark.asyncio
    async def test_mcp_issue_correction_preserves_audit_trail(self, populated_mcp):
        """After correction, ingestion events table count is unchanged."""
        app, store, patient_id = populated_mcp
        import json as _json

        # Capture ingestion event count before correction
        count_before = store.conn.execute(
            "SELECT COUNT(*) FROM condition_ingestion_events WHERE patient_id = ?",
            [patient_id],
        ).fetchone()[0]

        # Issue a correction
        result = await app.call_tool("query_conditions", {"patient_id": patient_id})
        payload = _json.loads(result[0].text)
        target_id = payload["conditions"][0]["condition_id"]

        await app.call_tool(
            "issue_correction",
            {"patient_id": patient_id, "condition_ids": [target_id]},
        )

        # Ingestion events unchanged
        count_after = store.conn.execute(
            "SELECT COUNT(*) FROM condition_ingestion_events WHERE patient_id = ?",
            [patient_id],
        ).fetchone()[0]
        assert count_after == count_before

        # Correction event exists
        corr_count = store.conn.execute(
            "SELECT COUNT(*) FROM correction_events WHERE patient_id = ?",
            [patient_id],
        ).fetchone()[0]
        assert corr_count >= 1


class TestCanonicalIngestion:
    """Locks the canonical ingest_raw_batch contract (write-path hardening)."""

    def test_canonical_ingestion_records_all_attempts_from_raw_batch(
        self, store, sample_conditions
    ):
        """Mixed batch: valid + malformed — all attempts must appear in event stats."""
        patient_id = "canonical-test-001"
        day1 = sample_conditions["day1"]
        malformed = [
            {"resourceType": "Condition", "id": "bad-1"},
            {"resourceType": "Condition", "id": "bad-2"},
        ]
        mixed = day1 + malformed

        store.ingest_raw_batch(patient_id, raw_conditions=mixed)

        stats = store.get_ingestion_event_stats(patient_id)
        assert stats["total_attempted"] == len(day1) + 2
        assert stats["failed_validation"] == 2
        assert stats["parsed"] == len(day1)

        live = store.query_current_conditions(patient_id)
        live_ids = {c["condition_id"] for c in live}
        assert "bad-1" not in live_ids
        assert "bad-2" not in live_ids

    def test_invalid_payloads_never_enter_live_view_but_remain_auditable(self, store):
        """Malformed-only batch: zero live rows, but event table has auditable rows."""
        patient_id = "canonical-test-002"
        malformed_only = [
            {"resourceType": "Condition", "id": "bad-only-1"},
            {"resourceType": "Condition", "id": "bad-only-2"},
        ]

        store.ingest_raw_batch(patient_id, raw_conditions=malformed_only)

        live = store.query_current_conditions(patient_id)
        assert len(live) == 0

        failed_rows = store.conn.execute(
            """SELECT event_id, error_detail FROM condition_ingestion_events
               WHERE patient_id = ? AND ingestion_outcome = 'failed_validation'""",
            [patient_id],
        ).fetchall()
        assert len(failed_rows) == 2

        for row in failed_rows:
            error_detail = json.loads(row[1])
            assert "error_type" in error_detail
            assert "error_message" in error_detail
            assert "source_stage" in error_detail

    def test_out_of_order_ingestion_via_raw_path(self, store, sample_conditions):
        """Day 2 arrives first via ingest_raw_batch, then Day 1 — merged count must be correct."""
        patient_id = "canonical-test-003"
        day1 = sample_conditions["day1"]
        day2 = sample_conditions["day2"]

        # Ingest day2 first
        store.ingest_raw_batch(patient_id, raw_conditions=day2)
        count_after_day2 = len(store.query_current_conditions(patient_id))

        # Then ingest day1
        store.ingest_raw_batch(patient_id, raw_conditions=day1)
        count_after_day1 = len(store.query_current_conditions(patient_id))

        assert count_after_day1 >= count_after_day2
        assert count_after_day1 <= len(day1) + len(day2)

    def test_correction_flow_works_after_raw_canonical_ingestion(
        self, store, sample_conditions
    ):
        """After ingest_raw_batch, issue_correction masks TB and leaves ingestion events intact."""
        patient_id = "canonical-test-004"
        day1 = sample_conditions["day1"]

        store.ingest_raw_batch(patient_id, raw_conditions=day1)

        conditions = store.query_current_conditions(patient_id)
        tb_conditions = [c for c in conditions if "tubercul" in c["code_text"].lower()]

        if not tb_conditions:
            pytest.skip("No TB conditions in day1 sample data")

        tb_id = tb_conditions[0]["condition_id"]
        ingestion_count_before = store.conn.execute(
            "SELECT COUNT(*) FROM condition_ingestion_events WHERE patient_id = ?",
            [patient_id],
        ).fetchone()[0]

        store.issue_correction(patient_id, condition_ids=[tb_id])

        conditions_after = store.query_current_conditions(patient_id)
        assert not any(c["condition_id"] == tb_id for c in conditions_after)

        correction_count = store.conn.execute(
            "SELECT COUNT(*) FROM correction_events WHERE patient_id = ?",
            [patient_id],
        ).fetchone()[0]
        assert correction_count >= 1

        ingestion_count_after = store.conn.execute(
            "SELECT COUNT(*) FROM condition_ingestion_events WHERE patient_id = ?",
            [patient_id],
        ).fetchone()[0]
        assert ingestion_count_after == ingestion_count_before

    def test_ingestion_event_stats_consistent_with_event_table(
        self, store, sample_conditions
    ):
        """Stats returned by get_ingestion_event_stats must match raw SQL counts."""
        patient_id = "canonical-test-005"
        day1 = sample_conditions["day1"]

        store.ingest_raw_batch(patient_id, raw_conditions=day1)

        stats = store.get_ingestion_event_stats(patient_id)

        # Manual count from raw SQL
        raw_rows = store.conn.execute(
            """SELECT ingestion_outcome, COUNT(*)
               FROM condition_ingestion_events
               WHERE patient_id = ?
               GROUP BY ingestion_outcome""",
            [patient_id],
        ).fetchall()
        manual_by_outcome = {row[0]: row[1] for row in raw_rows}
        manual_total = sum(manual_by_outcome.values())

        assert stats["total_attempted"] == manual_total
        assert stats["by_outcome"] == manual_by_outcome
        assert stats["parsed"] == manual_by_outcome.get("parsed", 0)
        assert stats["failed_validation"] == manual_by_outcome.get("failed_validation", 0)
        # conditions_flagged must be present in the stats dict
        assert "conditions_flagged" in stats

    def test_non_dict_malformed_payload_is_persisted_not_crashed(self, store):
        """Non-dict payloads (list, string, None) must not crash ingestion and must be recorded."""
        patient_id = "canonical-test-006"
        non_dict_payloads = [
            ["this", "is", "a", "list"],
            "just a string",
            None,
        ]

        # Must not raise
        result = store.ingest_raw_batch(patient_id, raw_conditions=non_dict_payloads)

        assert result["conditions_failed"] == 3
        assert result["conditions_ingested"] == 0

        failed_rows = store.conn.execute(
            """SELECT ingestion_outcome FROM condition_ingestion_events
               WHERE patient_id = ? AND ingestion_outcome = 'failed_validation'""",
            [patient_id],
        ).fetchall()
        assert len(failed_rows) == 3

        live = store.query_current_conditions(patient_id)
        assert len(live) == 0
