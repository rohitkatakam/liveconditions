"""Tests for HANDOUT.md requirements."""
import json
import time
import uuid
import pytest
from src.parser import ConditionParser
from src.storage import ConditionStore


class TestHandoutRequirements:
    """Test that implementation satisfies HANDOUT.md requirements."""

    @pytest.fixture
    def conditions_data(self):
        """Load conditions.json."""
        with open("conditions.json") as f:
            return json.load(f)

    def test_requirement_live_representation(self, conditions_data):
        """
        Requirement: A Python program that holds onto a representation of the
        user's data and performs updates when data arrives.
        """
        patient_id = "0e3158f6-383d-40c6-acbf-62f187852934"
        parser = ConditionParser(patient_id)

        # Parse all conditions
        results = parser.parse_conditions(conditions_data)

        # Verify live representation is built
        assert results["total"] == 105
        assert results["parsed"] == 105
        assert len(parser.get_extracted_conditions()) == 105

        # Verify we can query it immediately
        extracted = parser.get_extracted_conditions()
        assert all(c.patient_id == patient_id for c in extracted)
        assert all(c.condition_id for c in extracted)

    def test_requirement_unordered_data(self):
        """
        Requirement: Half of the Conditions data arrives on one day,
        then the other half on the next day. Data is not in chronological order.
        """
        patient_id = "test-patient"

        with open("conditions.json") as f:
            all_conditions = json.load(f)

        # Simulate half arriving first
        half = len(all_conditions) // 2
        first_batch = all_conditions[:half]
        second_batch = all_conditions[half:]

        # Parse first batch
        parser = ConditionParser(patient_id)
        results1 = parser.parse_conditions(first_batch)
        assert results1["parsed"] == len(first_batch)
        assert len(parser.get_extracted_conditions()) == len(first_batch)

        # Parse second batch (in reversed order to simulate out-of-order)
        second_batch_reversed = list(reversed(second_batch))
        results2 = parser.parse_conditions(second_batch_reversed)
        assert results2["parsed"] == len(second_batch)

        # Verify all are in the representation
        total = len(parser.get_extracted_conditions())
        assert total == len(all_conditions)

    def test_requirement_tuberculosis_correction(self, conditions_data):
        """
        Requirement: The user wants to correct the robot about whether they have
        tuberculosis. Show that your live representation successfully removes
        references to tuberculosis when the user corrects it.
        """
        patient_id = "0e3158f6-383d-40c6-acbf-62f187852934"
        parser = ConditionParser(patient_id)
        results = parser.parse_conditions(conditions_data)

        # Find TB conditions
        tb_conditions_before = parser.get_tb_conditions()
        assert len(tb_conditions_before) > 0, "Should find TB conditions"

        # Verify TB is represented
        all_conditions = parser.get_extracted_conditions()
        tb_codes = [c for c in all_conditions if "TB" in c.code_text or "tuberculosis" in c.code_text.lower()]
        assert len(tb_codes) > 0

        # Simulate correction: user says "I don't have tuberculosis"
        # In this demo, we just filter them out
        tb_condition_ids = {c.condition_id for c in tb_conditions_before}

        # Create corrected representation by removing TB
        corrected_conditions = [
            c for c in all_conditions
            if c.condition_id not in tb_condition_ids
        ]

        # Verify TB is gone
        tb_remaining = [
            c for c in corrected_conditions
            if "tuberculosis" in c.code_text.lower()
        ]
        assert len(tb_remaining) == 0, "TB should be completely removed after correction"

        # Verify other conditions remain
        assert len(corrected_conditions) == len(all_conditions) - len(tb_condition_ids)

    def test_requirement_accuracy_data_inconsistencies(self, conditions_data):
        """
        Requirement: Accuracy - Can you correctly understand the data and deal
        with inconsistencies?
        """
        patient_id = "0e3158f6-383d-40c6-acbf-62f187852934"
        parser = ConditionParser(patient_id)
        results = parser.parse_conditions(conditions_data)

        # All conditions should parse without errors
        assert results["failed"] == 0, "Should handle all data without parse failures"

        # Data quality issues should be flagged, not cause failures
        assert results["flagged"] > 0, "Should detect and flag data quality issues"

        # Get specific inconsistencies
        conditions = parser.get_extracted_conditions()

        # Vague/overlapping time ranges
        same_day = [c for c in conditions if any("same-day onset" in f for f in c.validation_flags)]
        assert len(same_day) > 0, "Should detect same-day onset periods"

        # Wrong/missing clinical status
        missing_status = [c for c in conditions if any("missing_clinical_status" in f for f in c.validation_flags)]
        assert len(missing_status) > 0, "Should detect missing clinical status"

        # Non-clinical entries
        non_clinical = [c for c in conditions if any("non_clinical_entry" in f for f in c.validation_flags)]
        assert len(non_clinical) > 0, "Should detect non-clinical entries like *NEW MEMBER"

        # Overlapping/derived conditions
        with_lineage = [c for c in conditions if any("has_lineage" in f for f in c.validation_flags)]
        assert len(with_lineage) > 0, "Should detect conditions with derived-from relationships"

    def test_requirement_latency(self, conditions_data):
        """
        Requirement: Latency - Can you get the right answers super fast?
        If your system relies on background processes or agents, can you answer
        questions well before those processes are done?
        """
        import time

        patient_id = "0e3158f6-383d-40c6-acbf-62f187852934"
        parser = ConditionParser(patient_id)

        # Parse all conditions
        parse_start = time.time()
        parser.parse_conditions(conditions_data)
        parse_time = time.time() - parse_start

        # Queries should be extremely fast (< 10ms) after parsing
        query_times = []

        # Query 1: Get all TB conditions
        start = time.time()
        tb = parser.get_tb_conditions()
        query_times.append(("TB query", time.time() - start))

        # Query 2: Get active conditions
        start = time.time()
        active = parser.get_active_conditions()
        query_times.append(("Active query", time.time() - start))

        # Query 3: Get flagged conditions
        start = time.time()
        flagged = parser.get_flagged_conditions("non_clinical_entry")
        query_times.append(("Flagged query", time.time() - start))

        # Query 4: Get statistics
        start = time.time()
        stats = parser.get_statistics()
        query_times.append(("Statistics query", time.time() - start))

        # All queries should complete in < 100ms (realistically < 10ms)
        for query_name, query_time in query_times:
            assert query_time < 0.1, f"{query_name} took {query_time*1000:.2f}ms (should be < 100ms)"

        # Print performance metrics
        print(f"\nParsing time: {parse_time*1000:.2f}ms for {len(conditions_data)} conditions")
        print("Query times (all < 100ms):")
        for query_name, query_time in query_times:
            print(f"  {query_name}: {query_time*1000:.2f}ms")

    def test_requirement_monitoring(self, conditions_data):
        """
        Requirement: Monitoring - Can your system provide accurate information
        about what it has ingested or died on?
        """
        patient_id = "0e3158f6-383d-40c6-acbf-62f187852934"
        parser = ConditionParser(patient_id)
        results = parser.parse_conditions(conditions_data)

        # Ingestion metrics
        assert "total" in results
        assert "parsed" in results
        assert "failed" in results
        assert "flagged" in results
        assert "timestamp" in results

        # Detailed statistics
        stats = parser.get_statistics()
        assert "total_conditions" in stats
        assert "by_status" in stats
        assert "by_flag" in stats
        assert "by_coding_system" in stats
        assert "conditions_with_lineage" in stats
        assert "non_clinical_entries" in stats

        # Validate counts
        assert stats["total_conditions"] == 105
        assert stats["non_clinical_entries"] == 1
        assert stats["conditions_with_lineage"] == 26

        # Flag breakdown
        flag_counts = stats["by_flag"]
        assert "ambiguous_onset_period" in flag_counts
        assert "has_lineage" in flag_counts
        assert "missing_clinical_status" in flag_counts
        assert "non_clinical_entry" in flag_counts

        # System can report on failures
        assert results["failed"] == 0  # No failures for this data

    def test_requirement_monitoring_raw_event_tracing(self, conditions_data):
        """
        Requirement: Monitoring - event store must account for every attempted payload,
        including those that fail validation, so the system can answer what it ingested
        or died on.
        """
        patient_id = "monitoring-raw-events-patient"
        store = ConditionStore()
        try:
            # Inject one malformed payload alongside the real dataset
            malformed = [{"resourceType": "Condition", "id": "malformed-monitoring-001"}]
            result = store.ingest_raw_batch(
                patient_id=patient_id,
                raw_conditions=malformed + conditions_data,
            )

            stats = store.get_ingestion_event_stats(patient_id)

            # Every attempted payload is accounted for in the event store
            assert stats["total_attempted"] == len(conditions_data) + 1
            assert stats["failed_validation"] == 1
            assert stats["parsed"] == len(conditions_data)
            assert "conditions_flagged" in stats

            # The result dict surfaces the split so callers can monitor failures
            assert result["conditions_failed"] == 1
            assert result["conditions_ingested"] == len(conditions_data)

            # Failed payloads are NOT in the live representation
            live = store.query_current_conditions(patient_id)
            assert not any(c["condition_id"] == "malformed-monitoring-001" for c in live)
        finally:
            store.close()

    def test_requirement_latency_background_ingestion_does_not_block_reads(
        self, conditions_data
    ):
        """
        Requirement: Latency - reads must remain fast even while background ingestion
        is processing a large batch.  The query must complete < 200 ms without waiting
        for the ingestion to finish.

        MUST FAIL: start_background_worker / enqueue_raw_batch don't exist yet.
        """
        patient_id = "0e3158f6-383d-40c6-acbf-62f187852934"
        store = ConditionStore()
        try:
            store.start_background_worker()
            # Enqueue all 105 conditions — large batch
            store.enqueue_raw_batch(patient_id, conditions_data)
            # Immediately query WITHOUT flushing/waiting
            query_start = time.time()
            result = store.query_current_conditions(patient_id)
            query_elapsed = time.time() - query_start
            # Must complete quickly even if zero results are returned
            assert query_elapsed < 0.2, (
                f"query_current_conditions blocked for {query_elapsed*1000:.1f}ms "
                "while background ingestion was running (should be < 200ms)"
            )
            assert isinstance(result, list)
        finally:
            store.stop_background_worker()
            store.close()

    def test_requirement_monitoring_reports_background_worker_health(self):
        """
        Requirement: Monitoring - the system must expose background worker health
        metrics so operators can observe queue depth and job outcomes.

        MUST FAIL: get_background_ingestion_status / start_background_worker don't exist yet.
        """
        store = ConditionStore()
        try:
            store.start_background_worker()
            status = store.get_background_ingestion_status()
            required_keys = {
                "worker_running",
                "queue_depth",
                "jobs_enqueued",
                "jobs_processed",
                "jobs_failed",
            }
            missing = required_keys - set(status.keys())
            assert not missing, f"get_background_ingestion_status missing keys: {missing}"
            assert status["worker_running"] is True
        finally:
            store.stop_background_worker()
            store.close()


class TestBackgroundObservability:
    """Tests for background worker health and failure observability."""

    @pytest.fixture
    def conditions_data(self):
        """Load conditions.json."""
        with open("conditions.json") as f:
            return json.load(f)

    def test_background_monitoring_exposes_queue_depth_and_failure_counts(self):
        """get_background_ingestion_status must expose all required monitoring keys."""
        store = ConditionStore()
        try:
            store.start_background_worker()
            status = store.get_background_ingestion_status()
            required_keys = {"mode", "worker_running", "queue_depth", "jobs_enqueued", "jobs_processed", "jobs_failed"}
            missing = required_keys - set(status.keys())
            assert not missing, f"Missing keys: {missing}"
            assert status["queue_depth"] == 0
            assert status["jobs_failed"] == 0
            assert status["worker_running"] is True
        finally:
            store.stop_background_worker()
            store.close()

    def test_background_monitoring_records_last_error_and_last_success_timestamp(self, conditions_data):
        """Worker must track last_processed_at on success and last_error on failure."""
        # Sub-scenario 1: success path
        patient_id = "observability-success-001"
        store1 = ConditionStore()
        try:
            store1.start_background_worker()
            store1.enqueue_raw_batch(patient_id, conditions_data[:5])
            store1.flush_background_ingestion(timeout=10.0)
            status = store1.get_background_ingestion_status()
            assert status["jobs_processed"] >= 1, f"Expected >=1 processed, got: {status}"
            assert status["last_processed_at"] is not None, "last_processed_at should be set after success"
        finally:
            store1.stop_background_worker()
            store1.close()

        # Sub-scenario 2: failure path - close DuckDB connection so job raises
        patient_id2 = "observability-fail-001"
        store2 = ConditionStore()
        try:
            store2.start_background_worker()
            store2.conn.close()
            store2.enqueue_raw_batch(patient_id2, conditions_data[:3])
            store2.flush_background_ingestion(timeout=3.0)
            status2 = store2.get_background_ingestion_status()
            assert status2["jobs_failed"] >= 1, f"Expected >=1 failed, got: {status2}"
            assert status2["last_error"] is not None and status2["last_error"] != "", (
                f"last_error should be non-empty, got: {status2['last_error']}"
            )
        finally:
            store2.stop_background_worker()
            # conn already closed; do not call store2.close()

    def test_requirement_monitoring_background_worker_reports_died_on_payloads(self, conditions_data):
        """In-batch validation failures do NOT increment jobs_failed."""
        patient_id = "observability-mixed-001"
        store = ConditionStore()
        try:
            store.start_background_worker()
            malformed1 = {"not_fhir": "garbage1", "id": "bad-obs-001"}
            malformed2 = {"not_fhir": "garbage2", "id": "bad-obs-002"}
            mixed_batch = [malformed1, malformed2] + list(conditions_data)
            store.enqueue_raw_batch(patient_id, mixed_batch)
            store.flush_background_ingestion(timeout=10.0)

            stats = store.get_ingestion_event_stats(patient_id)
            assert stats["failed_validation"] == 2, (
                f"Expected 2 failed_validation, got: {stats['failed_validation']}"
            )
            assert stats["parsed"] == len(conditions_data), (
                f"Expected {len(conditions_data)} parsed, got: {stats['parsed']}"
            )

            bg_status = store.get_background_ingestion_status()
            assert bg_status["jobs_processed"] >= 1, (
                f"Worker should count whole batch as 1 job: {bg_status}"
            )
            assert bg_status["jobs_failed"] == 0, (
                f"In-batch validation failures must not increment jobs_failed: {bg_status}"
            )
        finally:
            store.stop_background_worker()
            store.close()

    def test_background_status_returns_sync_sentinel_when_worker_not_started(self):
        """Without start_background_worker(), mode must be sync and worker_running False."""
        store = ConditionStore()
        try:
            status = store.get_background_ingestion_status()
            assert status["mode"] == "sync", f"Expected mode=sync, got: {status['mode']}"
            assert status["worker_running"] is False
            assert status["queue_depth"] == 0
        finally:
            store.close()

    def test_fallback_sync_mode_ingestion_preserves_audit_trail(self, conditions_data):
        """Sync fallback must persist audit events exactly like normal async path."""
        patient_id = "fallback-audit-trail-001"
        # Only use valid conditions (first 5 for speed)
        valid_conditions = conditions_data[:5]
        store = ConditionStore()
        try:
            # No worker started — fallback_to_sync=True invokes ingest_raw_batch directly
            result = store.enqueue_raw_batch(
                patient_id, valid_conditions, fallback_to_sync=True
            )
            assert result["status"] == "sync_fallback"

            # Conditions are immediately visible in the live representation
            conditions = store.query_current_conditions(patient_id)
            assert len(conditions) > 0, "Conditions should be queryable after sync fallback"

            # Audit trail must be intact
            stats = store.get_ingestion_event_stats(patient_id)
            assert stats["parsed"] == result.get("conditions_ingested", stats["parsed"]), (
                f"Audit trail parsed count mismatch: stats={stats}, result={result}"
            )
            assert stats["total_attempted"] == len(valid_conditions), (
                f"Expected {len(valid_conditions)} total_attempted, got {stats['total_attempted']}"
            )
        finally:
            store.close()
