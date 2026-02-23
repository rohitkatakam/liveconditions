"""DuckDB in-memory event store and live representation for FHIR Condition data.

Write path: append-only event log for every ingest attempt (valid or malformed).
Read path:  masked SQL view over the projected staging table — instant, no recompute.
Corrections: append-only events that mask conditions from the live view without deletions.
"""
import json
import logging
import threading
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

import duckdb

from src.models import ExtractedCondition, ConditionValidator
from .async_ingestion import BackgroundIngestionWorker, IngestionJob


logger = logging.getLogger(__name__)


class ConditionStore:
    """Event-sourced condition store backed by an in-memory DuckDB instance."""

    def __init__(self):
        self.conn = duckdb.connect(":memory:")
        self._db_lock = threading.RLock()
        self._worker: Optional[BackgroundIngestionWorker] = None
        self._initialize_schema()
        logger.info("ConditionStore initialised")

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _initialize_schema(self):
        self.conn.executemany("""{}""".format(""), [])  # no-op to keep conn warm
        stmts = [
            # ---- write path ----
            """
            CREATE TABLE condition_ingestion_events (
                event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                ingestion_batch_id UUID NOT NULL,
                patient_id VARCHAR NOT NULL,
                condition_id VARCHAR NOT NULL,
                raw_payload JSON NOT NULL,
                validation_flags JSON NOT NULL,
                ingestion_outcome VARCHAR NOT NULL DEFAULT 'parsed',
                error_detail JSON,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE INDEX idx_cie_patient ON condition_ingestion_events(patient_id, timestamp DESC)",
            "CREATE INDEX idx_cie_condition ON condition_ingestion_events(condition_id)",

            """
            CREATE TABLE correction_events (
                event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                patient_id VARCHAR NOT NULL,
                condition_id_to_mask VARCHAR NOT NULL,
                correction_type VARCHAR NOT NULL,
                correction_reason VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE INDEX idx_ce_patient ON correction_events(patient_id, timestamp DESC)",

            # ---- read path ----
            """
            CREATE TABLE extracted_conditions_staging (
                extracted_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                patient_id VARCHAR NOT NULL,
                condition_id VARCHAR NOT NULL,
                code_text VARCHAR,
                primary_code_system VARCHAR,
                primary_code VARCHAR,
                alternative_codes JSON,
                clinical_status VARCHAR,
                clinical_status_code VARCHAR,
                onset_start TIMESTAMP,
                onset_end TIMESTAMP,
                recorded_by_practitioner VARCHAR,
                derived_from_ids JSON,
                validation_flags JSON,
                last_ingestion_event_id UUID NOT NULL,
                extracted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE INDEX idx_ecs_patient ON extracted_conditions_staging(patient_id, condition_id)",

            """
            CREATE TABLE condition_lineage (
                lineage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                patient_id VARCHAR NOT NULL,
                parent_condition_id VARCHAR NOT NULL,
                child_condition_id VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """,
            "CREATE INDEX idx_cl_parent ON condition_lineage(patient_id, parent_condition_id)",

            # ---- views ----
            """
            CREATE VIEW correction_mask_current AS
            SELECT DISTINCT patient_id, condition_id_to_mask AS masked_condition_id
            FROM correction_events
            WHERE correction_type IN ('EXCLUDE', 'RETRACT')
            """,
            """
            CREATE VIEW current_conditions AS
            SELECT ec.*
            FROM extracted_conditions_staging ec
            LEFT JOIN correction_mask_current cmc
                ON ec.patient_id = cmc.patient_id
                AND ec.condition_id = cmc.masked_condition_id
            WHERE cmc.masked_condition_id IS NULL
            """,
        ]
        for s in stmts:
            s = s.strip()
            if s:
                self.conn.execute(s)
        logger.info("DuckDB schema created")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _upsert_extracted_condition(self, extracted: ExtractedCondition, last_event_id: str, patient_id: str) -> None:
        exists = self.conn.execute(
            "SELECT 1 FROM extracted_conditions_staging WHERE patient_id = ? AND condition_id = ?",
            [patient_id, extracted.condition_id],
        ).fetchone()
        if exists:
            self.conn.execute("""
                UPDATE extracted_conditions_staging SET
                    code_text = ?, primary_code_system = ?, primary_code = ?,
                    alternative_codes = ?, clinical_status = ?, clinical_status_code = ?,
                    onset_start = ?, onset_end = ?, recorded_by_practitioner = ?,
                    derived_from_ids = ?, validation_flags = ?, last_ingestion_event_id = ?
                WHERE patient_id = ? AND condition_id = ?
            """, [
                extracted.code_text, extracted.primary_code_system, extracted.primary_code,
                json.dumps(extracted.alternative_codes), extracted.clinical_status,
                extracted.clinical_status_code, extracted.onset_start, extracted.onset_end,
                extracted.recorded_by_practitioner, json.dumps(extracted.derived_from_ids),
                json.dumps(extracted.validation_flags), last_event_id,
                patient_id, extracted.condition_id,
            ])
        else:
            self.conn.execute("""
                INSERT INTO extracted_conditions_staging (
                    patient_id, condition_id, code_text, primary_code_system, primary_code,
                    alternative_codes, clinical_status, clinical_status_code, onset_start,
                    onset_end, recorded_by_practitioner, derived_from_ids,
                    validation_flags, last_ingestion_event_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                extracted.patient_id, extracted.condition_id, extracted.code_text,
                extracted.primary_code_system, extracted.primary_code,
                json.dumps(extracted.alternative_codes), extracted.clinical_status,
                extracted.clinical_status_code, extracted.onset_start, extracted.onset_end,
                extracted.recorded_by_practitioner, json.dumps(extracted.derived_from_ids),
                json.dumps(extracted.validation_flags), last_event_id,
            ])

    def _rebuild_lineage(self, patient_id: str) -> None:
        self.conn.execute("DELETE FROM condition_lineage WHERE patient_id = ?", [patient_id])
        for cond_id, derived_json in self.conn.execute(
            "SELECT condition_id, derived_from_ids FROM extracted_conditions_staging WHERE patient_id = ?",
            [patient_id],
        ).fetchall():
            try:
                parent_ids = json.loads(derived_json) if derived_json else []
            except (json.JSONDecodeError, TypeError):
                parent_ids = []
            for pid in parent_ids:
                self.conn.execute("""
                    INSERT INTO condition_lineage (patient_id, parent_condition_id, child_condition_id)
                    VALUES (?, ?, ?)
                """, [patient_id, pid, cond_id])

    def _rows_to_dicts(self, rows: List[tuple], view_name: str) -> List[Dict]:
        if not rows:
            return []
        desc = self.conn.execute(f"SELECT * FROM {view_name} LIMIT 0").description
        cols = [d[0] for d in desc]
        return [dict(zip(cols, row)) for row in rows]

    # ------------------------------------------------------------------
    # Ingestion (canonical write path)
    # ------------------------------------------------------------------

    def ingest_raw_batch(
        self,
        patient_id: str,
        raw_conditions: List[dict],
        batch_id: Optional[uuid.UUID] = None,
    ) -> Dict[str, Any]:
        """Persist every payload attempt as an append-only event, then project valid ones."""
        if batch_id is None:
            batch_id = uuid.uuid4()

        total = len(raw_conditions)
        parsed = failed = flagged = 0
        valid_extracted: List[ExtractedCondition] = []

        for raw in raw_conditions:
            event_id = uuid.uuid4()
            try:
                result = ConditionValidator.validate(raw)
                raw_json = json.dumps(raw)
            except Exception as exc:
                failed += 1
                cid = f"unknown-{event_id}"
                self.conn.execute("""
                    INSERT INTO condition_ingestion_events
                        (event_id, ingestion_batch_id, patient_id, condition_id,
                         raw_payload, validation_flags, ingestion_outcome, error_detail)
                    VALUES (?, ?, ?, ?, ?, ?, 'failed_validation', ?)
                """, [str(event_id), str(batch_id), patient_id, cid,
                      json.dumps({"raw": str(raw)}), json.dumps([]),
                      json.dumps({"error_type": "unparseable_payload", "error_message": str(exc),
                                  "source_stage": "ingest_raw_batch.pre_validate"})])
                logger.warning("unparseable_payload", extra={"patient_id": patient_id, "error": str(exc)})
                continue

            if not result.is_valid:
                failed += 1
                cid = raw.get("id", f"unknown-{event_id}") if isinstance(raw, dict) else f"unknown-{event_id}"
                self.conn.execute("""
                    INSERT INTO condition_ingestion_events
                        (event_id, ingestion_batch_id, patient_id, condition_id,
                         raw_payload, validation_flags, ingestion_outcome, error_detail)
                    VALUES (?, ?, ?, ?, ?, ?, 'failed_validation', ?)
                """, [str(event_id), str(batch_id), patient_id, cid, raw_json, json.dumps([]),
                      json.dumps({"error_type": "validation_failed",
                                  "error_message": result.error_message or "parse error",
                                  "source_stage": "ConditionValidator.validate"})])
                logger.warning("malformed_payload", extra={"patient_id": patient_id, "condition_id": cid})
            else:
                parsed += 1
                if result.flags:
                    flagged += 1
                self.conn.execute("""
                    INSERT INTO condition_ingestion_events
                        (event_id, ingestion_batch_id, patient_id, condition_id,
                         raw_payload, validation_flags, ingestion_outcome)
                    VALUES (?, ?, ?, ?, ?, ?, 'parsed')
                """, [str(event_id), str(batch_id), patient_id,
                      result.parsed_condition.id, raw_json, json.dumps(result.flags)])
                valid_extracted.append(
                    ExtractedCondition.from_condition(result.parsed_condition, patient_id, result.flags)
                )

        # Project into read model
        for extracted in valid_extracted:
            row = self.conn.execute("""
                SELECT event_id FROM condition_ingestion_events
                WHERE patient_id = ? AND condition_id = ? AND ingestion_outcome = 'parsed'
                ORDER BY timestamp DESC LIMIT 1
            """, [patient_id, extracted.condition_id]).fetchone()
            last_event_id = row[0] if row else str(uuid.uuid4())
            self._upsert_extracted_condition(extracted, last_event_id, patient_id)

        self._rebuild_lineage(patient_id)

        logger.info("batch_ingested", extra={
            "batch_id": str(batch_id), "patient_id": patient_id,
            "total": total, "parsed": parsed, "failed": failed,
        })
        return {
            "status": "success", "batch_id": str(batch_id), "patient_id": patient_id,
            "total": total, "conditions_ingested": parsed,
            "conditions_failed": failed, "conditions_flagged": flagged,
        }

    # ------------------------------------------------------------------
    # Queries (read path)
    # ------------------------------------------------------------------

    def query_current_conditions(
        self,
        patient_id: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        """Return all unmasked conditions for the patient, fast."""
        with self._db_lock:
            params = [patient_id]
            query = "SELECT * FROM current_conditions WHERE patient_id = ?"
            if filters:
                if "status" in filters:
                    query += " AND clinical_status = ?"
                    params.append(filters["status"])
                if "code_system" in filters:
                    query += " AND primary_code_system = ?"
                    params.append(filters["code_system"])
                if "code" in filters:
                    query += " AND primary_code = ?"
                    params.append(filters["code"])
            query += " ORDER BY extracted_at DESC"
            rows = self.conn.execute(query, params).fetchall()
            return self._rows_to_dicts(rows, "current_conditions")

    def query_active_conditions(self, patient_id: str) -> List[Dict]:
        with self._db_lock:
            rows = self.conn.execute(
                "SELECT * FROM current_conditions WHERE patient_id = ? AND clinical_status = 'Active' ORDER BY extracted_at DESC",
                [patient_id],
            ).fetchall()
            return self._rows_to_dicts(rows, "current_conditions")

    def get_conditions_by_code(self, patient_id: str, code: str, system: Optional[str] = None) -> List[Dict]:
        with self._db_lock:
            if system:
                rows = self.conn.execute(
                    "SELECT * FROM current_conditions WHERE patient_id = ? AND primary_code = ? AND primary_code_system = ?",
                    [patient_id, code, system],
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM current_conditions WHERE patient_id = ? AND primary_code = ?",
                    [patient_id, code],
                ).fetchall()
            return self._rows_to_dicts(rows, "current_conditions")

    def get_condition_lineage(self, patient_id: str, condition_id: str) -> Dict[str, Any]:
        parent_rows = self.conn.execute(
            "SELECT parent_condition_id FROM condition_lineage WHERE patient_id = ? AND child_condition_id = ?",
            [patient_id, condition_id],
        ).fetchall()
        child_rows = self.conn.execute(
            "SELECT child_condition_id FROM condition_lineage WHERE patient_id = ? AND parent_condition_id = ?",
            [patient_id, condition_id],
        ).fetchall()
        return {
            "condition_id": condition_id,
            "parent_ids": [r[0] for r in parent_rows],
            "child_ids": [r[0] for r in child_rows],
        }

    # ------------------------------------------------------------------
    # Corrections (append-only mutations)
    # ------------------------------------------------------------------

    def issue_correction(
        self,
        patient_id: str,
        condition_ids: Optional[List[str]] = None,
        correction_type: str = "EXCLUDE",
        reason: Optional[str] = None,
        condition_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Append correction events to mask conditions from the live view."""
        with self._db_lock:
            resolved: List[str] = list(condition_ids) if condition_ids else []
            if not resolved and condition_code:
                needle = condition_code.lower()
                rows = self.conn.execute(
                    "SELECT condition_id, code_text, primary_code FROM extracted_conditions_staging WHERE patient_id = ?",
                    [patient_id],
                ).fetchall()
                for cid, code_text, primary_code in rows:
                    if (code_text and needle in code_text.lower()) or (primary_code and needle in primary_code.lower()):
                        resolved.append(cid)

            for cid in resolved:
                self.conn.execute("""
                    INSERT INTO correction_events
                        (patient_id, condition_id_to_mask, correction_type, correction_reason)
                    VALUES (?, ?, ?, ?)
                """, [patient_id, cid, correction_type, reason])

            logger.info("correction_issued", extra={
                "patient_id": patient_id, "masked_count": len(resolved),
            })
            return {"status": "success", "patient_id": patient_id, "conditions_masked": len(resolved)}

    # ------------------------------------------------------------------
    # Monitoring & statistics
    # ------------------------------------------------------------------

    def get_ingestion_event_stats(self, patient_id: str) -> Dict[str, Any]:
        """Per-patient payload accounting from the immutable event log."""
        rows = self.conn.execute("""
            SELECT ingestion_outcome, COUNT(*) FROM condition_ingestion_events
            WHERE patient_id = ? GROUP BY ingestion_outcome
        """, [patient_id]).fetchall()
        by_outcome = {r[0]: r[1] for r in rows}
        flagged = self.conn.execute("""
            SELECT COUNT(*) FROM condition_ingestion_events
            WHERE patient_id = ? AND ingestion_outcome = 'parsed' AND validation_flags != '[]'
        """, [patient_id]).fetchone()[0]
        return {
            "patient_id": patient_id,
            "total_attempted": sum(by_outcome.values()),
            "by_outcome": by_outcome,
            "parsed": by_outcome.get("parsed", 0),
            "failed_validation": by_outcome.get("failed_validation", 0),
            "conditions_flagged": flagged,
        }

    def get_statistics(self, patient_id: str) -> Dict[str, Any]:
        with self._db_lock:
            total_raw = self.conn.execute(
                "SELECT COUNT(*) FROM extracted_conditions_staging WHERE patient_id = ?", [patient_id]
            ).fetchone()[0]
            total_current = self.conn.execute(
                "SELECT COUNT(*) FROM current_conditions WHERE patient_id = ?", [patient_id]
            ).fetchone()[0]
            status_rows = self.conn.execute("""
                SELECT clinical_status, COUNT(*) FROM current_conditions
                WHERE patient_id = ? GROUP BY clinical_status
            """, [patient_id]).fetchall()
            system_rows = self.conn.execute("""
                SELECT primary_code_system, COUNT(*) FROM current_conditions
                WHERE patient_id = ? GROUP BY primary_code_system
            """, [patient_id]).fetchall()
            corrections = self.conn.execute(
                "SELECT COUNT(*) FROM correction_events WHERE patient_id = ?", [patient_id]
            ).fetchone()[0]
            flagged = self.conn.execute("""
                SELECT COUNT(*) FROM current_conditions
                WHERE patient_id = ? AND validation_flags != '[]'
            """, [patient_id]).fetchone()[0]
            return {
                "patient_id": patient_id,
                "total_conditions_raw": total_raw,
                "total_conditions_current": total_current,
                "conditions_masked": corrections,
                "by_status": {r[0]: r[1] for r in status_rows},
                "by_coding_system": {r[0]: r[1] for r in system_rows},
                "conditions_with_flags": flagged,
            }

    # ------------------------------------------------------------------
    # Background worker (opt-in async ingestion for latency decoupling)
    # ------------------------------------------------------------------

    def start_background_worker(self, max_queue_size: int = 0, **_kwargs) -> None:
        if self._worker is None:
            self._worker = BackgroundIngestionWorker(
                process_fn=self._execute_ingestion_job,
                max_queue_size=max_queue_size,
            )
        self._worker.start()
        logger.info("background_worker_started")

    def stop_background_worker(self, timeout: float = 5.0) -> None:
        if self._worker is not None:
            self._worker.stop(timeout=timeout)

    def enqueue_raw_batch(
        self,
        patient_id: str,
        raw_conditions: list,
        batch_id=None,
        on_complete=None,
        fallback_to_sync: bool = True,
    ) -> dict:
        import queue as _q
        resolved_id = uuid.uuid4() if batch_id is None else (
            batch_id if isinstance(batch_id, uuid.UUID) else uuid.UUID(str(batch_id))
        )
        if self._worker is None or not self._worker.is_running:
            if not fallback_to_sync:
                raise RuntimeError("Background worker is not started. Call start_background_worker() first.")
            logger.warning("async_worker_not_running_fell_back_to_sync", extra={"patient_id": patient_id})
            with self._db_lock:
                result = self.ingest_raw_batch(patient_id, raw_conditions, resolved_id)
            return {**result, "status": "sync_fallback"}

        job = IngestionJob(patient_id=patient_id, raw_conditions=raw_conditions,
                           batch_id=str(resolved_id), on_complete=on_complete)
        try:
            self._worker.enqueue(job)
        except _q.Full:
            if not fallback_to_sync:
                raise RuntimeError("Background worker queue is full.") from None
            logger.warning("async_worker_queue_full_fell_back_to_sync", extra={"patient_id": patient_id})
            with self._db_lock:
                result = self.ingest_raw_batch(patient_id, raw_conditions, resolved_id)
            return {**result, "status": "sync_fallback"}
        return {"status": "queued", "batch_id": job.batch_id, "patient_id": patient_id,
                "enqueued_count": len(raw_conditions)}

    def flush_background_ingestion(self, timeout: float = 10.0) -> bool:
        if self._worker is None or not self._worker.is_running:
            return True
        return self._worker.flush(timeout=timeout)

    def get_background_ingestion_status(self) -> dict:
        if self._worker is None:
            return {
                "mode": "sync", "worker_running": False, "queue_depth": 0,
                "jobs_enqueued": 0, "jobs_processed": 0, "jobs_failed": 0,
                "last_error": None, "last_processed_at": None, "last_heartbeat": None,
            }
        return {"mode": "async", **self._worker.get_status()}

    def _execute_ingestion_job(self, job: IngestionJob) -> dict:
        with self._db_lock:
            return self.ingest_raw_batch(
                patient_id=job.patient_id,
                raw_conditions=job.raw_conditions,
                batch_id=uuid.UUID(job.batch_id) if job.batch_id else None,
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self):
        self.stop_background_worker()
        if self.conn:
            self.conn.close()
            logger.info("ConditionStore closed")
