"""DuckDB in-memory event store and live representation for condition data."""
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
    """
    DuckDB-based event store for FHIR Condition data.
    
    Implements CQRS pattern with:
    - Write path: append-only events tables
    - Read path: fast SQL views for live representation
    - Mutations: user corrections as new events
    """

    def __init__(self):
        """Initialize in-memory DuckDB instance and create schema."""
        self.conn = duckdb.connect(":memory:")
        self._db_lock = threading.RLock()
        self._worker: Optional[BackgroundIngestionWorker] = None
        self._consecutive_failures: int = 0
        self._max_consecutive_failures: Optional[int] = None
        self._initialize_schema()
        logger.info("ConditionStore initialized with in-memory DuckDB")

    def _initialize_schema(self):
        """Create all tables, indexes, and views."""
        # ========== WRITE PATH: Events Tables ==========
        
        # Immutable log of all incoming FHIR Condition data
        self.conn.execute("""
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
                parser_version VARCHAR NOT NULL DEFAULT '1.0',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_condition_ingestion_patient_timestamp 
            ON condition_ingestion_events(patient_id, timestamp DESC)
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_condition_ingestion_condition_id 
            ON condition_ingestion_events(condition_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_condition_ingestion_batch 
            ON condition_ingestion_events(ingestion_batch_id)
        """)
        
        # User-initiated corrections (mutations as events)
        self.conn.execute("""
            CREATE TABLE correction_events (
                event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                patient_id VARCHAR NOT NULL,
                condition_id_to_mask VARCHAR NOT NULL,
                correction_type VARCHAR NOT NULL,
                correction_reason VARCHAR,
                target_codes JSON,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_correction_patient_timestamp 
            ON correction_events(patient_id, timestamp DESC)
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_correction_condition 
            ON correction_events(condition_id_to_mask)
        """)
        
        # ========== READ PATH: Derived Tables & Views ==========
        
        # Normalized, structured representation of conditions
        self.conn.execute("""
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
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_extracted_patient_condition 
            ON extracted_conditions_staging(patient_id, condition_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_extracted_primary_code 
            ON extracted_conditions_staging(patient_id, primary_code)
        """)
        
        # Graph of derived-from relationships
        self.conn.execute("""
            CREATE TABLE condition_lineage (
                lineage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                patient_id VARCHAR NOT NULL,
                parent_condition_id VARCHAR NOT NULL,
                child_condition_id VARCHAR NOT NULL,
                relationship_type VARCHAR DEFAULT 'derived-from',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_lineage_parent 
            ON condition_lineage(patient_id, parent_condition_id)
        """)
        
        self.conn.execute("""
            CREATE INDEX idx_lineage_child 
            ON condition_lineage(patient_id, child_condition_id)
        """)
        
        # ========== VIEWS: Live Representation ==========
        
        # Determine which conditions are masked by corrections
        self.conn.execute("""
            CREATE VIEW correction_mask_current AS
            SELECT DISTINCT
                patient_id,
                condition_id_to_mask AS masked_condition_id,
                MAX(timestamp) AS correction_applied_at
            FROM correction_events
            WHERE correction_type IN ('EXCLUDE', 'RETRACT')
            GROUP BY patient_id, condition_id_to_mask
        """)
        
        # Single source of truth: active conditions with masking applied
        self.conn.execute("""
            CREATE VIEW current_conditions AS
            SELECT
                ec.extracted_id,
                ec.patient_id,
                ec.condition_id,
                ec.code_text,
                ec.primary_code_system,
                ec.primary_code,
                ec.alternative_codes,
                ec.clinical_status,
                ec.clinical_status_code,
                ec.onset_start,
                ec.onset_end,
                ec.recorded_by_practitioner,
                ec.validation_flags,
                COALESCE(cmc.masked_condition_id IS NOT NULL, FALSE) AS is_user_corrected,
                cmc.correction_applied_at,
                ec.last_ingestion_event_id,
                ec.extracted_at
            FROM extracted_conditions_staging ec
            LEFT JOIN correction_mask_current cmc
                ON ec.patient_id = cmc.patient_id
                AND ec.condition_id = cmc.masked_condition_id
            WHERE cmc.masked_condition_id IS NULL
        """)
        
        # Conditions enriched with parent/child lineage
        self.conn.execute("""
            CREATE VIEW conditions_with_lineage AS
            SELECT
                cc.*,
                ARRAY_AGG(DISTINCT cl.parent_condition_id) 
                    FILTER (WHERE cl.parent_condition_id IS NOT NULL) AS parent_ids,
                ARRAY_AGG(DISTINCT cl.child_condition_id) 
                    FILTER (WHERE cl.child_condition_id IS NOT NULL) AS child_ids
            FROM current_conditions cc
            LEFT JOIN condition_lineage cl
                ON cc.patient_id = cl.patient_id
                AND (cc.condition_id = cl.child_condition_id OR cc.condition_id = cl.parent_condition_id)
            GROUP BY 
                cc.extracted_id, cc.patient_id, cc.condition_id, cc.code_text,
                cc.primary_code_system, cc.primary_code, cc.alternative_codes,
                cc.clinical_status, cc.clinical_status_code, cc.onset_start, cc.onset_end,
                cc.recorded_by_practitioner, cc.validation_flags, cc.is_user_corrected,
                cc.correction_applied_at, cc.last_ingestion_event_id, cc.extracted_at
        """)
        
        # Quick filter for only active conditions
        self.conn.execute("""
            CREATE VIEW active_conditions_only AS
            SELECT * FROM current_conditions
            WHERE clinical_status = 'Active'
        """)
        
        logger.info("DuckDB schema created successfully")

    # -------------------------------------------------------------------------
    # Private helpers — shared between ingest_batch and ingest_raw_batch
    # -------------------------------------------------------------------------

    def _upsert_extracted_condition(
        self,
        extracted: "ExtractedCondition",
        last_event_id: str,
        patient_id: str,
    ) -> None:
        """Insert or update one extracted condition in the staging table."""
        existing = self.conn.execute(
            "SELECT extracted_id FROM extracted_conditions_staging WHERE patient_id = ? AND condition_id = ?",
            [patient_id, extracted.condition_id],
        ).fetchall()

        if existing:
            self.conn.execute("""
                UPDATE extracted_conditions_staging
                SET
                    code_text = ?,
                    primary_code_system = ?,
                    primary_code = ?,
                    alternative_codes = ?,
                    clinical_status = ?,
                    clinical_status_code = ?,
                    onset_start = ?,
                    onset_end = ?,
                    recorded_by_practitioner = ?,
                    derived_from_ids = ?,
                    validation_flags = ?,
                    last_ingestion_event_id = ?
                WHERE patient_id = ? AND condition_id = ?
            """, [
                extracted.code_text,
                extracted.primary_code_system,
                extracted.primary_code,
                json.dumps(extracted.alternative_codes),
                extracted.clinical_status,
                extracted.clinical_status_code,
                extracted.onset_start,
                extracted.onset_end,
                extracted.recorded_by_practitioner,
                json.dumps(extracted.derived_from_ids),
                json.dumps(extracted.validation_flags),
                last_event_id,
                patient_id,
                extracted.condition_id,
            ])
        else:
            self.conn.execute("""
                INSERT INTO extracted_conditions_staging (
                    patient_id, condition_id, code_text, primary_code_system,
                    primary_code, alternative_codes, clinical_status,
                    clinical_status_code, onset_start, onset_end,
                    recorded_by_practitioner, derived_from_ids,
                    validation_flags, last_ingestion_event_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                extracted.patient_id,
                extracted.condition_id,
                extracted.code_text,
                extracted.primary_code_system,
                extracted.primary_code,
                json.dumps(extracted.alternative_codes),
                extracted.clinical_status,
                extracted.clinical_status_code,
                extracted.onset_start,
                extracted.onset_end,
                extracted.recorded_by_practitioner,
                json.dumps(extracted.derived_from_ids),
                json.dumps(extracted.validation_flags),
                last_event_id,
            ])

    def _rebuild_lineage(self, patient_id: str) -> None:
        """Delete and recreate condition_lineage rows for the patient."""
        self.conn.execute("DELETE FROM condition_lineage WHERE patient_id = ?", [patient_id])
        for cond_id, derived_json in self.conn.execute(
            "SELECT condition_id, derived_from_ids FROM extracted_conditions_staging WHERE patient_id = ?",
            [patient_id],
        ).fetchall():
            try:
                parent_ids = json.loads(derived_json) if derived_json else []
            except (json.JSONDecodeError, TypeError):
                parent_ids = []
            for parent_id in parent_ids:
                self.conn.execute("""
                    INSERT INTO condition_lineage (patient_id, parent_condition_id, child_condition_id)
                    VALUES (?, ?, ?)
                """, [patient_id, parent_id, cond_id])

    def ingest_batch(
        self,
        batch_id: Optional[uuid.UUID] = None,
        patient_id: str = None,
        extracted_conditions: List[ExtractedCondition] = None,
        raw_events: List[dict] = None,
    ) -> Dict[str, Any]:
        """
        Ingest a batch of pre-parsed conditions: append to events, rebuild derived tables.

        Compatibility path: prefer ``ingest_raw_batch`` for new callers.

        Args:
            batch_id: UUID for this ingestion batch (auto-generated if not provided)
            patient_id: Patient identifier
            extracted_conditions: List of ExtractedCondition objects
            raw_events: List of raw FHIR Condition dicts (used as raw_payload when provided)

        Returns:
            Dictionary with ingestion statistics: status, batch_id, patient_id,
            total, conditions_ingested, conditions_failed, conditions_flagged.
        """
        if batch_id is None:
            batch_id = uuid.uuid4()

        if extracted_conditions is None or not extracted_conditions:
            logger.warning(f"No conditions to ingest for patient {patient_id}")
            return {"status": "no_data", "batch_id": str(batch_id)}

        try:
            # 1. Append to condition_ingestion_events
            for extracted, raw in zip(
                extracted_conditions, raw_events or [None] * len(extracted_conditions)
            ):
                event_id = uuid.uuid4()
                flags = extracted.validation_flags if extracted.validation_flags else []
                raw_payload_json = (
                    json.dumps(raw) if raw is not None
                    else json.dumps({"id": extracted.condition_id})
                )
                self.conn.execute("""
                    INSERT INTO condition_ingestion_events (
                        event_id, ingestion_batch_id, patient_id, condition_id,
                        raw_payload, validation_flags, ingestion_outcome
                    ) VALUES (?, ?, ?, ?, ?, ?, 'parsed')
                """, [
                    str(event_id), str(batch_id), patient_id,
                    extracted.condition_id, raw_payload_json, json.dumps(flags),
                ])

            # 2. Upsert into extracted_conditions_staging
            for extracted in extracted_conditions:
                event_result = self.conn.execute("""
                    SELECT event_id FROM condition_ingestion_events
                    WHERE patient_id = ? AND condition_id = ?
                    ORDER BY timestamp DESC LIMIT 1
                """, [patient_id, extracted.condition_id]).fetchall()
                last_event_id = event_result[0][0] if event_result else str(uuid.uuid4())
                self._upsert_extracted_condition(extracted, last_event_id, patient_id)

            # 3. Rebuild lineage
            self._rebuild_lineage(patient_id)

            logger.info(
                f"Ingested batch {batch_id}: {len(extracted_conditions)} conditions for patient {patient_id}",
                extra={
                    "batch_id": str(batch_id),
                    "patient_id": patient_id,
                    "condition_count": len(extracted_conditions),
                },
            )

            flagged = sum(1 for c in extracted_conditions if c.validation_flags)
            return {
                "status": "success",
                "batch_id": str(batch_id),
                "patient_id": patient_id,
                "total": len(extracted_conditions),
                "conditions_ingested": len(extracted_conditions),
                "conditions_failed": 0,
                "conditions_flagged": flagged,
            }

        except Exception as e:
            logger.error(
                f"Failed to ingest batch {batch_id}: {str(e)}",
                extra={"batch_id": str(batch_id), "error": str(e)},
            )
            raise

    def query_current_conditions(
        self,
        patient_id: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        """
        Query the live representation of current conditions.
        
        Args:
            patient_id: Patient identifier
            filters: Optional dict with keys like 'status', 'code_system', 'code'
            
        Returns:
            List of condition dictionaries
        """
        query = "SELECT * FROM current_conditions WHERE patient_id = ?"
        params = [patient_id]

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

        with self._db_lock:
            result = self.conn.execute(query, params).fetchall()
            return self._rows_to_dicts(result, "current_conditions")

    def query_active_conditions(self, patient_id: str) -> List[Dict]:
        """
        Query only clinically active conditions.
        
        Args:
            patient_id: Patient identifier
            
        Returns:
            List of active condition dictionaries
        """
        result = self.conn.execute(
            "SELECT * FROM active_conditions_only WHERE patient_id = ? ORDER BY extracted_at DESC",
            [patient_id],
        ).fetchall()
        return self._rows_to_dicts(result, "active_conditions_only")

    def get_conditions_by_code(
        self,
        patient_id: str,
        code: str,
        system: Optional[str] = None,
    ) -> List[Dict]:
        """
        Query conditions by SNOMED, ICD-10, or other code system.
        
        Args:
            patient_id: Patient identifier
            code: The condition code
            system: Optional coding system (e.g., 'http://snomed.info/sct')
            
        Returns:
            List of matching conditions
        """
        if system:
            result = self.conn.execute("""
                SELECT * FROM current_conditions
                WHERE patient_id = ? AND primary_code = ? AND primary_code_system = ?
                ORDER BY extracted_at DESC
            """, [patient_id, code, system]).fetchall()
        else:
            result = self.conn.execute("""
                SELECT * FROM current_conditions
                WHERE patient_id = ? AND primary_code = ?
                ORDER BY extracted_at DESC
            """, [patient_id, code]).fetchall()
        
        return self._rows_to_dicts(result, "current_conditions")

    def issue_correction(
        self,
        patient_id: str,
        condition_ids: Optional[List[str]] = None,
        correction_type: str = "EXCLUDE",
        reason: Optional[str] = None,
        condition_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Issue a correction: append event to mask/exclude conditions.

        Accepts either explicit ``condition_ids`` or a fuzzy ``condition_code`` that
        is resolved against the current extracted conditions for the patient.
        
        Args:
            patient_id: Patient identifier
            condition_ids: IDs of conditions to mask (takes priority over condition_code)
            correction_type: 'EXCLUDE' or 'RETRACT'
            reason: Optional user rationale
            condition_code: Free-text or code string; conditions whose code_text or
                primary_code match (case-insensitive) will be masked
            
        Returns:
            Dictionary with correction details
        """
        with self._db_lock:
            # Resolve condition_ids from condition_code if needed
            resolved_ids: List[str] = list(condition_ids) if condition_ids else []
            if not resolved_ids and condition_code:
                needle = condition_code.lower()
                rows = self.conn.execute(
                    "SELECT condition_id, code_text, primary_code FROM extracted_conditions_staging "
                    "WHERE patient_id = ?",
                    [patient_id],
                ).fetchall()
                for cid, code_text, primary_code in rows:
                    if ((code_text and needle in code_text.lower()) or
                            (primary_code and needle in primary_code.lower())):
                        resolved_ids.append(cid)

            affected_count = 0

            try:
                for condition_id in resolved_ids:
                    event_id = uuid.uuid4()

                    self.conn.execute("""
                        INSERT INTO correction_events (
                            event_id, patient_id, condition_id_to_mask,
                            correction_type, correction_reason
                        ) VALUES (?, ?, ?, ?, ?)
                    """, [str(event_id), patient_id, condition_id, correction_type, reason])

                    affected_count += 1

                logger.info(
                    f"Issued correction for patient {patient_id}: {affected_count} conditions masked",
                    extra={
                        "patient_id": patient_id,
                        "affected_count": affected_count,
                        "correction_type": correction_type,
                    },
                )

                return {
                    "status": "success",
                    "patient_id": patient_id,
                    "conditions_masked": affected_count,
                    "correction_type": correction_type,
                }

            except Exception as e:
                logger.error(
                    f"Failed to issue correction: {str(e)}",
                    extra={"patient_id": patient_id, "error": str(e)},
                )
                raise

    def get_condition_lineage(
        self,
        patient_id: str,
        condition_id: str,
    ) -> Dict[str, Any]:
        """
        Get parent/child relationships for a condition.
        
        Args:
            patient_id: Patient identifier
            condition_id: Condition to look up
            
        Returns:
            Dictionary with parent_ids and child_ids
        """
        result = self.conn.execute("""
            SELECT parent_ids, child_ids FROM conditions_with_lineage
            WHERE patient_id = ? AND condition_id = ?
        """, [patient_id, condition_id]).fetchall()
        
        if result:
            parent_ids = result[0][0] or []
            child_ids = result[0][1] or []
            return {
                "condition_id": condition_id,
                "parent_ids": parent_ids,
                "child_ids": child_ids,
            }
        
        return {
            "condition_id": condition_id,
            "parent_ids": [],
            "child_ids": [],
        }

    def get_statistics(self, patient_id: str) -> Dict[str, Any]:
        """
        Get statistics about patient's conditions.
        
        Args:
            patient_id: Patient identifier
            
        Returns:
            Dictionary with counts and breakdowns
        """
        # Total conditions (before masking)
        total_raw = self.conn.execute(
            "SELECT COUNT(*) FROM extracted_conditions_staging WHERE patient_id = ?",
            [patient_id],
        ).fetchall()[0][0]
        
        # Current conditions (after masking)
        total_current = self.conn.execute(
            "SELECT COUNT(*) FROM current_conditions WHERE patient_id = ?",
            [patient_id],
        ).fetchall()[0][0]
        
        # Status breakdown
        status_breakdown = self.conn.execute("""
            SELECT clinical_status, COUNT(*) as count
            FROM current_conditions
            WHERE patient_id = ?
            GROUP BY clinical_status
        """, [patient_id]).fetchall()
        
        status_dict = {row[0]: row[1] for row in status_breakdown}
        
        # Coding system breakdown
        system_breakdown = self.conn.execute("""
            SELECT primary_code_system, COUNT(*) as count
            FROM current_conditions
            WHERE patient_id = ?
            GROUP BY primary_code_system
        """, [patient_id]).fetchall()
        
        system_dict = {row[0]: row[1] for row in system_breakdown}
        
        # Corrections
        corrections_count = self.conn.execute(
            "SELECT COUNT(*) FROM correction_events WHERE patient_id = ?",
            [patient_id],
        ).fetchall()[0][0]
        
        # Validation flags
        flag_counts = self.conn.execute("""
            SELECT COUNT(*) FROM current_conditions
            WHERE patient_id = ? AND validation_flags != '[]'
        """, [patient_id]).fetchall()[0][0]
        
        return {
            "patient_id": patient_id,
            "total_conditions_raw": total_raw,
            "total_conditions_current": total_current,
            "conditions_masked": corrections_count,
            "by_status": status_dict,
            "by_coding_system": system_dict,
            "conditions_with_flags": flag_counts,
        }

    def _rows_to_dicts(self, rows: List[tuple], view_name: str) -> List[Dict]:
        """Convert query result rows to dictionaries."""
        if not rows:
            return []
        
        # Get column names from view
        description = self.conn.execute(f"SELECT * FROM {view_name} LIMIT 0").description
        columns = [desc[0] for desc in description]
        
        return [dict(zip(columns, row)) for row in rows]

    def ingest_raw_batch(
        self,
        patient_id: str = None,
        raw_conditions: List[dict] = None,
        batch_id: Optional[uuid.UUID] = None,
        extracted_conditions: List[ExtractedCondition] = None,
        raw_events: List[dict] = None,
    ) -> Dict[str, Any]:
        """
        Event-first ingestion: persist every incoming payload (valid or malformed)
        as an append-only event, then project valid conditions into the read model.

        This guarantees lossless traceability — every attempted payload is auditable
        even when Pydantic validation fails.

        Args:
            patient_id: Patient identifier
            raw_conditions: Raw FHIR Condition dicts; may include malformed records
            batch_id: UUID for this batch (auto-generated if not provided)
            raw_events: Backward-compat alias for raw_conditions

        Returns:
            Dictionary with ingestion statistics including failed count
        """
        # Support legacy raw_events kwarg alias
        if raw_conditions is None and raw_events is not None:
            raw_conditions = raw_events
        if raw_conditions is None:
            raw_conditions = []

        if batch_id is None:
            batch_id = uuid.uuid4()

        total = len(raw_conditions)
        parsed = 0
        failed = 0
        flagged = 0
        valid_extracted: List[ExtractedCondition] = []

        for raw in raw_conditions:
            event_id = uuid.uuid4()
            try:
                result = ConditionValidator.validate(raw)
                raw_payload_json = json.dumps(raw)
            except Exception as exc:
                # Completely un-parseable payload (e.g. non-dict): persist as failed event
                failed += 1
                condition_id = f"unknown-{event_id}"
                error_detail_json = json.dumps({
                    "error_type": "unparseable_payload",
                    "error_message": str(exc),
                    "source_stage": "ingest_raw_batch.pre_validate",
                })
                try:
                    raw_payload_json = json.dumps(raw)
                except Exception:
                    raw_payload_json = json.dumps({"raw": str(raw)})
                self.conn.execute("""
                    INSERT INTO condition_ingestion_events (
                        event_id, ingestion_batch_id, patient_id, condition_id,
                        raw_payload, validation_flags, ingestion_outcome, error_detail
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    str(event_id), str(batch_id), patient_id, condition_id,
                    raw_payload_json, json.dumps([]),
                    "failed_validation", error_detail_json,
                ])
                logger.warning(
                    "Unparseable payload persisted as failed_validation event",
                    extra={
                        "batch_id": str(batch_id),
                        "patient_id": patient_id,
                        "error": str(exc),
                    },
                )
                continue

            if not result.is_valid:
                failed += 1
                condition_id = raw.get("id", f"unknown-{event_id}") if isinstance(raw, dict) else f"unknown-{event_id}"
                error_detail_json = json.dumps({
                    "error_type": "validation_failed",
                    "error_message": result.error_message or "Unknown parse error",
                    "source_stage": "ConditionValidator.validate",
                })
                self.conn.execute("""
                    INSERT INTO condition_ingestion_events (
                        event_id, ingestion_batch_id, patient_id, condition_id,
                        raw_payload, validation_flags, ingestion_outcome, error_detail
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    str(event_id), str(batch_id), patient_id, condition_id,
                    raw_payload_json, json.dumps([]),
                    "failed_validation", error_detail_json,
                ])
                logger.warning(
                    "Malformed payload persisted as failed_validation event",
                    extra={
                        "batch_id": str(batch_id),
                        "patient_id": patient_id,
                        "condition_id": condition_id,
                        "error": result.error_message,
                    },
                )
            else:
                parsed += 1
                if result.flags:
                    flagged += 1
                self.conn.execute("""
                    INSERT INTO condition_ingestion_events (
                        event_id, ingestion_batch_id, patient_id, condition_id,
                        raw_payload, validation_flags, ingestion_outcome, error_detail
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    str(event_id), str(batch_id), patient_id, result.parsed_condition.id,
                    raw_payload_json, json.dumps(result.flags),
                    "parsed", None,
                ])
                extracted = ExtractedCondition.from_condition(
                    result.parsed_condition, patient_id, result.flags
                )
                valid_extracted.append(extracted)

        # Project valid conditions into the read model
        for extracted in valid_extracted:
            event_result = self.conn.execute("""
                SELECT event_id FROM condition_ingestion_events
                WHERE patient_id = ? AND condition_id = ? AND ingestion_outcome = 'parsed'
                ORDER BY timestamp DESC LIMIT 1
            """, [patient_id, extracted.condition_id]).fetchall()
            last_event_id = event_result[0][0] if event_result else str(uuid.uuid4())
            self._upsert_extracted_condition(extracted, last_event_id, patient_id)

        # Rebuild lineage from staging
        self._rebuild_lineage(patient_id)

        logger.info(
            "Raw batch ingested",
            extra={
                "batch_id": str(batch_id),
                "patient_id": patient_id,
                "total": total,
                "parsed": parsed,
                "failed": failed,
            },
        )

        return {
            "status": "success",
            "batch_id": str(batch_id),
            "patient_id": patient_id,
            "total": total,
            "conditions_ingested": parsed,
            "conditions_failed": failed,
            "conditions_flagged": flagged,
        }

    def get_ingestion_event_stats(self, patient_id: str) -> Dict[str, Any]:
        """
        Return breakdown of raw ingestion event outcomes for a patient.

        Answers the monitoring question: how many payloads arrived, parsed successfully,
        or failed validation — from the immutable event log.
        """
        rows = self.conn.execute("""
            SELECT ingestion_outcome, COUNT(*) as count
            FROM condition_ingestion_events
            WHERE patient_id = ?
            GROUP BY ingestion_outcome
        """, [patient_id]).fetchall()
        by_outcome = {row[0]: row[1] for row in rows}
        flagged_count = self.conn.execute("""
            SELECT COUNT(*) FROM condition_ingestion_events
            WHERE patient_id = ? AND ingestion_outcome = 'parsed' AND validation_flags != '[]'
        """, [patient_id]).fetchone()[0]
        return {
            "patient_id": patient_id,
            "total_attempted": sum(by_outcome.values()),
            "by_outcome": by_outcome,
            "parsed": by_outcome.get("parsed", 0),
            "failed_validation": by_outcome.get("failed_validation", 0),
            "conditions_flagged": flagged_count,
        }

    # -------------------------------------------------------------------------
    # Background worker integration — opt-in async ingestion
    # -------------------------------------------------------------------------

    def start_background_worker(
        self,
        max_queue_size: int = 0,
        max_consecutive_failures: Optional[int] = None,
    ) -> None:
        """Start the background ingestion worker. Idempotent."""
        self._max_consecutive_failures = max_consecutive_failures
        if self._worker is None:
            self._worker = BackgroundIngestionWorker(
                process_fn=self._execute_ingestion_job,
                max_queue_size=max_queue_size,
            )
        self._worker.start()
        logger.info(
            "Background ingestion worker started",
            extra={"max_queue_size": max_queue_size, "worker_running": self._worker.is_running},
        )

    def stop_background_worker(self, timeout: float = 5.0) -> None:
        """Stop the background ingestion worker. Idempotent."""
        if self._worker is not None:
            self._worker.stop(timeout=timeout)
            logger.info(
                "Background ingestion worker stopped",
                extra={"timeout": timeout},
            )

    def enqueue_raw_batch(
        self,
        patient_id: str,
        raw_conditions: list,
        batch_id=None,
        on_complete=None,
        fallback_to_sync: bool = True,
    ) -> dict:
        """Enqueue a raw batch for background ingestion.

        When ``fallback_to_sync=True`` (default), if the worker is not running or
        the queue is full the batch is ingested synchronously so no data is lost.
        When ``fallback_to_sync=False`` and the worker is unavailable, a
        ``RuntimeError`` is raised.
        """
        import queue as _queue_module

        resolved_batch_id = uuid.uuid4() if batch_id is None else (
            batch_id if isinstance(batch_id, uuid.UUID) else uuid.UUID(str(batch_id))
        )

        worker_available = self._worker is not None and self._worker.is_running

        if not worker_available:
            if not fallback_to_sync:
                raise RuntimeError(
                    "Background worker is not started. Call start_background_worker() first."
                )
            # Sync fallback — worker never started or has been stopped
            logger.warning(
                "async_worker_not_running_fell_back_to_sync",
                extra={"patient_id": patient_id, "batch_id": str(resolved_batch_id)},
            )
            with self._db_lock:
                result = self.ingest_raw_batch(
                    patient_id=patient_id,
                    raw_conditions=raw_conditions,
                    batch_id=resolved_batch_id,
                )
            return {**result, "status": "sync_fallback"}

        job = IngestionJob(
            patient_id=patient_id,
            raw_conditions=raw_conditions,
            batch_id=str(resolved_batch_id),
            on_complete=on_complete,
        )
        try:
            self._worker.enqueue(job)
        except _queue_module.Full:
            if not fallback_to_sync:
                raise RuntimeError(
                    f"Background worker queue is full. "
                    f"Increase max_queue_size or use fallback_to_sync=True."
                ) from None
            logger.warning(
                "async_worker_queue_full_fell_back_to_sync",
                extra={"patient_id": patient_id, "batch_id": str(resolved_batch_id)},
            )
            with self._db_lock:
                result = self.ingest_raw_batch(
                    patient_id=patient_id,
                    raw_conditions=raw_conditions,
                    batch_id=resolved_batch_id,
                )
            return {**result, "status": "sync_fallback"}

        return {
            "status": "queued",
            "batch_id": job.batch_id,
            "patient_id": patient_id,
            "enqueued_count": len(raw_conditions),
        }

    def flush_background_ingestion(self, timeout: float = 10.0) -> bool:
        """Block until all queued background jobs are processed. Returns True if fully drained."""
        if self._worker is None or not self._worker.is_running:
            return True
        return self._worker.flush(timeout=timeout)

    def get_background_ingestion_status(self) -> dict:
        """Return health snapshot of the background worker."""
        if self._worker is None:
            return {
                "mode": "sync",
                "worker_running": False,
                "queue_depth": 0,
                "jobs_enqueued": 0,
                "jobs_processed": 0,
                "jobs_failed": 0,
                "last_error": None,
                "last_processed_at": None,
                "last_heartbeat": None,
            }
        return {"mode": "async", **self._worker.get_status()}

    def _execute_ingestion_job(self, job: IngestionJob) -> dict:
        """Process one IngestionJob — called from the worker thread."""
        try:
            with self._db_lock:
                result = self.ingest_raw_batch(
                    patient_id=job.patient_id,
                    raw_conditions=job.raw_conditions,
                    batch_id=uuid.UUID(job.batch_id) if job.batch_id else None,
                )
            # Success — reset consecutive failure counter
            self._consecutive_failures = 0
            return result
        except Exception:
            self._consecutive_failures += 1
            if (
                self._max_consecutive_failures is not None
                and self._consecutive_failures >= self._max_consecutive_failures
            ):
                logger.warning(
                    "async_worker_consecutive_failure_threshold_reached",
                    extra={
                        "consecutive_failures": self._consecutive_failures,
                        "threshold": self._max_consecutive_failures,
                    },
                )
            raise

    def close(self):
        """Close the DuckDB connection."""
        self.stop_background_worker()
        if self._worker is not None and self._worker.is_running:
            logger.warning(
                "background_worker_still_alive_at_db_close",
                extra={"note": "Worker did not stop within timeout; closing DB anyway"},
            )
        if self.conn:
            self.conn.close()
            logger.info("ConditionStore connection closed")
