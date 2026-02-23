"""DuckDB in-memory event store and live representation for condition data."""
import json
import logging
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
import duckdb

from src.models import ExtractedCondition


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

    def ingest_batch(
        self,
        batch_id: Optional[uuid.UUID] = None,
        patient_id: str = None,
        extracted_conditions: List[ExtractedCondition] = None,
        raw_events: List[dict] = None,
    ) -> Dict[str, Any]:
        """
        Ingest a batch of conditions: append to events, rebuild derived tables.
        
        Args:
            batch_id: UUID for this ingestion batch (auto-generated if not provided)
            patient_id: Patient identifier
            extracted_conditions: List of ExtractedCondition objects
            raw_events: List of raw FHIR Condition dicts
            
        Returns:
            Dictionary with ingestion statistics
        """
        if batch_id is None:
            batch_id = uuid.uuid4()
        
        if extracted_conditions is None or not extracted_conditions:
            logger.warning(f"No conditions to ingest for patient {patient_id}")
            return {"status": "no_data", "batch_id": str(batch_id)}
        
        try:
            # 1. Append to condition_ingestion_events
            for extracted, raw in zip(extracted_conditions, raw_events or [None] * len(extracted_conditions)):
                event_id = uuid.uuid4()
                
                # Get validation flags
                flags = extracted.validation_flags if extracted.validation_flags else []
                flags_json = json.dumps(flags)
                
                # Get raw payload
                if raw is not None:
                    raw_payload_json = json.dumps(raw)
                else:
                    # Reconstruct from extracted if raw not provided
                    raw_payload_json = json.dumps({"id": extracted.condition_id})
                
                self.conn.execute("""
                    INSERT INTO condition_ingestion_events (
                        event_id, ingestion_batch_id, patient_id, condition_id, 
                        raw_payload, validation_flags
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, [
                    str(event_id),
                    str(batch_id),
                    patient_id,
                    extracted.condition_id,
                    raw_payload_json,
                    flags_json,
                ])
            
            # 2. Rebuild extracted_conditions_staging (merge with existing, don't delete)
            # For each condition, keep the most recent version
            for extracted in extracted_conditions:
                # Get the most recent ingestion event for this condition
                event_result = self.conn.execute("""
                    SELECT event_id FROM condition_ingestion_events
                    WHERE patient_id = ? AND condition_id = ?
                    ORDER BY timestamp DESC LIMIT 1
                """, [patient_id, extracted.condition_id]).fetchall()
                
                last_event_id = event_result[0][0] if event_result else str(uuid.uuid4())
                
                # Check if this condition already exists in staging
                existing = self.conn.execute("""
                    SELECT extracted_id FROM extracted_conditions_staging
                    WHERE patient_id = ? AND condition_id = ?
                """, [patient_id, extracted.condition_id]).fetchall()
                
                if existing:
                    # Update existing
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
                    # Insert new
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
            
            # 3. Rebuild condition_lineage (delete and recreate for patient)
            self.conn.execute("DELETE FROM condition_lineage WHERE patient_id = ?", [patient_id])
            
            # Get all conditions for this patient and rebuild lineage
            all_conditions = self.conn.execute("""
                SELECT condition_id, derived_from_ids FROM extracted_conditions_staging
                WHERE patient_id = ?
            """, [patient_id]).fetchall()
            
            for condition_id, derived_from_ids_json in all_conditions:
                try:
                    derived_from_ids = json.loads(derived_from_ids_json) if derived_from_ids_json else []
                except (json.JSONDecodeError, TypeError):
                    derived_from_ids = []
                
                if derived_from_ids:
                    for parent_id in derived_from_ids:
                        self.conn.execute("""
                            INSERT INTO condition_lineage (
                                patient_id, parent_condition_id, child_condition_id
                            ) VALUES (?, ?, ?)
                        """, [patient_id, parent_id, condition_id])
            
            logger.info(
                f"Ingested batch {batch_id}: {len(extracted_conditions)} conditions for patient {patient_id}",
                extra={
                    "batch_id": str(batch_id),
                    "patient_id": patient_id,
                    "condition_count": len(extracted_conditions),
                },
            )
            
            return {
                "status": "success",
                "batch_id": str(batch_id),
                "patient_id": patient_id,
                "conditions_ingested": len(extracted_conditions),
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
        condition_ids: List[str],
        correction_type: str = "EXCLUDE",
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Issue a correction: append event to mask/exclude conditions.
        
        Args:
            patient_id: Patient identifier
            condition_ids: IDs of conditions to mask
            correction_type: 'EXCLUDE' or 'RETRACT'
            reason: Optional user rationale
            
        Returns:
            Dictionary with correction details
        """
        affected_count = 0
        
        try:
            for condition_id in condition_ids:
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

    def close(self):
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("ConditionStore connection closed")
