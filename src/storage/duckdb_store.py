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

        # ========== RECONCILIATION: Internal artifact tables ==========

        # Flat index mapping conditions to their codes (primary + alternative)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS condition_code_index (
                index_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                patient_id VARCHAR NOT NULL,
                condition_id VARCHAR NOT NULL,
                code_system VARCHAR NOT NULL,
                code_value VARCHAR NOT NULL,
                is_primary BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX idx_code_index_patient_condition
            ON condition_code_index(patient_id, condition_id)
        """)
        self.conn.execute("""
            CREATE INDEX idx_code_index_patient_system_value
            ON condition_code_index(patient_id, code_system, code_value)
        """)

        # One row per reconciliation group (canonical + metadata)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS condition_reconciliation_groups (
                group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                patient_id VARCHAR NOT NULL,
                canonical_condition_id VARCHAR NOT NULL,
                is_ambiguous BOOLEAN NOT NULL DEFAULT FALSE,
                decision_basis VARCHAR NOT NULL,
                member_count INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX idx_recon_groups_patient
            ON condition_reconciliation_groups(patient_id)
        """)

        # One row per (group, condition_id) pair
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS condition_reconciliation_members (
                member_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                group_id UUID NOT NULL,
                patient_id VARCHAR NOT NULL,
                condition_id VARCHAR NOT NULL,
                is_canonical BOOLEAN NOT NULL DEFAULT FALSE,
                precedence_rank INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE INDEX idx_recon_members_group
            ON condition_reconciliation_members(group_id)
        """)
        self.conn.execute("""
            CREATE INDEX idx_recon_members_patient_condition
            ON condition_reconciliation_members(patient_id, condition_id)
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

    def _rebuild_code_index(self, patient_id: str) -> None:
        """Delete and recreate condition_code_index rows for a patient."""
        self.conn.execute("DELETE FROM condition_code_index WHERE patient_id = ?", [patient_id])
        rows = self.conn.execute(
            "SELECT condition_id, primary_code_system, primary_code, alternative_codes "
            "FROM extracted_conditions_staging WHERE patient_id = ?",
            [patient_id],
        ).fetchall()
        for cond_id, primary_system, primary_code, alt_codes_json in rows:
            # Insert primary code
            if primary_system and primary_code:
                self.conn.execute("""
                    INSERT INTO condition_code_index
                        (patient_id, condition_id, code_system, code_value, is_primary)
                    VALUES (?, ?, ?, ?, TRUE)
                """, [patient_id, cond_id, primary_system, primary_code])
            # Insert alternative codes
            try:
                alt_codes = json.loads(alt_codes_json) if alt_codes_json else []
            except (json.JSONDecodeError, TypeError):
                alt_codes = []
            for alt in alt_codes:
                alt_system = alt.get("system") if isinstance(alt, dict) else None
                alt_code = alt.get("code") if isinstance(alt, dict) else None
                if alt_system and alt_code:
                    self.conn.execute("""
                        INSERT INTO condition_code_index
                            (patient_id, condition_id, code_system, code_value, is_primary)
                        VALUES (?, ?, ?, ?, FALSE)
                    """, [patient_id, cond_id, alt_system, alt_code])

    # Clinical status precedence ranks for canonicalization
    _CLINICAL_STATUS_RANK: Dict[str, int] = {
        "Active": 4,
        "In remission": 3,
        "Inactive": 2,
        "Resolved": 1,
    }

    @staticmethod
    def _lineage_depth(cid: str, member_set: set, parents_of: Dict[str, set]) -> int:
        """
        Return how many ancestor hops (within member_set) exist above cid.

        A condition with no ancestors in the group has depth 0; a direct child has
        depth 1; a grandchild has depth 2; etc.  Higher depth = preferred canonical.
        """
        visited: set = {cid}
        current_level: set = {cid}
        depth = 0
        while True:
            next_level: set = set()
            for node in current_level:
                for anc in parents_of.get(node, set()):
                    if anc in member_set and anc not in visited:
                        next_level.add(anc)
                        visited.add(anc)
            if not next_level:
                break
            depth += 1
            current_level = next_level
        return depth

    def reconcile_conditions(self, patient_id: str, mode: str = "on") -> Dict[str, Any]:
        """
        Compute reconciliation groups for a patient and persist them in the
        condition_reconciliation_groups / condition_reconciliation_members tables.

        Canonical selection precedence within each overlap group:
          1. Lineage depth: deepest descendant wins (child over parent).
          2. Clinical status rank: Active > In remission > Inactive > Resolved > other.
          3. Extraction timestamp: most recent wins.
          4. Stable tiebreak: alphabetically first condition_id.

        Only conditions present in ``current_conditions`` (i.e. not masked by a
        correction event) are considered as candidates.

        Returns a dict with reconciliation statistics.
        """
        if mode not in {"on", "shadow", "off"}:
            return {"status": "error", "patient_id": patient_id, "error": f"Unknown reconciliation mode: {mode!r}. Use 'on', 'shadow', or 'off'."}

        with self._db_lock:
            # ---- mode="off": clear all reconciliation state and return --------
            if mode == "off":
                self.conn.execute(
                    "DELETE FROM condition_reconciliation_groups WHERE patient_id = ?", [patient_id]
                )
                self.conn.execute(
                    "DELETE FROM condition_reconciliation_members WHERE patient_id = ?", [patient_id]
                )
                self.conn.execute(
                    "DELETE FROM condition_code_index WHERE patient_id = ?", [patient_id]
                )
                logger.info(
                    "reconciliation_disabled",
                    extra={"patient_id": patient_id, "mode": "off"},
                )
                return {
                    "status": "off",
                    "patient_id": patient_id,
                    "groups_computed": 0,
                    "groups_merged": 0,
                    "conditions_deduped": 0,
                    "ambiguous_groups": 0,
                    "lineage_overrides": 0,
                }

            try:
                self._rebuild_code_index(patient_id)

                # --- Step 1: collect current (unmasked) conditions -------------------
                cand_rows = self.conn.execute(
                    "SELECT condition_id, clinical_status, extracted_at "
                    "FROM current_conditions WHERE patient_id = ?",
                    [patient_id],
                ).fetchall()

                if not cand_rows:
                    # Clear any stale reconciliation rows and return empty stats
                    self.conn.execute(
                        "DELETE FROM condition_reconciliation_members WHERE patient_id = ?", [patient_id]
                    )
                    self.conn.execute(
                        "DELETE FROM condition_reconciliation_groups WHERE patient_id = ?", [patient_id]
                    )
                    return {
                        "status": mode if mode == "shadow" else "success",
                        "patient_id": patient_id,
                        "groups_computed": 0,
                        "groups_merged": 0,
                        "conditions_deduped": 0,
                        "ambiguous_groups": 0,
                        "lineage_overrides": 0,
                    }

                cand_ids = [row[0] for row in cand_rows]
                cand_status: Dict[str, str] = {row[0]: (row[1] or "") for row in cand_rows}
                cand_ts: Dict[str, Any] = {row[0]: row[2] for row in cand_rows}
                cand_set: set = set(cand_ids)

                # --- Step 2: find code-sharing pairs (both ends must be candidates) --
                all_pairs = self.conn.execute("""
                SELECT DISTINCT a.condition_id, b.condition_id
                FROM condition_code_index a
                JOIN condition_code_index b
                    ON  a.patient_id   = b.patient_id
                    AND a.code_system  = b.code_system
                    AND a.code_value   = b.code_value
                    AND a.condition_id < b.condition_id
                WHERE a.patient_id = ?
                """, [patient_id]).fetchall()
                pairs = [(a, b) for a, b in all_pairs if a in cand_set and b in cand_set]

                # --- Step 3: union-find to build overlap groups -----------------------
                uf_parent: Dict[str, str] = {cid: cid for cid in cand_ids}

                def find(x: str) -> str:
                    while uf_parent[x] != x:
                        uf_parent[x] = uf_parent[uf_parent[x]]
                        x = uf_parent[x]
                    return x

                def union(x: str, y: str) -> None:
                    rx, ry = find(x), find(y)
                    if rx != ry:
                        if rx < ry:
                            uf_parent[ry] = rx
                        else:
                            uf_parent[rx] = ry

                for cid_a, cid_b in pairs:
                    if cid_a in uf_parent and cid_b in uf_parent:
                        union(cid_a, cid_b)

                groups: Dict[str, list] = {}
                for cid in cand_ids:
                    root = find(cid)
                    groups.setdefault(root, []).append(cid)

                # --- Step 4: fetch lineage restricted to current candidates ----------
                lineage_rows = self.conn.execute(
                    "SELECT parent_condition_id, child_condition_id "
                    "FROM condition_lineage WHERE patient_id = ?",
                    [patient_id],
                ).fetchall()
                parents_of: Dict[str, set] = {}
                for p_id, c_id in lineage_rows:
                    if p_id in cand_set and c_id in cand_set:
                        parents_of.setdefault(c_id, set()).add(p_id)

                # --- Step 5: clear old reconciliation rows for patient ----------------
                self.conn.execute(
                    "DELETE FROM condition_reconciliation_members WHERE patient_id = ?", [patient_id]
                )
                self.conn.execute(
                    "DELETE FROM condition_reconciliation_groups WHERE patient_id = ?", [patient_id]
                )

                # --- Step 6: insert groups + members ----------------------------------
                groups_computed = 0
                groups_merged = 0
                conditions_deduped = 0
                ambiguous_groups = 0
                lineage_overrides = 0

                for root, members in groups.items():
                    members_sorted = sorted(members)
                    member_set = set(members)
                    is_ambiguous = False
                    decision_basis: str
                    canonical: str

                    if len(members) == 1:
                        canonical = members[0]
                        decision_basis = "no_overlap"
                    else:
                        # 1. Lineage depth ranking -----------------------------------
                        depths = {
                            cid: self._lineage_depth(cid, member_set, parents_of)
                            for cid in members
                        }
                        max_depth = max(depths.values())

                        if max_depth > 0:
                            deepest = sorted(
                                cid for cid, d in depths.items() if d == max_depth
                            )
                            canonical = deepest[0]
                            decision_basis = "lineage"
                            lineage_overrides += 1
                        else:
                            # 2. Clinical status rank ---------------------------------
                            ranks = {
                                cid: self._CLINICAL_STATUS_RANK.get(cand_status.get(cid, ""), 0)
                                for cid in members
                            }
                            max_rank = max(ranks.values())
                            best_by_rank = sorted(
                                cid for cid, r in ranks.items() if r == max_rank
                            )

                            if len(best_by_rank) < len(members):
                                canonical = best_by_rank[0]
                                decision_basis = "status_rank"
                            else:
                                # 3. Extraction timestamp ----------------------------
                                ts_values = [
                                    cand_ts[cid]
                                    for cid in members
                                    if cand_ts.get(cid) is not None
                                ]
                                if ts_values:
                                    max_ts = max(ts_values)
                                    best_by_ts = sorted(
                                        cid for cid in members
                                        if cand_ts.get(cid) is not None
                                        and abs((max_ts - cand_ts[cid]).total_seconds()) < 1.0
                                    )
                                else:
                                    best_by_ts = members_sorted

                                if len(best_by_ts) < len(members):
                                    canonical = best_by_ts[0]
                                    decision_basis = "timestamp"
                                else:
                                    # 4. Stable alphabetical tiebreak (all tied) -----
                                    canonical = members_sorted[0]
                                    is_ambiguous = True
                                    decision_basis = "code_overlap_no_lineage"
                                    ambiguous_groups += 1

                        groups_merged += 1
                        conditions_deduped += len(members) - 1

                    group_id = str(uuid.uuid4())
                    self.conn.execute("""
                        INSERT INTO condition_reconciliation_groups
                            (group_id, patient_id, canonical_condition_id, is_ambiguous,
                             decision_basis, member_count)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, [group_id, patient_id, canonical, is_ambiguous, decision_basis, len(members)])

                    for rank, cid in enumerate(members_sorted):
                        # In shadow mode, never mark any condition as canonical
                        is_canonical_val = (cid == canonical) if mode == "on" else False
                        self.conn.execute("""
                            INSERT INTO condition_reconciliation_members
                                (group_id, patient_id, condition_id, is_canonical, precedence_rank)
                            VALUES (?, ?, ?, ?, ?)
                        """, [group_id, patient_id, cid, is_canonical_val, rank])

                    groups_computed += 1

                logger.info(
                    "reconcile_conditions completed",
                    extra={
                        "patient_id": patient_id,
                        "mode": mode,
                        "groups_computed": groups_computed,
                        "groups_merged": groups_merged,
                        "ambiguous_groups": ambiguous_groups,
                        "lineage_overrides": lineage_overrides,
                    },
                )

                return {
                    "status": "shadow" if mode == "shadow" else "success",
                    "patient_id": patient_id,
                    "groups_computed": groups_computed,
                    "groups_merged": groups_merged,
                    "conditions_deduped": conditions_deduped,
                    "ambiguous_groups": ambiguous_groups,
                    "lineage_overrides": lineage_overrides,
                }

            except Exception as exc:
                logger.error(
                    "reconciliation_failed_reverting_to_unreconciled",
                    extra={"patient_id": patient_id, "error": str(exc)},
                )
                # Clean up ALL partial state so query always falls back to unreconciled
                self.conn.execute(
                    "DELETE FROM condition_reconciliation_groups WHERE patient_id = ?", [patient_id]
                )
                self.conn.execute(
                    "DELETE FROM condition_reconciliation_members WHERE patient_id = ?", [patient_id]
                )
                self.conn.execute(
                    "DELETE FROM condition_code_index WHERE patient_id = ?", [patient_id]
                )
                return {"status": "error", "patient_id": patient_id, "error": str(exc)}

    def get_reconciliation_mode(self, patient_id: str) -> str:
        """
        Return the current reconciliation mode for a patient.

        Returns:
            ``"off"``    — no reconciliation groups exist.
            ``"shadow"`` — groups exist but no member is marked canonical.
            ``"on"``     — at least one member is marked canonical.
        """
        with self._db_lock:
            group_count = self.conn.execute(
                "SELECT COUNT(*) FROM condition_reconciliation_groups WHERE patient_id = ?",
                [patient_id],
            ).fetchone()[0]
            if group_count == 0:
                return "off"
            canonical_count = self.conn.execute(
                "SELECT COUNT(*) FROM condition_reconciliation_members "
                "WHERE patient_id = ? AND is_canonical = TRUE",
                [patient_id],
            ).fetchone()[0]
            if canonical_count == 0:
                return "shadow"
            return "on"

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

        When reconciliation groups exist for the patient (i.e. ``reconcile_conditions``
        has been called), only the canonical condition per group is returned.  This
        provides a deduplicated, lineage-aware view of current patient state.

        When no reconciliation groups exist yet, all unmasked conditions are returned
        (backward-compatible behaviour for callers that never call reconcile).

        Args:
            patient_id: Patient identifier
            filters: Optional dict with keys like 'status', 'code_system', 'code'

        Returns:
            List of condition dictionaries (same schema as before)
        """
        with self._db_lock:
            # Check whether reconciliation groups exist for this patient
            group_count = self.conn.execute(
                "SELECT COUNT(*) FROM condition_reconciliation_groups WHERE patient_id = ?",
                [patient_id],
            ).fetchone()[0]

            params = [patient_id]

            # In shadow mode, groups exist but no member is canonical — fall back to unreconciled.
            canonical_count = 0
            if group_count > 0:
                canonical_count = self.conn.execute(
                    "SELECT COUNT(*) FROM condition_reconciliation_members "
                    "WHERE patient_id = ? AND is_canonical = TRUE",
                    [patient_id],
                ).fetchone()[0]

            if group_count == 0 or canonical_count == 0:
                # No reconciliation run yet (off) or shadow mode — return all unmasked conditions
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
            else:
                # Return only the canonical condition per reconciliation group,
                # while still respecting the correction mask (current_conditions view).
                query = """
                    SELECT cc.* FROM current_conditions cc
                    INNER JOIN condition_reconciliation_members crm
                        ON cc.patient_id = crm.patient_id
                        AND cc.condition_id = crm.condition_id
                    WHERE cc.patient_id = ?
                      AND crm.is_canonical = TRUE
                """
                if filters:
                    if "status" in filters:
                        query += " AND cc.clinical_status = ?"
                        params.append(filters["status"])
                    if "code_system" in filters:
                        query += " AND cc.primary_code_system = ?"
                        params.append(filters["code_system"])
                    if "code" in filters:
                        query += " AND cc.primary_code = ?"
                        params.append(filters["code"])
                query += " ORDER BY cc.extracted_at DESC"

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

                # Invalidate stale reconciliation groups: a corrected condition may have been
                # the stored canonical. Clear groups so query_current_conditions falls back to
                # the unreconciled path (which already respects the correction mask). The caller
                # should re-run reconcile_conditions() to rebuild groups after corrections.
                if affected_count > 0:
                    self.conn.execute(
                        "DELETE FROM condition_reconciliation_members WHERE patient_id = ?", [patient_id]
                    )
                    self.conn.execute(
                        "DELETE FROM condition_reconciliation_groups WHERE patient_id = ?", [patient_id]
                    )
                    self.conn.execute(
                        "DELETE FROM condition_code_index WHERE patient_id = ?", [patient_id]
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
        with self._db_lock:
            return self._get_statistics_locked(patient_id)

    def _get_statistics_locked(self, patient_id: str) -> Dict[str, Any]:
        """Thread-unsafe inner implementation — caller must hold self._db_lock."""
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

        # Reconciliation statistics (populated after reconcile_conditions())
        recon_groups_total = self.conn.execute(
            "SELECT COUNT(*) FROM condition_reconciliation_groups WHERE patient_id = ?",
            [patient_id],
        ).fetchone()[0]
        recon_groups_merged = self.conn.execute(
            "SELECT COUNT(*) FROM condition_reconciliation_groups WHERE patient_id = ? AND member_count > 1",
            [patient_id],
        ).fetchone()[0]
        recon_conditions_deduped = self.conn.execute(
            "SELECT COALESCE(SUM(member_count - 1), 0) FROM condition_reconciliation_groups "
            "WHERE patient_id = ? AND member_count > 1",
            [patient_id],
        ).fetchone()[0]
        recon_ambiguous = self.conn.execute(
            "SELECT COUNT(*) FROM condition_reconciliation_groups WHERE patient_id = ? AND is_ambiguous = TRUE",
            [patient_id],
        ).fetchone()[0]
        recon_lineage_overrides = self.conn.execute(
            "SELECT COUNT(*) FROM condition_reconciliation_groups "
            "WHERE patient_id = ? AND decision_basis IN ('lineage', 'lineage_tiebreak')",
            [patient_id],
        ).fetchone()[0]

        return {
            "patient_id": patient_id,
            "total_conditions_raw": total_raw,
            "total_conditions_current": total_current,
            "conditions_masked": corrections_count,
            "by_status": status_dict,
            "by_coding_system": system_dict,
            "conditions_with_flags": flag_counts,
            "reconciliation_groups_total": int(recon_groups_total),
            "reconciliation_groups_merged": int(recon_groups_merged),
            "reconciliation_conditions_deduped": int(recon_conditions_deduped),
            "reconciliation_ambiguous_groups": int(recon_ambiguous),
            "reconciliation_lineage_overrides": int(recon_lineage_overrides),
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
        patient_id: str,
        raw_conditions: List[dict],
        batch_id: Optional[uuid.UUID] = None,
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

        Returns:
            Dictionary with ingestion statistics including failed count
        """
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
