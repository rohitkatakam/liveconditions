"""FastMCP tools for querying and correcting patient condition data."""
import json
import logging
from typing import Any, Optional, Dict, List
from src.storage import ConditionStore


logger = logging.getLogger(__name__)


class ConditionTools:
    """FastMCP-compatible tools for condition RAG and corrections."""
    
    def __init__(self, store: ConditionStore):
        """
        Initialize tools with a ConditionStore instance.
        
        Args:
            store: ConditionStore for all queries and mutations
        """
        self.store = store
    
    def query_conditions(
        self,
        patient_id: str,
        status: Optional[str] = None,
        code_system: Optional[str] = None,
        code: Optional[str] = None,
        include_lineage: bool = False,
    ) -> Dict[str, Any]:
        """
        RAG tool: Query the live representation of patient conditions.
        
        This tool provides instant access to the current, deduplicated condition state.
        Respects user corrections (masked conditions are excluded).
        Includes validation flags for data quality awareness.
        
        Args:
            patient_id: Patient identifier (required)
            status: Filter by clinical status (e.g., 'Active', 'Resolved')
            code_system: Filter by coding system (e.g., 'http://snomed.info/sct')
            code: Filter by specific code
            include_lineage: If True, include parent/child relationships
            
        Returns:
            Dictionary with:
                - 'patient_id': Patient identifier
                - 'conditions': List of conditions
                - 'total_count': Number of conditions returned
                - 'has_corrections': Whether user has issued corrections
                - 'metadata': Statistics about the query result
        """
        try:
            filters = {}
            if status:
                filters["status"] = status
            if code_system:
                filters["code_system"] = code_system
            if code:
                filters["code"] = code
            
            # Query conditions
            conditions = self.store.query_current_conditions(patient_id, filters)
            
            # Optionally enrich with lineage
            if include_lineage and conditions:
                for condition in conditions:
                    lineage = self.store.get_condition_lineage(
                        patient_id, condition["condition_id"]
                    )
                    condition["parent_ids"] = lineage.get("parent_ids", [])
                    condition["child_ids"] = lineage.get("child_ids", [])
            
            # Get statistics
            stats = self.store.get_statistics(patient_id)
            has_corrections = stats.get("conditions_masked", 0) > 0
            
            logger.info(
                f"Queried conditions for patient {patient_id}: {len(conditions)} results",
                extra={
                    "patient_id": patient_id,
                    "result_count": len(conditions),
                    "filters_applied": bool(filters),
                },
            )
            
            return {
                "status": "success",
                "patient_id": patient_id,
                "conditions": self._serialize_conditions(conditions),
                "total_count": len(conditions),
                "has_corrections": has_corrections,
                "metadata": {
                    "total_raw_conditions": stats.get("total_conditions_raw"),
                    "total_current_conditions": stats.get("total_conditions_current"),
                    "conditions_with_quality_flags": stats.get("conditions_with_flags"),
                    "by_status": stats.get("by_status"),
                },
            }
        
        except Exception as e:
            logger.error(
                f"Failed to query conditions for patient {patient_id}: {str(e)}",
                extra={"patient_id": patient_id, "error": str(e)},
            )
            return {
                "status": "error",
                "patient_id": patient_id,
                "error": str(e),
            }
    
    def issue_correction(
        self,
        patient_id: str,
        condition_ids: List[str],
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Mutation tool: Issue a user correction to mask/exclude conditions.
        
        This tool appends a correction event to the event store without deleting
        original clinical data. The correction is immediately reflected in the live
        representation (conditions are hidden from query results).
        
        Original clinical records remain in the audit log for compliance.
        
        Args:
            patient_id: Patient identifier (required)
            condition_ids: List of condition IDs to mask
            reason: Optional user rationale for the correction
            
        Returns:
            Dictionary with:
                - 'status': 'success' or 'error'
                - 'conditions_masked': Number of conditions excluded
                - 'patient_id': Patient identifier
                - 'updated_condition_count': Total active conditions after correction
        """
        try:
            if not condition_ids:
                return {
                    "status": "error",
                    "error": "No condition IDs provided",
                }
            
            # Append correction events
            result = self.store.issue_correction(
                patient_id=patient_id,
                condition_ids=condition_ids,
                correction_type="EXCLUDE",
                reason=reason,
            )
            
            # Get updated statistics
            stats = self.store.get_statistics(patient_id)
            
            logger.info(
                f"Issued correction for patient {patient_id}: {len(condition_ids)} conditions masked",
                extra={
                    "patient_id": patient_id,
                    "masked_count": len(condition_ids),
                    "reason": reason,
                },
            )
            
            return {
                "status": result["status"],
                "patient_id": patient_id,
                "conditions_masked": result["conditions_masked"],
                "updated_condition_count": stats.get("total_conditions_current"),
                "message": f"Successfully masked {result['conditions_masked']} condition(s)",
            }
        
        except Exception as e:
            logger.error(
                f"Failed to issue correction for patient {patient_id}: {str(e)}",
                extra={"patient_id": patient_id, "error": str(e)},
            )
            return {
                "status": "error",
                "patient_id": patient_id,
                "error": str(e),
            }
    
    @staticmethod
    def _serialize_conditions(conditions: List[Dict]) -> List[Dict]:
        """
        Serialize conditions for JSON response, handling datetime and JSON fields.
        
        Args:
            conditions: Raw condition dictionaries from DuckDB
            
        Returns:
            Serializable condition dictionaries
        """
        serialized = []
        
        for condition in conditions:
            serialized_cond = {}
            
            for key, value in condition.items():
                # Skip None values
                if value is None:
                    continue
                
                # Convert JSON string fields to dicts/lists
                if key in ["alternative_codes", "validation_flags", "derived_from_ids"] and isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                # Convert datetime to ISO string
                if hasattr(value, "isoformat"):
                    value = value.isoformat()
                
                serialized_cond[key] = value
            
            serialized.append(serialized_cond)
        
        return serialized
