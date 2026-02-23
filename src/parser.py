"""Main parser interface for ingesting and validating FHIR Condition data."""
import json
import logging
from typing import List, Dict, Any
from datetime import datetime
from src.models import (
    Condition,
    ConditionValidator,
    ExtractedCondition,
)


class ConditionParser:
    """Parses and validates FHIR Condition resources with data quality flagging."""

    def __init__(self, patient_id: str):
        """Initialize parser for a specific patient."""
        self.patient_id = patient_id
        self.logger = logging.getLogger(__name__)
        self.extracted_conditions: Dict[str, ExtractedCondition] = {}
        self.validation_results = []

    def parse_conditions(self, conditions_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Parse and validate a batch of FHIR Condition resources.

        Returns a summary of parsing results.
        """
        parsed = 0
        failed = 0
        flagged = 0

        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)

            if not result.is_valid:
                failed += 1
                self.logger.error(
                    f"Failed to parse condition: {result.error_message}",
                    extra={"condition_id": condition_data.get("id")},
                )
                continue

            parsed += 1
            if result.flags:
                flagged += 1

            # Extract normalized condition
            extracted = ExtractedCondition.from_condition(
                result.parsed_condition, self.patient_id, result.flags
            )
            self.extracted_conditions[extracted.condition_id] = extracted
            self.validation_results.append((result.parsed_condition.id, result.flags))

        return {
            "total": len(conditions_data),
            "parsed": parsed,
            "failed": failed,
            "flagged": flagged,
            "timestamp": datetime.now().isoformat(),
        }

    def get_extracted_conditions(self) -> List[ExtractedCondition]:
        """Get all successfully extracted and normalized conditions."""
        return list(self.extracted_conditions.values())

    def get_condition_by_id(self, condition_id: str) -> ExtractedCondition:
        """Retrieve a specific condition by ID."""
        return self.extracted_conditions.get(condition_id)

    def filter_by_text(self, search_text: str) -> List[ExtractedCondition]:
        """Filter conditions by code text (case-insensitive)."""
        search_lower = search_text.lower()
        return [
            c
            for c in self.extracted_conditions.values()
            if search_lower in c.code_text.lower()
        ]

    def get_tb_conditions(self) -> List[ExtractedCondition]:
        """Get all tuberculosis-related conditions."""
        return self.filter_by_text("tuberculosis")

    def get_flagged_conditions(self, flag_type: str = None) -> List[ExtractedCondition]:
        """
        Get conditions with data quality flags.

        If flag_type is provided, filter to conditions with that specific flag.
        """
        if flag_type:
            return [
                c
                for c in self.extracted_conditions.values()
                if any(flag_type in flag for flag in c.validation_flags)
            ]
        return [c for c in self.extracted_conditions.values() if c.validation_flags]

    def get_active_conditions(self) -> List[ExtractedCondition]:
        """Get conditions with Active clinical status."""
        return [
            c
            for c in self.extracted_conditions.values()
            if c.clinical_status == "Active"
        ]

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about parsed conditions."""
        all_conditions = list(self.extracted_conditions.values())

        # Count by status
        status_counts = {}
        for c in all_conditions:
            status_counts[c.clinical_status] = status_counts.get(c.clinical_status, 0) + 1

        # Count flags
        flag_counts = {}
        for c in all_conditions:
            for flag in c.validation_flags:
                # Extract base flag type (before colon)
                base_flag = flag.split(":")[0]
                flag_counts[base_flag] = flag_counts.get(base_flag, 0) + 1

        # Count by coding system
        system_counts = {}
        for c in all_conditions:
            sys = c.primary_code_system
            system_counts[sys] = system_counts.get(sys, 0) + 1

        return {
            "total_conditions": len(all_conditions),
            "by_status": status_counts,
            "by_flag": flag_counts,
            "by_coding_system": system_counts,
            "conditions_with_lineage": len(self.get_flagged_conditions("has_lineage")),
            "non_clinical_entries": len(self.get_flagged_conditions("non_clinical_entry")),
        }
