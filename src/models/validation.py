"""Data quality validation for FHIR Condition resources."""
from typing import List, Optional
from datetime import datetime, timedelta
import re
from .fhir import Condition


class ValidationResult:
    """Result of validating a FHIR Condition."""

    def __init__(
        self,
        is_valid: bool,
        flags: Optional[List[str]] = None,
        parsed_condition: Optional[Condition] = None,
        error_message: Optional[str] = None,
    ):
        self.is_valid = is_valid
        self.flags = flags or []
        self.parsed_condition = parsed_condition
        self.error_message = error_message


class ConditionValidator:
    """Validator for FHIR Condition resources with data quality flagging."""

    VALID_CLINICAL_STATUS_CODES = {
        "55561003",  # Active
        "413322009",  # Resolved
        "385656007",  # In remission
        "723506003",  # Inactive
    }

    ADMIN_CODE_PATTERNS = [
        r"^\*NEW",
        r"^ADMIN",
        r"NEW MEMBER",
        r"PLACEHOLDER",
        r"TEST CODE",
    ]

    @staticmethod
    def validate(raw_payload: dict) -> ValidationResult:
        """
        Parse and validate condition, returning result with flags.

        All records parse successfully with flags as metadata - no failures on parsing.
        """
        flags = []

        try:
            condition = Condition(**raw_payload)
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                flags=[],
                parsed_condition=None,
                error_message=f"Failed to parse condition: {str(e)}",
            )

        # 1. Clinical status validation
        if condition.clinicalStatus and condition.clinicalStatus.coding:
            status_codes = {c.code for c in condition.clinicalStatus.coding}
            if not any(
                code in ConditionValidator.VALID_CLINICAL_STATUS_CODES
                for code in status_codes
            ):
                flags.append(f"invalid_clinical_status: {status_codes}")
        elif not condition.clinicalStatus or not condition.clinicalStatus.coding:
            flags.append("missing_clinical_status")

        # 2. Non-clinical entry detection
        code_text = condition.code.text or ""
        if code_text:
            for pattern in ConditionValidator.ADMIN_CODE_PATTERNS:
                if re.search(pattern, code_text, re.IGNORECASE):
                    flags.append(f"non_clinical_entry: {code_text}")
                    break

        # 3. Onset period validation
        if condition.onsetPeriod:
            period = condition.onsetPeriod
            if period.start and period.end:
                if period.start == period.end:
                    flags.append("ambiguous_onset_period: start equals end")
                # Check for same-day onset/end (ambiguous single-day conditions)
                if period.start.date() == period.end.date():
                    flags.append("ambiguous_onset_period: same-day onset")
                time_diff = abs((period.end - period.start).days)
                if time_diff > 365:
                    flags.append(f"large_onset_range: {time_diff} days")
            if not period.start and not period.end:
                flags.append("missing_onset_dates")

        # 4. No coding references
        if not condition.code.coding:
            flags.append("no_code_references")

        # 5. Derived-from lineage
        if condition.extension:
            derived_from_count = sum(
                1
                for ext in condition.extension
                if ext.valueRelatedArtifact
                and ext.valueRelatedArtifact.get("type") == "derived-from"
            )
            if derived_from_count > 0:
                flags.append(f"has_lineage: {derived_from_count} parent(s)")

        return ValidationResult(
            is_valid=True, flags=flags, parsed_condition=condition
        )
