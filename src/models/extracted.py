"""Extracted and normalized representation of FHIR Condition data."""
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
from .fhir import Condition


class ExtractedCondition(BaseModel):
    """Normalized extraction of a Condition for storage and querying."""

    condition_id: str
    patient_id: str
    code_text: str
    primary_code_system: str
    primary_code: str
    alternative_codes: List[dict] = []
    clinical_status: str
    clinical_status_code: str
    onset_start: Optional[datetime] = None
    onset_end: Optional[datetime] = None
    recorded_by_practitioner: Optional[str] = None
    derived_from_ids: List[str] = []
    validation_flags: List[str] = []

    @staticmethod
    def from_condition(
        condition: Condition, patient_id: str, flags: List[str]
    ) -> "ExtractedCondition":
        """Extract and normalize fields from parsed Condition."""
        primary_code_system = None
        primary_code = None
        alternative_codes = []

        for i, coding in enumerate(condition.code.coding):
            coding_dict = {
                "system": coding.system,
                "code": coding.code,
                "display": coding.display,
            }
            if i == 0:
                primary_code_system = coding.system
                primary_code = coding.code
            else:
                alternative_codes.append(coding_dict)

        derived_from_ids = []
        if condition.extension:
            for ext in condition.extension:
                if (
                    ext.valueRelatedArtifact
                    and ext.valueRelatedArtifact.get("type") == "derived-from"
                ):
                    display = ext.valueRelatedArtifact.get("display", "")
                    if "/" in display:
                        derived_from_ids.append(display.split("/")[-1])

        clinical_status_text = (
            condition.clinicalStatus.text or "Unknown"
            if condition.clinicalStatus
            else "Unknown"
        )
        clinical_status_code = (
            condition.clinicalStatus.coding[0].code
            if condition.clinicalStatus and condition.clinicalStatus.coding
            else "Unknown"
        )

        return ExtractedCondition(
            condition_id=condition.id,
            patient_id=patient_id,
            code_text=condition.code.text or "",
            primary_code_system=primary_code_system or "unknown",
            primary_code=primary_code or "unknown",
            alternative_codes=alternative_codes,
            clinical_status=clinical_status_text,
            clinical_status_code=clinical_status_code,
            onset_start=condition.onsetPeriod.start if condition.onsetPeriod else None,
            onset_end=condition.onsetPeriod.end if condition.onsetPeriod else None,
            recorded_by_practitioner=(
                condition.recorder.reference if condition.recorder else None
            ),
            derived_from_ids=derived_from_ids,
            validation_flags=flags,
        )
