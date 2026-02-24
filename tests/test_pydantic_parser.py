"""Tests for FHIR model schema protection and data quality flags."""
import pytest
from src.models import Coding, Period, ConditionValidator


class TestSchemaProtection:
    """FHIR schema must reject structurally invalid data before it enters the pipeline."""

    def test_coding_rejects_non_uri_system(self):
        with pytest.raises(ValueError, match="must be a valid URI"):
            Coding(code="123", system="not-a-uri")

    def test_period_rejects_end_before_start(self):
        with pytest.raises(ValueError, match="end must be after or equal to start"):
            Period(start="2020-01-10T00:00:00", end="2020-01-01T00:00:00")

    def test_period_allows_same_start_and_end(self):
        p = Period(start="2020-01-01T00:00:00", end="2020-01-01T00:00:00")
        assert p.start == p.end


class TestValidationFlags:
    """All HANDOUT-required inconsistency types must be flagged (not silently dropped)."""

    _BASE = {
        "resourceType": "Condition",
        "id": "flag-test",
        "code": {
            "text": "Test",
            "coding": [{"code": "X", "system": "http://example.org"}],
        },
        "subject": {"reference": "Patient/p1"},
    }

    def _with(self, **kwargs):
        return {**self._BASE, **kwargs}

    def test_invalid_clinical_status_code(self):
        data = self._with(clinicalStatus={
            "coding": [{"code": "INVALID", "system": "http://snomed.info/sct"}],
            "text": "Invalid",
        })
        r = ConditionValidator.validate(data)
        assert r.is_valid
        assert any("invalid_clinical_status" in f for f in r.flags)

    def test_missing_clinical_status(self):
        r = ConditionValidator.validate(self._BASE)
        assert r.is_valid
        assert any("missing_clinical_status" in f for f in r.flags)

    def test_non_clinical_entry(self):
        data = self._with(code={"text": "*NEW MEMBER", "coding": []})
        r = ConditionValidator.validate(data)
        assert r.is_valid
        assert any("non_clinical_entry" in f for f in r.flags)

    def test_ambiguous_onset_same_day(self):
        data = self._with(onsetPeriod={
            "start": "2020-01-01T08:00:00",
            "end": "2020-01-01T12:00:00",
        })
        r = ConditionValidator.validate(data)
        assert r.is_valid
        assert any("ambiguous_onset_period" in f for f in r.flags)

    def test_large_onset_range(self):
        data = self._with(onsetPeriod={
            "start": "2010-01-01T00:00:00",
            "end": "2013-01-01T00:00:00",
        })
        r = ConditionValidator.validate(data)
        assert r.is_valid
        assert any("large_onset_range" in f for f in r.flags)

    def test_missing_onset_dates(self):
        data = self._with(onsetPeriod={})
        r = ConditionValidator.validate(data)
        assert r.is_valid
        assert any("missing_onset_dates" in f for f in r.flags)

    def test_no_code_references(self):
        data = self._with(code={"text": "No coding"})
        r = ConditionValidator.validate(data)
        assert r.is_valid
        assert any("no_code_references" in f for f in r.flags)

    def test_has_lineage(self):
        data = self._with(extension=[{
            "url": "http://example.org/derived",
            "valueRelatedArtifact": {"type": "derived-from", "display": "Condition/parent-1"},
        }])
        r = ConditionValidator.validate(data)
        assert r.is_valid
        assert any("has_lineage" in f for f in r.flags)
