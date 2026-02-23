"""Unit tests for FHIR Condition Pydantic parser."""
import json
import pytest
from datetime import datetime
from src.models import (
    Condition,
    ConditionValidator,
    ValidationResult,
    ExtractedCondition,
    Coding,
    CodeableConcept,
    Period,
)


class TestPydanticModels:
    """Test basic Pydantic model parsing."""

    def test_parse_coding_valid(self):
        """Test parsing a valid SNOMED coding."""
        data = {
            "code": "55561003",
            "system": "http://snomed.info/sct",
            "display": "Active",
        }
        coding = Coding(**data)
        assert coding.code == "55561003"
        assert coding.system == "http://snomed.info/sct"
        assert coding.display == "Active"

    def test_parse_coding_invalid_system_no_uri(self):
        """Test that coding rejects non-URI system."""
        data = {"code": "55561003", "system": "invalid", "display": "Active"}
        with pytest.raises(ValueError, match="must be a valid URI"):
            Coding(**data)

    def test_parse_codeable_concept_with_coding(self):
        """Test parsing CodeableConcept with coding."""
        data = {
            "text": "Active",
            "coding": [
                {
                    "code": "55561003",
                    "system": "http://snomed.info/sct",
                    "display": "Active",
                }
            ],
        }
        concept = CodeableConcept(**data)
        assert concept.text == "Active"
        assert len(concept.coding) == 1
        assert concept.coding[0].code == "55561003"

    def test_parse_period_valid_start_and_end(self):
        """Test parsing Period with start and end."""
        data = {
            "start": "2015-09-06T18:35:04.086223",
            "end": "2015-09-06T19:03:52.697709",
        }
        period = Period(**data)
        assert period.start < period.end

    def test_parse_period_invalid_end_before_start(self):
        """Test that Period rejects end before start."""
        data = {
            "start": "2015-09-06T19:03:52.697709",
            "end": "2015-09-06T18:35:04.086223",
        }
        with pytest.raises(ValueError, match="end must be after or equal to start"):
            Period(**data)

    def test_parse_period_same_start_and_end(self):
        """Test Period with same start and end (valid but flagged)."""
        data = {
            "start": "2015-09-06T18:35:04.086223",
            "end": "2015-09-06T18:35:04.086223",
        }
        period = Period(**data)
        assert period.start == period.end


class TestConditionParsing:
    """Test parsing complete Condition resources."""

    def test_parse_minimal_condition(self):
        """Test parsing a minimal valid Condition."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "code": {
                "text": "Test Condition",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
        }
        condition = Condition(**data)
        assert condition.id == "test-123"
        assert condition.code.text == "Test Condition"

    def test_parse_condition_with_clinical_status(self):
        """Test parsing Condition with clinical status."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
        }
        condition = Condition(**data)
        assert condition.clinicalStatus.text == "Active"
        assert condition.clinicalStatus.coding[0].code == "55561003"

    def test_parse_condition_with_multiple_identifiers(self):
        """Test parsing Condition with multiple identifiers."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "identifier": [
                {"system": "urn:uuid:sys1", "value": "id1"},
                {"system": "urn:uuid:sys2", "value": "id2"},
            ],
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
        }
        condition = Condition(**data)
        assert len(condition.identifier) == 2

    def test_parse_condition_with_derived_from_extension(self):
        """Test parsing Condition with derived-from lineage."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
            "extension": [
                {
                    "url": "http://hl7.org/fhir/StructureDefinition/artifact-relatedArtifact",
                    "valueRelatedArtifact": {
                        "type": "derived-from",
                        "display": "Condition/parent-id",
                    },
                }
            ],
        }
        condition = Condition(**data)
        assert len(condition.extension) == 1
        assert condition.extension[0].valueRelatedArtifact["type"] == "derived-from"

    def test_parse_condition_with_multiple_coding_systems(self):
        """Test parsing Condition with multiple coding systems."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "code": {
                "text": "Sleep-disordered breathing",
                "coding": [
                    {
                        "code": "G47.30",
                        "system": "http://hl7.org/fhir/sid/icd-10-cm",
                        "display": "Sleep-disordered breathing",
                    },
                    {
                        "code": "106168000",
                        "system": "http://snomed.info/sct",
                    },
                    {
                        "code": "780.59",
                        "system": "http://terminology.hl7.org/CodeSystem/ICD-9CM-diagnosiscodes",
                        "display": "Sleep-disordered breathing",
                    },
                ],
            },
            "subject": {"reference": "Patient/123"},
        }
        condition = Condition(**data)
        assert len(condition.code.coding) == 3


class TestDataValidation:
    """Test data quality validation."""

    def test_validate_valid_active_condition(self):
        """Test validation of valid active condition."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Test Condition",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert len(result.flags) == 0

    def test_validate_invalid_clinical_status(self):
        """Test flagging invalid clinical status code."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "INVALID-CODE",
                        "system": "http://snomed.info/sct",
                        "display": "Invalid",
                    }
                ],
                "text": "Invalid",
            },
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert any("invalid_clinical_status" in flag for flag in result.flags)

    def test_validate_missing_clinical_status(self):
        """Test flagging missing clinical status."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert any("missing_clinical_status" in flag for flag in result.flags)

    def test_validate_admin_code_new_member(self):
        """Test flagging *NEW MEMBER admin code."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "413322009",
                        "system": "http://snomed.info/sct",
                        "display": "Resolved",
                    }
                ],
                "text": "Resolved",
            },
            "code": {
                "text": "*NEW MEMBER",
                "coding": [{"code": "ADMIN", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert any("non_clinical_entry" in flag for flag in result.flags)

    def test_validate_ambiguous_onset_period_same_start_end(self):
        """Test flagging ambiguous onset period (start == end)."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "onsetPeriod": {
                "start": "2015-09-06T18:35:04.086223",
                "end": "2015-09-06T18:35:04.086223",
            },
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert any("ambiguous_onset_period" in flag for flag in result.flags)

    def test_validate_large_onset_range(self):
        """Test flagging large onset range (>365 days)."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "onsetPeriod": {
                "start": "2010-01-01T00:00:00",
                "end": "2012-01-01T00:00:00",  # 2 years
            },
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert any("large_onset_range" in flag for flag in result.flags)

    def test_validate_missing_onset_dates(self):
        """Test flagging period with no start or end."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "onsetPeriod": {},
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert any("missing_onset_dates" in flag for flag in result.flags)

    def test_validate_no_code_references(self):
        """Test flagging condition with no coding references."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {"text": "Test Condition"},
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert any("no_code_references" in flag for flag in result.flags)

    def test_validate_has_lineage(self):
        """Test flagging condition with derived-from lineage."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/123"},
            "extension": [
                {
                    "url": "http://hl7.org/fhir/StructureDefinition/artifact-relatedArtifact",
                    "valueRelatedArtifact": {
                        "type": "derived-from",
                        "display": "Condition/parent-id",
                    },
                }
            ],
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert any("has_lineage" in flag for flag in result.flags)

    def test_validate_multiple_flags(self):
        """Test condition with multiple data quality issues."""
        data = {
            "resourceType": "Condition",
            "id": "test-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "INVALID",
                        "system": "http://snomed.info/sct",
                        "display": "Invalid",
                    }
                ],
                "text": "Invalid",
            },
            "code": {
                "text": "*NEW MEMBER",
                "coding": [],
            },
            "onsetPeriod": {
                "start": "2015-09-06T18:35:04.086223",
                "end": "2015-09-06T18:35:04.086223",
            },
            "subject": {"reference": "Patient/123"},
        }
        result = ConditionValidator.validate(data)
        assert result.is_valid is True
        assert len(result.flags) >= 3


class TestExtractedCondition:
    """Test extraction and normalization of conditions."""

    def test_extract_condition_basic(self):
        """Test basic extraction of condition fields."""
        condition_data = {
            "resourceType": "Condition",
            "id": "cond-123",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Hypertension",
                "coding": [
                    {
                        "code": "I10",
                        "system": "http://hl7.org/fhir/sid/icd-10-cm",
                        "display": "Essential hypertension",
                    }
                ],
            },
            "subject": {"reference": "Patient/patient-123"},
        }
        condition = Condition(**condition_data)
        patient_id = "patient-123"
        flags = []

        extracted = ExtractedCondition.from_condition(condition, patient_id, flags)
        assert extracted.condition_id == "cond-123"
        assert extracted.patient_id == "patient-123"
        assert extracted.code_text == "Hypertension"
        assert extracted.clinical_status == "Active"
        assert extracted.clinical_status_code == "55561003"
        assert extracted.primary_code == "I10"
        assert extracted.primary_code_system == "http://hl7.org/fhir/sid/icd-10-cm"

    def test_extract_condition_with_alternative_codes(self):
        """Test extraction of condition with multiple coding systems."""
        condition_data = {
            "resourceType": "Condition",
            "id": "cond-456",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Sleep-disordered breathing",
                "coding": [
                    {
                        "code": "G47.30",
                        "system": "http://hl7.org/fhir/sid/icd-10-cm",
                        "display": "Sleep-disordered breathing",
                    },
                    {
                        "code": "106168000",
                        "system": "http://snomed.info/sct",
                    },
                    {
                        "code": "780.59",
                        "system": "http://terminology.hl7.org/CodeSystem/ICD-9CM-diagnosiscodes",
                        "display": "Sleep-disordered breathing",
                    },
                ],
            },
            "subject": {"reference": "Patient/patient-123"},
        }
        condition = Condition(**condition_data)
        patient_id = "patient-123"
        flags = []

        extracted = ExtractedCondition.from_condition(condition, patient_id, flags)
        assert extracted.primary_code == "G47.30"
        assert len(extracted.alternative_codes) == 2
        assert extracted.alternative_codes[0]["code"] == "106168000"

    def test_extract_condition_with_derived_from(self):
        """Test extraction of derived-from lineage."""
        condition_data = {
            "resourceType": "Condition",
            "id": "cond-789",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/patient-123"},
            "extension": [
                {
                    "url": "http://hl7.org/fhir/StructureDefinition/artifact-relatedArtifact",
                    "valueRelatedArtifact": {
                        "type": "derived-from",
                        "display": "Condition/parent-id-1",
                    },
                },
                {
                    "url": "http://hl7.org/fhir/StructureDefinition/artifact-relatedArtifact",
                    "valueRelatedArtifact": {
                        "type": "derived-from",
                        "display": "Condition/parent-id-2",
                    },
                },
            ],
        }
        condition = Condition(**condition_data)
        patient_id = "patient-123"
        flags = []

        extracted = ExtractedCondition.from_condition(condition, patient_id, flags)
        assert len(extracted.derived_from_ids) == 2
        assert "parent-id-1" in extracted.derived_from_ids
        assert "parent-id-2" in extracted.derived_from_ids

    def test_extract_condition_with_validation_flags(self):
        """Test that validation flags are included in extraction."""
        condition_data = {
            "resourceType": "Condition",
            "id": "cond-999",
            "clinicalStatus": {
                "coding": [
                    {
                        "code": "55561003",
                        "system": "http://snomed.info/sct",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "code": {
                "text": "Test",
                "coding": [{"code": "TEST", "system": "http://example.org"}],
            },
            "subject": {"reference": "Patient/patient-123"},
        }
        condition = Condition(**condition_data)
        patient_id = "patient-123"
        flags = ["flag1", "flag2"]

        extracted = ExtractedCondition.from_condition(condition, patient_id, flags)
        assert extracted.validation_flags == flags


class TestRealData:
    """Test parsing with real conditions.json data."""

    @pytest.fixture
    def conditions_data(self):
        """Load conditions.json."""
        with open("conditions.json", "r") as f:
            return json.load(f)

    def test_parse_all_conditions_without_errors(self, conditions_data):
        """Test that all conditions parse successfully."""
        results = []
        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)
            results.append(result)

        # All should parse successfully
        assert all(r.is_valid for r in results)
        assert len(results) > 100  # Should have 100+ conditions

    def test_identify_tuberculosis_conditions(self, conditions_data):
        """Test that TB conditions are properly identified."""
        tb_conditions = []
        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)
            if result.parsed_condition:
                code_text = result.parsed_condition.code.text or ""
                if "tb" in code_text.lower() or "tuberculosis" in code_text.lower():
                    tb_conditions.append(result.parsed_condition)

        # Should find TB conditions
        assert len(tb_conditions) > 0

    def test_identify_non_clinical_entries(self, conditions_data):
        """Test that non-clinical entries are flagged."""
        non_clinical = []
        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)
            if any("non_clinical_entry" in flag for flag in result.flags):
                non_clinical.append(result)

        # Should find at least one non-clinical entry
        assert len(non_clinical) > 0

    def test_identify_single_day_entries(self, conditions_data):
        """Test that single-day onset periods are flagged."""
        single_day = []
        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)
            if any("ambiguous_onset_period" in flag for flag in result.flags):
                single_day.append(result)

        # Should find single-day entries
        assert len(single_day) > 0

    def test_identify_derived_from_conditions(self, conditions_data):
        """Test that conditions with lineage are properly identified."""
        with_lineage = []
        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)
            if any("has_lineage" in flag for flag in result.flags):
                with_lineage.append(result)

        # Should find conditions with lineage
        assert len(with_lineage) > 0

    def test_extract_all_conditions(self, conditions_data):
        """Test extraction of all conditions."""
        extracted_conditions = []
        patient_id = "0e3158f6-383d-40c6-acbf-62f187852934"

        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)
            if result.is_valid and result.parsed_condition:
                extracted = ExtractedCondition.from_condition(
                    result.parsed_condition, patient_id, result.flags
                )
                extracted_conditions.append(extracted)

        # All should extract successfully
        assert len(extracted_conditions) > 100
        # All should have required fields
        for extracted in extracted_conditions:
            assert extracted.condition_id
            assert extracted.patient_id == patient_id
            assert extracted.code_text
            assert extracted.clinical_status
            assert extracted.primary_code_system
            assert extracted.primary_code

    def test_multi_coding_system_parsing(self, conditions_data):
        """Test that conditions with multiple coding systems parse correctly."""
        multi_system = []
        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)
            if result.parsed_condition and len(result.parsed_condition.code.coding) > 1:
                multi_system.append(result.parsed_condition)

        # Should find conditions with multiple coding systems
        assert len(multi_system) > 0
        # Each should have extracted properly
        for condition in multi_system:
            assert len(condition.code.coding) >= 2
            for coding in condition.code.coding:
                assert coding.system
                assert coding.code

    def test_data_consistency_across_identifiers(self, conditions_data):
        """Test that conditions with multiple identifiers parse correctly."""
        multi_identifier = []
        for condition_data in conditions_data:
            result = ConditionValidator.validate(condition_data)
            if result.parsed_condition and len(result.parsed_condition.identifier) > 1:
                multi_identifier.append(result.parsed_condition)

        # Should find conditions with multiple identifiers
        assert len(multi_identifier) > 0
