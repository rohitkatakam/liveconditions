"""Pydantic models for FHIR Condition resources."""
from typing import Optional, List, Literal
from datetime import datetime
from pydantic import BaseModel, field_validator, ConfigDict


class Coding(BaseModel):
    """FHIR Coding - a reference to a code defined by a terminology system."""
    code: Optional[str] = None
    system: str
    display: Optional[str] = None

    @field_validator("system")
    @classmethod
    def system_must_be_uri(cls, v):
        """Validate system is a URI."""
        if not v or not v.startswith(("http://", "urn:")):
            raise ValueError("system must be a valid URI (http:// or urn:)")
        return v


class CodeableConcept(BaseModel):
    """FHIR CodeableConcept - a value that is usually supplied by a terminology system."""
    text: Optional[str] = None
    coding: List[Coding] = []


class Period(BaseModel):
    """FHIR Period - a time period with optional start and end dates."""
    start: Optional[datetime] = None
    end: Optional[datetime] = None

    @field_validator("end")
    @classmethod
    def end_must_be_after_start(cls, v, info):
        """Validate that end date is after or equal to start date."""
        if v and info.data.get("start") and v < info.data["start"]:
            raise ValueError("period end must be after or equal to start")
        return v


class Identifier(BaseModel):
    """FHIR Identifier - an identifier intended for external use."""
    system: str
    value: str


class Reference(BaseModel):
    """FHIR Reference - a reference to another resource."""
    reference: str


class Extension(BaseModel):
    """FHIR Extension - additional content defined by implementations."""
    url: str
    valueRelatedArtifact: Optional[dict] = None


class Condition(BaseModel):
    """FHIR Condition - a clinical condition, problem, diagnosis, or other event, situation, issue, or clinical concept that has risen to a level of concern."""
    
    model_config = ConfigDict(extra="allow")
    
    resourceType: Literal["Condition"]
    id: str
    identifier: List[Identifier] = []
    clinicalStatus: Optional[CodeableConcept] = None
    code: CodeableConcept
    onsetPeriod: Optional[Period] = None
    category: List[CodeableConcept] = []
    subject: Reference
    recorder: Optional[Reference] = None
    extension: Optional[List[Extension]] = None
