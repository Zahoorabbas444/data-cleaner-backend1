from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime


class DataStatus(str, Enum):
    READY = "ready"
    WARNING = "warning"
    NOT_READY = "not_ready"


class IssueType(str, Enum):
    MISSING_VALUE = "missing_value"
    DUPLICATE_ROW = "duplicate_row"
    MIXED_TYPE = "mixed_type"
    INVALID_DATE = "invalid_date"
    INVALID_NUMERIC = "invalid_numeric"


class IssueSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ValidationIssue(BaseModel):
    row_number: Optional[int] = None
    column_name: Optional[str] = None
    issue_type: IssueType
    severity: IssueSeverity
    description: str


class ColumnInfo(BaseModel):
    name: str
    original_name: str
    dtype: str
    non_null_count: int
    null_count: int
    unique_count: int
    sample_values: List[Any]


class ValidationSummary(BaseModel):
    total_rows: int
    total_columns: int
    missing_value_count: int
    duplicate_row_count: int
    issue_count: int
    status: DataStatus
    status_reason: str


class ChartData(BaseModel):
    chart_id: str
    chart_type: str
    title: str
    data: Dict[str, Any]
    column_name: str


class UploadResponse(BaseModel):
    job_id: str
    filename: str
    message: str


class ProcessingStatus(BaseModel):
    job_id: str
    status: str
    progress: int
    message: str


class ProcessingResult(BaseModel):
    job_id: str
    validation_summary: ValidationSummary
    columns: List[ColumnInfo]
    issues: List[ValidationIssue]
    charts: List[ChartData]
    preview_data: List[Dict[str, Any]]
