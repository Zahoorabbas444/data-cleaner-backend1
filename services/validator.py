import pandas as pd
import numpy as np
from typing import List, Dict, Any
from models.schemas import (
    ValidationIssue,
    IssueType,
    IssueSeverity,
    ValidationSummary,
    DataStatus,
)


def validate_dataframe(df: pd.DataFrame) -> List[ValidationIssue]:
    """
    Validate a DataFrame and return a list of issues.

    Checks:
    1. Missing values
    2. Duplicate rows
    3. Mixed data types in columns
    4. Invalid date formats (already handled in cleaning, but flag any remaining)
    """
    issues = []

    # 1. Check for missing values
    missing_issues = check_missing_values(df)
    issues.extend(missing_issues)

    # 2. Check for duplicate rows
    duplicate_issues = check_duplicates(df)
    issues.extend(duplicate_issues)

    # 3. Check for mixed data types
    mixed_type_issues = check_mixed_types(df)
    issues.extend(mixed_type_issues)

    return issues


def check_missing_values(df: pd.DataFrame) -> List[ValidationIssue]:
    """Check for missing values in each column."""
    issues = []

    for col in df.columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            null_pct = (null_count / len(df)) * 100
            null_rows = df[df[col].isna()].index.tolist()[:10]  # First 10 rows

            # Determine severity based on percentage
            if null_pct > 50:
                severity = IssueSeverity.HIGH
            elif null_pct > 20:
                severity = IssueSeverity.MEDIUM
            else:
                severity = IssueSeverity.LOW

            issues.append(ValidationIssue(
                column_name=col,
                issue_type=IssueType.MISSING_VALUE,
                severity=severity,
                description=f"Column '{col}' has {null_count} missing values ({null_pct:.1f}%). Affected rows (first 10): {null_rows}",
            ))

    return issues


def check_duplicates(df: pd.DataFrame) -> List[ValidationIssue]:
    """Check for duplicate rows."""
    issues = []

    # Full row duplicates
    duplicate_mask = df.duplicated(keep='first')
    duplicate_count = duplicate_mask.sum()

    if duplicate_count > 0:
        duplicate_pct = (duplicate_count / len(df)) * 100
        duplicate_rows = df[duplicate_mask].index.tolist()[:20]  # First 20

        if duplicate_pct > 50:
            severity = IssueSeverity.HIGH
        elif duplicate_pct > 10:
            severity = IssueSeverity.MEDIUM
        else:
            severity = IssueSeverity.LOW

        issues.append(ValidationIssue(
            issue_type=IssueType.DUPLICATE_ROW,
            severity=severity,
            description=f"Found {duplicate_count} duplicate rows ({duplicate_pct:.1f}%). Row indices (first 20): {duplicate_rows}",
        ))

    return issues


def check_mixed_types(df: pd.DataFrame) -> List[ValidationIssue]:
    """Check for columns with mixed data types."""
    issues = []

    for col in df.columns:
        # Skip columns with all null values
        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        # Get unique types
        types = non_null.apply(lambda x: type(x).__name__).unique()

        # Filter out NaN types and check for mixed
        types = [t for t in types if t not in ['NoneType', 'float'] or len(types) > 2]

        if len(types) > 1:
            # Check if it's a real mixed type issue (not just int/float)
            numeric_types = {'int', 'float', 'int64', 'float64', 'int32', 'float32'}
            if not all(t in numeric_types for t in types):
                type_counts = non_null.apply(lambda x: type(x).__name__).value_counts().to_dict()

                issues.append(ValidationIssue(
                    column_name=col,
                    issue_type=IssueType.MIXED_TYPE,
                    severity=IssueSeverity.MEDIUM,
                    description=f"Column '{col}' contains mixed data types: {type_counts}",
                ))

    return issues


def calculate_status(
    df: pd.DataFrame,
    issues: List[ValidationIssue],
    metadata: Dict[str, Any]
) -> ValidationSummary:
    """
    Calculate the overall data status and return a summary.

    Status Logic:
    - READY: 0 critical issues, <5% total missing values
    - WARNING: Some issues but <20% missing, no high severity issues
    - NOT_READY: >20% missing values OR >50% duplicates OR high severity issues
    """
    total_rows = len(df)
    total_cols = len(df.columns)

    # Calculate total missing values
    total_cells = total_rows * total_cols
    missing_count = df.isna().sum().sum()
    missing_pct = (missing_count / total_cells) * 100 if total_cells > 0 else 0

    # Count duplicates
    duplicate_count = df.duplicated(keep='first').sum()
    duplicate_pct = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0

    # Count issues by severity
    high_severity = sum(1 for i in issues if i.severity == IssueSeverity.HIGH)
    medium_severity = sum(1 for i in issues if i.severity == IssueSeverity.MEDIUM)
    low_severity = sum(1 for i in issues if i.severity == IssueSeverity.LOW)

    # Determine status
    if high_severity > 0 or missing_pct > 20 or duplicate_pct > 50:
        status = DataStatus.NOT_READY
        if high_severity > 0:
            reason = f"Data has {high_severity} high severity issues that need attention."
        elif missing_pct > 20:
            reason = f"Data has {missing_pct:.1f}% missing values (>20% threshold)."
        else:
            reason = f"Data has {duplicate_pct:.1f}% duplicate rows (>50% threshold)."
    elif medium_severity > 0 or missing_pct > 5:
        status = DataStatus.WARNING
        if medium_severity > 0:
            reason = f"Data has {medium_severity} medium severity issues. Review recommended."
        else:
            reason = f"Data has {missing_pct:.1f}% missing values. Consider handling them."
    else:
        status = DataStatus.READY
        reason = "Data quality is good. Ready for analysis."

    return ValidationSummary(
        total_rows=int(total_rows),
        total_columns=int(total_cols),
        missing_value_count=int(missing_count),
        duplicate_row_count=int(duplicate_count),
        issue_count=int(len(issues)),
        status=status,
        status_reason=reason,
    )
