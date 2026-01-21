import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Any
from datetime import datetime
import re


def clean_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Clean and normalize a DataFrame.

    Operations:
    1. Remove fully empty rows
    2. Remove fully empty columns
    3. Trim whitespace from string cells
    4. Normalize text to lowercase (for non-date string columns)
    5. Convert dates to YYYY-MM-DD format
    6. Coerce invalid numeric values to NaN

    Args:
        df: Input DataFrame

    Returns:
        Tuple of (cleaned DataFrame, list of cleaning operations performed)
    """
    cleaning_log = []
    df = df.copy()

    # 1. Remove fully empty rows
    original_rows = len(df)
    df = df.dropna(how='all')
    rows_removed = original_rows - len(df)
    if rows_removed > 0:
        cleaning_log.append({
            "operation": "remove_empty_rows",
            "description": f"Removed {rows_removed} empty rows",
            "count": rows_removed,
        })

    # 2. Remove fully empty columns
    original_cols = len(df.columns)
    empty_cols = df.columns[df.isna().all()].tolist()
    df = df.dropna(axis=1, how='all')
    cols_removed = original_cols - len(df.columns)
    if cols_removed > 0:
        cleaning_log.append({
            "operation": "remove_empty_columns",
            "description": f"Removed {cols_removed} empty columns: {empty_cols}",
            "count": cols_removed,
            "columns": empty_cols,
        })

    # 3. Trim whitespace from string cells
    string_cols = df.select_dtypes(include=['object']).columns
    trimmed_count = 0
    for col in string_cols:
        mask = df[col].notna()
        original = df.loc[mask, col].astype(str)
        trimmed = original.str.strip()
        changes = (original != trimmed).sum()
        trimmed_count += changes
        df.loc[mask, col] = trimmed

    if trimmed_count > 0:
        cleaning_log.append({
            "operation": "trim_whitespace",
            "description": f"Trimmed whitespace from {trimmed_count} cells",
            "count": trimmed_count,
        })

    # 4 & 5. Process columns: detect dates, normalize text, handle numerics
    for col in df.columns:
        df, col_log = process_column(df, col)
        cleaning_log.extend(col_log)

    return df, cleaning_log


def process_column(df: pd.DataFrame, col: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Process a single column for cleaning."""
    log = []

    # Skip if column is already numeric or datetime
    if pd.api.types.is_numeric_dtype(df[col]):
        df, numeric_log = clean_numeric_column(df, col)
        log.extend(numeric_log)
        return df, log

    if pd.api.types.is_datetime64_any_dtype(df[col]):
        return df, log

    # Try to detect if column is a date column
    if is_date_column(df[col]):
        df, date_log = convert_to_date(df, col)
        log.extend(date_log)
        return df, log

    # Try to detect if column should be numeric
    if should_be_numeric(df[col]):
        df, numeric_log = convert_to_numeric(df, col)
        log.extend(numeric_log)
        return df, log

    # For text columns, normalize to lowercase
    df, text_log = normalize_text_column(df, col)
    log.extend(text_log)

    return df, log


def is_date_column(series: pd.Series) -> bool:
    """Check if a column appears to contain dates."""
    # Sample non-null values
    sample = series.dropna().head(100)
    if len(sample) == 0:
        return False

    # Date patterns to check
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY or DD/MM/YYYY
        r'^\d{2}-\d{2}-\d{4}$',  # MM-DD-YYYY or DD-MM-YYYY
        r'^\d{1,2}/\d{1,2}/\d{2,4}$',  # M/D/YY or M/D/YYYY
        r'^\w+ \d{1,2}, \d{4}$',  # Month DD, YYYY
    ]

    date_count = 0
    for val in sample.astype(str):
        for pattern in date_patterns:
            if re.match(pattern, val.strip()):
                date_count += 1
                break

    # If >70% of samples match date patterns, treat as date column
    return date_count / len(sample) > 0.7


def convert_to_date(df: pd.DataFrame, col: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Convert a column to standardized date format (YYYY-MM-DD)."""
    log = []

    original = df[col].copy()

    try:
        # Try to parse dates with multiple formats
        df[col] = pd.to_datetime(df[col], errors='coerce')

        # Count successful conversions
        converted_count = df[col].notna().sum()
        failed_count = original.notna().sum() - converted_count

        if converted_count > 0:
            # Convert to YYYY-MM-DD string format
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            # Replace 'NaT' string with None
            df[col] = df[col].replace('NaT', None)

            log.append({
                "operation": "convert_dates",
                "description": f"Converted column '{col}' to YYYY-MM-DD format ({converted_count} values)",
                "column": col,
                "converted_count": converted_count,
                "failed_count": failed_count,
            })

    except Exception:
        # If conversion fails, keep original
        df[col] = original

    return df, log


def should_be_numeric(series: pd.Series) -> bool:
    """Check if a column should be numeric."""
    sample = series.dropna().head(100)
    if len(sample) == 0:
        return False

    numeric_count = 0
    for val in sample:
        val_str = str(val).strip()
        # Remove common numeric formatting
        val_str = val_str.replace(',', '').replace('$', '').replace('%', '')
        try:
            float(val_str)
            numeric_count += 1
        except (ValueError, TypeError):
            pass

    # If >80% of samples are numeric, treat as numeric column
    return numeric_count / len(sample) > 0.8


def convert_to_numeric(df: pd.DataFrame, col: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Convert a column to numeric, coercing invalid values to NaN."""
    log = []

    original_non_null = df[col].notna().sum()

    # Remove common formatting
    df[col] = df[col].astype(str).str.replace(',', '', regex=False)
    df[col] = df[col].str.replace('$', '', regex=False)
    df[col] = df[col].str.replace('%', '', regex=False)
    df[col] = df[col].str.strip()
    df[col] = df[col].replace(['', 'nan', 'None', 'null', 'N/A', 'n/a', '-'], None)

    df[col] = pd.to_numeric(df[col], errors='coerce')

    new_non_null = df[col].notna().sum()
    coerced_count = original_non_null - new_non_null

    if coerced_count > 0:
        log.append({
            "operation": "coerce_to_numeric",
            "description": f"Converted column '{col}' to numeric ({coerced_count} invalid values set to NaN)",
            "column": col,
            "coerced_count": coerced_count,
        })
    else:
        log.append({
            "operation": "convert_to_numeric",
            "description": f"Converted column '{col}' to numeric type",
            "column": col,
        })

    return df, log


def clean_numeric_column(df: pd.DataFrame, col: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Clean an already numeric column."""
    log = []

    # Check for infinity values
    if pd.api.types.is_float_dtype(df[col]):
        inf_count = np.isinf(df[col]).sum()
        if inf_count > 0:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            log.append({
                "operation": "remove_infinity",
                "description": f"Replaced {inf_count} infinity values with NaN in column '{col}'",
                "column": col,
                "count": inf_count,
            })

    return df, log


def normalize_text_column(df: pd.DataFrame, col: str) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Normalize text column to lowercase."""
    log = []

    mask = df[col].notna()
    if mask.sum() == 0:
        return df, log

    original = df.loc[mask, col].astype(str)
    normalized = original.str.lower()
    changes = (original != normalized).sum()

    if changes > 0:
        df.loc[mask, col] = normalized
        log.append({
            "operation": "normalize_text",
            "description": f"Normalized text to lowercase in column '{col}' ({changes} cells)",
            "column": col,
            "count": changes,
        })

    return df, log
