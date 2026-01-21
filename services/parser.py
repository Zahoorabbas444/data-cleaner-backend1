import pandas as pd
import re
from pathlib import Path
from typing import Tuple, List
import numpy as np


class ParserError(Exception):
    """Custom exception for parsing errors."""
    pass


def normalize_column_name(name: str) -> str:
    """
    Normalize a column name:
    - Trim whitespace
    - Convert to lowercase
    - Replace spaces with underscores
    - Remove special characters except underscores
    """
    if pd.isna(name) or name is None:
        return "unnamed_column"

    name = str(name).strip().lower()
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'[^\w]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')

    if not name:
        return "unnamed_column"

    return name


def ensure_unique_columns(columns: List[str]) -> List[str]:
    """Ensure all column names are unique by appending numbers to duplicates."""
    seen = {}
    result = []

    for col in columns:
        if col in seen:
            seen[col] += 1
            result.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            result.append(col)

    return result


def parse_file(file_path: Path) -> Tuple[pd.DataFrame, dict]:
    """
    Parse a CSV or Excel file into a pandas DataFrame.

    Args:
        file_path: Path to the file

    Returns:
        Tuple of (DataFrame, metadata dict)

    Raises:
        ParserError: If file cannot be parsed
    """
    suffix = file_path.suffix.lower()
    original_columns = []

    try:
        if suffix == '.csv':
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'cp1252']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ParserError("Could not decode CSV file. Please ensure it uses UTF-8 encoding.")

        elif suffix in ['.xlsx', '.xls']:
            # Read first sheet only
            df = pd.read_excel(file_path, sheet_name=0, engine='openpyxl')

        else:
            raise ParserError(f"Unsupported file format: {suffix}")

        if df.empty:
            raise ParserError("The uploaded file is empty.")

        # Store original column names before normalization
        original_columns = df.columns.tolist()

        # Normalize column names
        normalized_columns = [normalize_column_name(col) for col in df.columns]
        normalized_columns = ensure_unique_columns(normalized_columns)
        df.columns = normalized_columns

        # Create column mapping
        column_mapping = {
            norm: orig for norm, orig in zip(normalized_columns, original_columns)
        }

        metadata = {
            "original_filename": file_path.name,
            "file_type": suffix,
            "original_row_count": len(df),
            "original_column_count": len(df.columns),
            "column_mapping": column_mapping,
            "original_columns": original_columns,
        }

        return df, metadata

    except ParserError:
        raise
    except pd.errors.EmptyDataError:
        raise ParserError("The uploaded file is empty or contains no data.")
    except Exception as e:
        raise ParserError(f"Failed to parse file: {str(e)}")


def get_column_info(df: pd.DataFrame, column_mapping: dict) -> List[dict]:
    """Extract information about each column."""
    columns_info = []

    for col in df.columns:
        sample_values = df[col].dropna().head(5).tolist()
        # Convert numpy types to Python types for JSON serialization
        sample_values = [
            v.item() if hasattr(v, 'item') else v for v in sample_values
        ]

        info = {
            "name": col,
            "original_name": column_mapping.get(col, col),
            "dtype": str(df[col].dtype),
            "non_null_count": int(df[col].notna().sum()),
            "null_count": int(df[col].isna().sum()),
            "unique_count": int(df[col].nunique()),
            "sample_values": sample_values,
        }
        columns_info.append(info)

    return columns_info
