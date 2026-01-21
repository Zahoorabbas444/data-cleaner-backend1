import numpy as np
import pandas as pd
from typing import Any, Dict, List


def convert_to_serializable(obj: Any) -> Any:
    """
    Recursively convert numpy/pandas types to JSON-serializable Python types.
    """
    if obj is None:
        return None

    # Handle numpy types
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        if np.isnan(obj):
            return None
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return [convert_to_serializable(item) for item in obj.tolist()]

    # Handle pandas types
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, pd.Timedelta):
        return str(obj)

    # Check for scalar NA values (avoid arrays)
    try:
        if pd.isna(obj) and not isinstance(obj, (list, tuple, np.ndarray)):
            return None
    except (ValueError, TypeError):
        # pd.isna() fails on arrays, handle them in collections section
        pass

    # Handle collections
    if isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [convert_to_serializable(item) for item in obj]

    # Return as-is if already serializable
    return obj


def make_json_safe(data: Any) -> Any:
    """Alias for convert_to_serializable for clarity."""
    return convert_to_serializable(data)
