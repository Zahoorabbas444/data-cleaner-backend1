import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import io
import base64
import uuid

from models.schemas import ChartData


# Column type detection thresholds
CATEGORICAL_UNIQUE_THRESHOLD = 20
MIN_VALUES_FOR_CHART = 2


def detect_column_type(series: pd.Series) -> str:
    """
    Detect the semantic type of a column.

    Returns: 'categorical', 'temporal', 'numerical', or 'unknown'
    """
    # Skip if too few non-null values
    non_null = series.dropna()
    if len(non_null) < MIN_VALUES_FOR_CHART:
        return 'unknown'

    # Check for datetime
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'temporal'

    # Check if string column looks like dates
    if series.dtype == 'object':
        # Check if values look like dates (YYYY-MM-DD format from cleaning)
        sample = non_null.head(20).astype(str)
        date_like = sample.str.match(r'^\d{4}-\d{2}-\d{2}').sum()
        if date_like / len(sample) > 0.7:
            return 'temporal'

    # Check for numeric
    if pd.api.types.is_numeric_dtype(series):
        # If few unique values, treat as categorical
        if series.nunique() <= CATEGORICAL_UNIQUE_THRESHOLD:
            return 'categorical'
        return 'numerical'

    # String/object columns
    if series.dtype == 'object':
        if series.nunique() <= CATEGORICAL_UNIQUE_THRESHOLD:
            return 'categorical'
        # High cardinality text - not useful for charts
        return 'unknown'

    return 'unknown'


def generate_charts(df: pd.DataFrame) -> List[ChartData]:
    """
    Generate appropriate charts based on column types.

    Chart Rules:
    - Categorical: Bar chart (top 10 frequencies)
    - Temporal: Line chart (trend over time)
    - Numerical: Histogram + Box plot
    - Data Quality: Missingness heatmap
    """
    charts = []

    # Generate missingness heatmap
    missingness_chart = generate_missingness_chart(df)
    if missingness_chart:
        charts.append(missingness_chart)

    # Analyze each column
    for col in df.columns:
        col_type = detect_column_type(df[col])

        if col_type == 'categorical':
            chart = generate_bar_chart(df, col)
            if chart:
                charts.append(chart)

        elif col_type == 'temporal':
            chart = generate_line_chart(df, col)
            if chart:
                charts.append(chart)

        elif col_type == 'numerical':
            # Histogram
            hist_chart = generate_histogram(df, col)
            if hist_chart:
                charts.append(hist_chart)

            # Box plot
            box_chart = generate_boxplot(df, col)
            if box_chart:
                charts.append(box_chart)

    return charts


def generate_missingness_chart(df: pd.DataFrame) -> ChartData:
    """Generate a missingness heatmap for data quality visualization."""
    # Calculate missing percentage per column
    missing_pct = (df.isna().sum() / len(df) * 100).round(2)

    # Only show columns with missing values
    missing_cols = missing_pct[missing_pct > 0]

    if len(missing_cols) == 0:
        return None

    # Create data for Plotly heatmap
    chart_data = {
        "type": "bar",
        "x": missing_cols.index.tolist(),
        "y": missing_cols.values.tolist(),
        "marker": {
            "color": missing_cols.values.tolist(),
            "colorscale": "Reds",
        },
    }

    return ChartData(
        chart_id=str(uuid.uuid4()),
        chart_type="missingness",
        title="Missing Values by Column (%)",
        data=chart_data,
        column_name="_quality",
    )


def generate_bar_chart(df: pd.DataFrame, col: str) -> ChartData:
    """Generate a bar chart for categorical columns (top 10 values)."""
    value_counts = df[col].value_counts().head(10)

    if len(value_counts) < 2:
        return None

    chart_data = {
        "type": "bar",
        "x": [str(v) for v in value_counts.index.tolist()],
        "y": value_counts.values.tolist(),
        "marker": {"color": "#3b82f6"},
    }

    return ChartData(
        chart_id=str(uuid.uuid4()),
        chart_type="bar",
        title=f"Top Values: {col}",
        data=chart_data,
        column_name=col,
    )


def generate_line_chart(df: pd.DataFrame, col: str) -> ChartData:
    """Generate a line chart for temporal columns."""
    # Try to convert to datetime for proper ordering
    try:
        date_series = pd.to_datetime(df[col], errors='coerce')
        valid_dates = date_series.dropna()

        if len(valid_dates) < 2:
            return None

        # Group by date and count
        date_counts = valid_dates.value_counts().sort_index()

        # If too many points, resample to reasonable frequency
        if len(date_counts) > 100:
            # Create a DataFrame for resampling
            temp_df = pd.DataFrame({'count': 1}, index=valid_dates)
            # Determine resampling frequency based on date range
            date_range = (valid_dates.max() - valid_dates.min()).days
            if date_range > 365 * 2:
                freq = 'ME'  # Monthly
            elif date_range > 90:
                freq = 'W'  # Weekly
            else:
                freq = 'D'  # Daily
            date_counts = temp_df.resample(freq).count()['count']

        chart_data = {
            "type": "scatter",
            "mode": "lines+markers",
            "x": [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in date_counts.index],
            "y": date_counts.values.tolist(),
            "line": {"color": "#10b981"},
            "marker": {"size": 4},
        }

        return ChartData(
            chart_id=str(uuid.uuid4()),
            chart_type="line",
            title=f"Trend Over Time: {col}",
            data=chart_data,
            column_name=col,
        )

    except Exception:
        return None


def generate_histogram(df: pd.DataFrame, col: str) -> ChartData:
    """Generate a histogram for numerical columns."""
    values = df[col].dropna()

    if len(values) < 2:
        return None

    # Calculate histogram bins
    hist, bin_edges = np.histogram(values, bins='auto')

    # Limit to 50 bins max for readability
    if len(hist) > 50:
        hist, bin_edges = np.histogram(values, bins=50)

    chart_data = {
        "type": "bar",
        "x": [(bin_edges[i] + bin_edges[i+1]) / 2 for i in range(len(hist))],
        "y": hist.tolist(),
        "marker": {"color": "#8b5cf6"},
        "width": [(bin_edges[i+1] - bin_edges[i]) * 0.9 for i in range(len(hist))],
    }

    return ChartData(
        chart_id=str(uuid.uuid4()),
        chart_type="histogram",
        title=f"Distribution: {col}",
        data=chart_data,
        column_name=col,
    )


def generate_boxplot(df: pd.DataFrame, col: str) -> ChartData:
    """Generate a box plot for numerical columns to show outliers."""
    values = df[col].dropna()

    if len(values) < 5:
        return None

    # Calculate box plot statistics
    q1 = values.quantile(0.25)
    q2 = values.quantile(0.50)
    q3 = values.quantile(0.75)
    iqr = q3 - q1
    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    # Find outliers
    outliers = values[(values < lower_fence) | (values > upper_fence)].tolist()

    chart_data = {
        "type": "box",
        "y": values.tolist(),
        "name": col,
        "boxpoints": "outliers",
        "marker": {"color": "#f59e0b"},
        "stats": {
            "min": float(values.min()),
            "q1": float(q1),
            "median": float(q2),
            "q3": float(q3),
            "max": float(values.max()),
            "outlier_count": len(outliers),
        },
    }

    return ChartData(
        chart_id=str(uuid.uuid4()),
        chart_type="boxplot",
        title=f"Outlier Analysis: {col}",
        data=chart_data,
        column_name=col,
    )


def generate_static_chart(chart_data: ChartData, output_path: Path) -> Path:
    """Generate a static matplotlib chart for Excel embedding."""
    plt.figure(figsize=(10, 6))
    plt.style.use('seaborn-v0_8-whitegrid')

    data = chart_data.data

    if chart_data.chart_type == "bar" or chart_data.chart_type == "missingness":
        plt.bar(data['x'], data['y'], color=data.get('marker', {}).get('color', '#3b82f6'))
        plt.xticks(rotation=45, ha='right')

    elif chart_data.chart_type == "line":
        plt.plot(data['x'], data['y'], marker='o', markersize=4, color='#10b981')
        plt.xticks(rotation=45, ha='right')

    elif chart_data.chart_type == "histogram":
        plt.bar(data['x'], data['y'], width=data.get('width', [0.8] * len(data['x'])), color='#8b5cf6')

    elif chart_data.chart_type == "boxplot":
        plt.boxplot(data['y'], vert=True)

    plt.title(chart_data.title, fontsize=14, fontweight='bold')
    plt.tight_layout()

    # Save to file
    chart_path = output_path / f"{chart_data.chart_id}.png"
    plt.savefig(chart_path, dpi=150, bbox_inches='tight')
    plt.close()

    return chart_path
