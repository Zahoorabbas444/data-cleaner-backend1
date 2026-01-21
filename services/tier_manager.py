from enum import Enum
from typing import Optional
import pandas as pd


class UserTier(str, Enum):
    FREE = "free"
    PAID = "paid"


# Tier limits
TIER_LIMITS = {
    UserTier.FREE: {
        "max_rows": 100,
        "watermark": True,
    },
    UserTier.PAID: {
        "max_rows": None,  # Unlimited
        "watermark": False,
    },
}


def get_tier_from_token(token: Optional[str]) -> UserTier:
    """
    Determine user tier from authentication token.
    For v1, this is a simple implementation.
    In production, this would verify with Stripe.
    """
    if token and token.startswith("paid_"):
        return UserTier.PAID
    return UserTier.FREE


def apply_tier_limits(df: pd.DataFrame, tier: UserTier) -> tuple[pd.DataFrame, dict]:
    """
    Apply tier-based limits to the dataframe.

    Returns:
        Tuple of (limited dataframe, limit info dict)
    """
    limits = TIER_LIMITS[tier]
    limit_info = {
        "tier": tier.value,
        "original_rows": len(df),
        "truncated": False,
        "watermark": limits["watermark"],
    }

    max_rows = limits.get("max_rows")

    if max_rows and len(df) > max_rows:
        df = df.head(max_rows)
        limit_info["truncated"] = True
        limit_info["processed_rows"] = max_rows
        limit_info["message"] = f"Free tier limited to {max_rows} rows. Upgrade for unlimited processing."
    else:
        limit_info["processed_rows"] = len(df)

    return df, limit_info


def add_watermark_to_charts(charts: list, tier: UserTier) -> list:
    """Add watermark information to charts for free tier."""
    if TIER_LIMITS[tier]["watermark"]:
        for chart in charts:
            chart.data["watermark"] = "Free Tier - Upgrade for watermark-free exports"
    return charts


def should_add_watermark(tier: UserTier) -> bool:
    """Check if watermark should be added."""
    return TIER_LIMITS[tier]["watermark"]
