# Databricks notebook source
"""
Utility helpers for WarcraftLogs ingestion.
These functions are intentionally lightweight and pure Python so they
work cleanly in Databricks notebooks, DLT pipelines, and unit tests.
"""

# COMMAND ----------

# DBTITLE 1,Import dependencies
from __future__ import annotations
import os
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# COMMAND ----------

# DBTITLE 1,Query loader
def load_query(name: str) -> str:
    """
    Load a GraphQL query from src/warcraftlogs/queries/<name>.graphql

    Example:
      load_query("report_summary")
    """
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "queries", f"{name}.graphql")

    if not os.path.exists(path):
        raise FileNotFoundError(f"GraphQL query not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return f.read()

# COMMAND ----------

# DBTITLE 1,Nested lookup
def safe_get(obj: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Safely access nested dictionaries using dotted paths.

    Example:
      safe_get(payload, "reportData.report.title")
    """
    parts = path.split(".")
    cur = obj
    for p in parts:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

# COMMAND ----------

# DBTITLE 1,Flattening helpers
def flatten_dict(
    data: Dict[str, Any],
    parent_key: str = "",
    sep: str = "_"
) -> Dict[str, Any]:
    """
    Flatten nested dicts into a single level.
    Useful for bronze stage before applying strict Silver schemas.
    """
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# COMMAND ----------

# DBTITLE 1,Timestamp normalisation
def normalise_timestamp_ms(ts: Optional[int]) -> Optional[str]:
    """
    WarcraftLogs timestamps are in milliseconds since epoch.
    Convert to ISO8601 string (UTC). Returns None if ts is None.
    """
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None

# COMMAND ----------

# DBTITLE 1,Slug generator (for filenames/table names)
def slugify(text: str) -> str:
    """
    Lowercases and converts any string into a safe slug.
    """
    text = text.lower().strip()
    text = _slug_re.sub("-", text)
    return text.strip("-")
