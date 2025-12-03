"""
warcraftlogs package

Provides:
- WarcraftLogsClient   → API client for GraphQL requests
- schemas              → Pydantic models for validation
- utils                → Helper utilities (flattening, timestamp parsing, etc.)
- ingestion            → High-level ingestion orchestration helpers
"""

from .client import WarcraftLogsClient
from . import schemas
from . import utils
from . import ingestion

__all__ = [
    "WarcraftLogsClient",
    "schemas",
    "utils",
    "ingestion",
]
