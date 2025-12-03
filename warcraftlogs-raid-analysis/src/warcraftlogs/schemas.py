# Databricks notebook source
# DBTITLE 1,Import dependencies
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

# COMMAND ----------

"""
Schemas for WarcraftLogs GraphQL responses.
These are intentionally lightweight Pydantic models that:
- Validate expected fields returned by the API
- Provide clear typing for downstream transformations (bronze → silver)
"""

# COMMAND ----------

# DBTITLE 1,Report summary (from `report(code: ...)`)
class FightMetadata(BaseModel):
    id: int
    name: str
    startTime: int
    endTime: int
    bossPercentage: Optional[float] = None
    lastPhase: Optional[int] = None
    kill: Optional[bool] = None


class ReportSummary(BaseModel):
    code: str
    title: Optional[str]
    zone: Optional[int]
    fights: List[FightMetadata] = Field(default_factory=list)

# COMMAND ----------

# DBTITLE 1,Table query (from GraphQL `table(dataType: ...)`)
class TableData(BaseModel):
    entries: Optional[List[Dict[str, Any]]] = None


class TableResponse(BaseModel):
    """
    Represents the outer shape of all table(dataType: ...) calls:
    {
      "reportData": {
        "report": {
           "table": {
               "data": { ... }
           }
        }
      }
    }
    """

    data: Optional[Dict[str, Any]] = None

    @classmethod
    def from_graphql(cls, payload: Dict[str, Any]):
        """
        Normalizes the GraphQL nesting into TableResponse(data={...})
        """
        try:
            table_data = (
                payload.get("reportData", {})
                       .get("report", {})
                       .get("table", {})
            )
        except Exception:
            table_data = {}

        return cls(data=table_data)
