# Databricks notebook source
# DBTITLE 1,Import Dependencies
import time
import random
import logging
from typing import Optional, Dict, Any
import requests

# COMMAND ----------

class WarcraftLogsClient:
    """
    Thin wrapper around the Warcraft Logs v2 API that handles:
      • OAuth token retrieval
      • automatic token refresh
      • retry logic + exponential backoff
      • GraphQL request helpers

    This class is intentionally “dumb”: it only executes GraphQL
    and does not know anything about Databricks, DLT, or storage.
    """

    TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
    API_URL = "https://www.warcraftlogs.com/api/v2/client"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        max_retries: int = 5,
        timeout: int = 30,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.max_retries = max_retries
        self.timeout = timeout

        self._token: Optional[str] = None
        self._token_expiry: Optional[int] = None  # epoch seconds

        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

        self.refresh_token()

# COMMAND ----------

# DBTITLE 1,Token handling
def refresh_token(self):
        """Retrieve a new OAuth token from Warcraft Logs."""
        logging.info("Refreshing WarcraftLogs API token...")

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        resp = self._session.post(self.TOKEN_URL, data=payload, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()
        self._token = data["access_token"]
        expires_in = data.get("expires_in", 3600)  # default 1 hour
        self._token_expiry = int(time.time()) + expires_in - 60  # refresh 1 min early

        self._session.headers.update({"Authorization": f"Bearer {self._token}"})

def _ensure_token(self):
    """Refresh the token if near expiry."""
    if not self._token or (self._token_expiry and time.time() >= self._token_expiry):
        self.refresh_token()

# COMMAND ----------

# DBTITLE 1,GraphQL
def graphql(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query with retry & backoff.

        Returns the `"data"` field of the GraphQL response.
        Throws if:
          • HTTP failure after retries
          • GraphQL `errors` after retries
        """
        self._ensure_token()

        last_error = None

        for attempt in range(1, self.max_retries + 1):

            try:
                body = {"query": query}
                if variables:
                    body["variables"] = variables

                resp = self._session.post(
                    self.API_URL,
                    json=body,
                    timeout=self.timeout,
                )
                resp.raise_for_status()

                payload = resp.json()

                # If no GraphQL errors → return data
                if "errors" not in payload:
                    return payload.get("data", {})

                # If GraphQL-level errors exist
                logging.warning(
                    f"[GraphQL Error] Attempt {attempt}/{self.max_retries}: "
                    f"{payload['errors']}"
                )
                last_error = RuntimeError(str(payload["errors"]))

            except Exception as e:
                logging.warning(
                    f"[HTTP Exception] Attempt {attempt}/{self.max_retries}: {e}"
                )
                last_error = e

            # Stop retrying if final attempt
            if attempt == self.max_retries:
                raise last_error

            # Exponential backoff + jitter
            backoff = (2 ** attempt) + random.random()
            time.sleep(backoff)

        raise last_error  # shouldn't reach here

# COMMAND ----------

# DBTITLE 1,Convenience wrappers
def get_latest_report_metadata(
        self, guild_name: str, server_slug: str, region: str
    ) -> Dict[str, Any]:
        """
        Fetch the newest report for a guild.
        Returns: { "code": str, "startTime": int }
        """
        q = """
        query($name:String!, $slug:String!, $region:String!) {
          reportData {
            reports(
              guildName: $name,
              guildServerSlug: $slug,
              guildServerRegion: $region,
              limit: 1
            ) {
              data { code startTime }
            }
          }
        }"""

        data = self.graphql(
            q,
            {
                "name": guild_name,
                "slug": server_slug,
                "region": region,
            },
        )
        reports = data["reportData"]["reports"]["data"]

        if not reports:
            raise ValueError("Guild has no available reports.")

        return reports[0]

def get_report_metadata(self, report_code: str) -> Dict[str, Any]:
    """Return code + startTime for a specific report ID."""
    q = """
    query($code:String!) {
      reportData {
        report(code:$code) { code startTime }
      }
    }"""

    data = self.graphql(q, {"code": report_code})
    return data["reportData"]["report"]
