"""Generic AWS Athena query client."""

from __future__ import annotations

import time

import boto3
import structlog

logger = structlog.get_logger(__name__)


class AthenaQueryError(Exception):
    """Raised when an Athena query fails."""


class AthenaClient:
    """Generic, reusable AWS Athena query client.

    Not intended for direct use by pipelines — use PartidaStore.query_athena() instead.
    """

    def __init__(
        self,
        region: str,
        output_location: str,
        database: str = "inmoba_sunarp",
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        profile_name: str | None = None,
    ) -> None:
        """Initialize Athena client.

        Args:
            region: AWS region (e.g. 'us-east-2')
            output_location: S3 URI for Athena query results (e.g. 's3://bucket/prefix/')
            database: Default Athena database name
            access_key_id: Optional AWS access key (uses env/instance profile if None)
            secret_access_key: Optional AWS secret key
            session_token: Optional AWS session token (required for STS temporary credentials)
            profile_name: Optional AWS profile name (e.g. 'developer-leonardo-candio')
        """
        kwargs: dict = {"region_name": region}
        if access_key_id and secret_access_key:
            kwargs["aws_access_key_id"] = access_key_id
            kwargs["aws_secret_access_key"] = secret_access_key
            if session_token:
                kwargs["aws_session_token"] = session_token
        if profile_name:
            session = boto3.Session(profile_name=profile_name)
            self._client = session.client("athena", **kwargs)
        else:
            self._client = boto3.client("athena", **kwargs)
        self._output_location = output_location
        self._database = database
        self._region = region
        self._log = logger.bind(
            component="AthenaClient", region=region, database=database
        )

    def execute_query(self, sql: str) -> str:
        """Submit a query to Athena and return the QueryExecutionId."""
        self._log.debug("submitting_athena_query", sql_preview=sql[:200])
        response = self._client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self._database},
            ResultConfiguration={"OutputLocation": self._output_location},
        )
        query_id = response["QueryExecutionId"]
        self._log.info("athena_query_submitted", query_id=query_id)
        return query_id

    def wait_for_query(
        self,
        query_id: str,
        timeout: float = 300.0,
        poll_interval: float = 2.0,
    ) -> str:
        """Poll until query completes. Returns final status string."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self._client.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]
            state = status["State"]
            if state == "SUCCEEDED":
                self._log.info("athena_query_succeeded", query_id=query_id)
                return state
            elif state in ("FAILED", "CANCELLED"):
                reason = status.get("StateChangeReason", "unknown")
                self._log.error(
                    "athena_query_failed",
                    query_id=query_id,
                    state=state,
                    reason=reason,
                )
                raise AthenaQueryError(f"Query {query_id} {state}: {reason}")
            self._log.debug("athena_query_running", query_id=query_id, state=state)
            time.sleep(poll_interval)
        raise AthenaQueryError(f"Query {query_id} timed out after {timeout}s")

    def get_results(self, query_id: str) -> list[dict]:
        """Fetch all result rows as list of dicts. Skips the header row."""
        results: list[dict] = []
        paginator = self._client.get_paginator("get_query_results")
        pages = paginator.paginate(QueryExecutionId=query_id)
        columns: list[str] | None = None
        for page in pages:
            rows = page["ResultSet"]["Rows"]
            if columns is None:
                # First row is the header
                columns = [col["VarCharValue"] for col in rows[0]["Data"]]
                rows = rows[1:]
            for row in rows:
                values = [cell.get("VarCharValue", None) for cell in row["Data"]]
                results.append(dict(zip(columns, values)))
        self._log.info(
            "athena_results_fetched", query_id=query_id, row_count=len(results)
        )
        return results

    def query(self, sql: str, timeout: float = 300.0) -> list[dict]:
        """Execute query, wait for completion, return results as list of dicts."""
        query_id = self.execute_query(sql)
        self.wait_for_query(query_id, timeout=timeout)
        return self.get_results(query_id)
