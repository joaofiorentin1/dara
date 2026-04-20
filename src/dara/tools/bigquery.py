from __future__ import annotations

from typing import Any


class BigQueryClient:
    def __init__(self, settings):
        self._project = settings.bigquery_project
        self._client = None

    def _get_client(self):
        if self._client is None:
            from google.cloud import bigquery
            self._client = bigquery.Client(project=self._project)
        return self._client

    def health(self) -> bool:
        client = self._get_client()
        list(client.list_datasets(max_results=1))
        return True

    def get_recent_job_failures(self, hours: int = 24) -> list[dict]:
        query = f"""
            SELECT
                job_id,
                creation_time,
                end_time,
                error_result.reason AS error_reason,
                error_result.message AS error_message,
                query
            FROM `{self._project}`.`region-us`.INFORMATION_SCHEMA.JOBS
            WHERE
                creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
                AND error_result IS NOT NULL
            ORDER BY creation_time DESC
            LIMIT 50
        """
        return self._run_query(query)

    def get_table_row_count_history(self, dataset: str, table: str, days: int = 7) -> list[dict]:
        query = f"""
            SELECT
                DATE(creation_time) AS date,
                COUNT(*) AS job_count,
                SUM(total_bytes_processed) AS bytes_processed
            FROM `{self._project}`.`region-us`.INFORMATION_SCHEMA.JOBS
            WHERE
                creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
                AND referenced_tables IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM UNNEST(referenced_tables) t
                    WHERE t.dataset_id = '{dataset}' AND t.table_id = '{table}'
                )
            GROUP BY date
            ORDER BY date DESC
        """
        return self._run_query(query)

    def get_recent_jobs_for_dag(self, dag_id: str, hours: int = 24) -> list[dict]:
        query = f"""
            SELECT
                job_id,
                creation_time,
                end_time,
                state,
                error_result.message AS error_message,
                total_bytes_processed,
                query
            FROM `{self._project}`.`region-us`.INFORMATION_SCHEMA.JOBS
            WHERE
                creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
                AND labels.airflow_dag_id = '{dag_id}'
            ORDER BY creation_time DESC
            LIMIT 20
        """
        return self._run_query(query)

    def check_table_freshness(self, dataset: str, table: str) -> dict:
        query = f"""
            SELECT
                MAX(_PARTITIONTIME) AS last_partition,
                COUNT(*) AS row_count
            FROM `{self._project}.{dataset}.{table}`
            WHERE DATE(_PARTITIONTIME) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        """
        rows = self._run_query(query)
        return rows[0] if rows else {}

    def _run_query(self, query: str) -> list[dict]:
        client = self._get_client()
        result = client.query(query).result()
        return [dict(row) for row in result]
