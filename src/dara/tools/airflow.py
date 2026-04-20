from __future__ import annotations

import httpx
from typing import Any


class AirflowClient:
    def __init__(self, settings):
        self._base = settings.airflow_base_url.rstrip("/")
        self._auth = (settings.airflow_username, settings.airflow_password)

    def _get(self, path: str, **params) -> Any:
        url = f"{self._base}/api/v1{path}"
        r = httpx.get(url, auth=self._auth, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def health(self) -> bool:
        self._get("/health")
        return True

    def get_dag(self, dag_id: str) -> dict:
        return self._get(f"/dags/{dag_id}")

    def get_dag_runs(self, dag_id: str, limit: int = 5) -> list[dict]:
        data = self._get(f"/dags/{dag_id}/dagRuns", limit=limit, order_by="-execution_date")
        return data.get("dag_runs", [])

    def get_latest_run(self, dag_id: str) -> dict | None:
        runs = self.get_dag_runs(dag_id, limit=1)
        return runs[0] if runs else None

    def get_task_instances(self, dag_id: str, dag_run_id: str) -> list[dict]:
        data = self._get(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
        return data.get("task_instances", [])

    def get_task_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> str:
        url = f"{self._base}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        r = httpx.get(url, auth=self._auth, timeout=30)
        r.raise_for_status()
        return r.text

    def get_failed_tasks(self, dag_id: str, dag_run_id: str) -> list[dict]:
        tasks = self.get_task_instances(dag_id, dag_run_id)
        return [t for t in tasks if t.get("state") in ("failed", "upstream_failed")]

    def get_dag_run_summary(self, dag_id: str, run_id: str | None = None) -> dict:
        if run_id is None:
            run = self.get_latest_run(dag_id)
            if not run:
                return {"error": f"No runs found for DAG {dag_id}"}
            run_id = run["dag_run_id"]

        run = self._get(f"/dags/{dag_id}/dagRuns/{run_id}")
        failed_tasks = self.get_failed_tasks(dag_id, run_id)

        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": run.get("state"),
            "execution_date": run.get("execution_date"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
            "failed_tasks": failed_tasks,
        }
