from __future__ import annotations

import hmac
import hashlib
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks

from dara.config import settings
from dara.agent import run_investigation


def create_app() -> FastAPI:
    app = FastAPI(title="dara webhook", version="0.1.0")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/airflow/failure")
    async def airflow_failure_callback(request: Request, background_tasks: BackgroundTasks):
        """
        Endpoint for Airflow on_failure_callback.

        Configure in your DAG:
            from airflow.operators.http_operator import SimpleHttpOperator
            default_args = {
                "on_failure_callback": notify_dara,
            }

        Or use the dara Airflow plugin (see docs).
        """
        if settings.webhook_secret:
            signature = request.headers.get("X-Dara-Signature", "")
            body = await request.body()
            expected = hmac.new(
                settings.webhook_secret.encode(), body, hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(signature, expected):
                raise HTTPException(status_code=401, detail="Invalid signature")

        payload = await request.json()
        dag_id = payload.get("dag_id")
        task_id = payload.get("task_id")
        run_id = payload.get("run_id")
        context = payload.get("context")

        if not dag_id:
            raise HTTPException(status_code=400, detail="dag_id is required")

        background_tasks.add_task(
            run_investigation,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            context=context,
            output_format="markdown",
        )

        return {"status": "investigation_started", "dag_id": dag_id}

    return app
