from __future__ import annotations

import json
from typing import Any

import anthropic
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

from dara.config import settings
from dara.tools.airflow import AirflowClient
from dara.tools.bigquery import BigQueryClient
from dara.tools.dbt import DbtManifest

console = Console()

SYSTEM_PROMPT = """You are dara — a Data Anomaly Root-cause Agent.

Your job is to investigate failures in data pipelines that use Apache Airflow, dbt, and BigQuery.
When given a DAG failure, you:
1. Collect context from Airflow (run state, failed tasks, logs)
2. Map the failing task to a dbt model or test via the manifest
3. Trace the lineage upstream to find where data broke
4. Check BigQuery job history and table freshness for anomalies
5. Form ranked hypotheses about the root cause
6. Produce a clear, actionable report in Portuguese (pt-BR)

Always be concise and direct. Format your final report as Markdown with sections:
- DIAGNÓSTICO
- IMPACTO
- CAUSA RAIZ (with confidence %)
- AÇÃO SUGERIDA
"""

TOOLS = [
    {
        "name": "get_dag_run_summary",
        "description": "Get the summary of a DAG run including state, failed tasks, and execution time.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "run_id": {"type": "string", "description": "Optional. Uses latest run if omitted."},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "get_task_log",
        "description": "Fetch the execution log of a specific Airflow task instance.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id": {"type": "string"},
                "try_number": {"type": "integer", "default": 1},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
    {
        "name": "get_dbt_model_for_task",
        "description": "Find the dbt model or test that corresponds to an Airflow task ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
            },
            "required": ["task_id"],
        },
    },
    {
        "name": "get_upstream_lineage",
        "description": "Get the upstream dbt models that feed into a given model (by unique_id).",
        "input_schema": {
            "type": "object",
            "properties": {
                "node_unique_id": {"type": "string"},
                "depth": {"type": "integer", "default": 3},
            },
            "required": ["node_unique_id"],
        },
    },
    {
        "name": "get_downstream_lineage",
        "description": "Get downstream dbt models and tables affected by a given model (by unique_id).",
        "input_schema": {
            "type": "object",
            "properties": {
                "node_unique_id": {"type": "string"},
                "depth": {"type": "integer", "default": 3},
            },
            "required": ["node_unique_id"],
        },
    },
    {
        "name": "get_bq_recent_failures",
        "description": "Get recent BigQuery job failures in the last N hours.",
        "input_schema": {
            "type": "object",
            "properties": {
                "hours": {"type": "integer", "default": 24},
            },
        },
    },
    {
        "name": "get_bq_jobs_for_dag",
        "description": "Get BigQuery jobs triggered by a specific Airflow DAG.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "hours": {"type": "integer", "default": 24},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "check_table_freshness",
        "description": "Check how fresh a BigQuery table is (last partition, row count).",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string"},
                "table": {"type": "string"},
            },
            "required": ["dataset", "table"],
        },
    },
]


def _dispatch_tool(name: str, inputs: dict, airflow: AirflowClient, bq: BigQueryClient, dbt: DbtManifest) -> Any:
    if name == "get_dag_run_summary":
        return airflow.get_dag_run_summary(inputs["dag_id"], inputs.get("run_id"))
    elif name == "get_task_log":
        return airflow.get_task_log(
            inputs["dag_id"], inputs["dag_run_id"], inputs["task_id"], inputs.get("try_number", 1)
        )
    elif name == "get_dbt_model_for_task":
        node = dbt.get_model_for_task(inputs["task_id"])
        return node or {"error": f"No dbt model found for task '{inputs['task_id']}'"}
    elif name == "get_upstream_lineage":
        return dbt.get_upstream_models(inputs["node_unique_id"], inputs.get("depth", 3))
    elif name == "get_downstream_lineage":
        return dbt.get_downstream_models(inputs["node_unique_id"], inputs.get("depth", 3))
    elif name == "get_bq_recent_failures":
        return bq.get_recent_job_failures(inputs.get("hours", 24))
    elif name == "get_bq_jobs_for_dag":
        return bq.get_recent_jobs_for_dag(inputs["dag_id"], inputs.get("hours", 24))
    elif name == "check_table_freshness":
        return bq.check_table_freshness(inputs["dataset"], inputs["table"])
    else:
        return {"error": f"Unknown tool: {name}"}


def run_investigation(
    dag_id: str,
    task_id: str | None = None,
    run_id: str | None = None,
    context: str | None = None,
    output_format: str = "terminal",
) -> str:
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    airflow = AirflowClient(settings)
    bq = BigQueryClient(settings)
    dbt = DbtManifest(settings)

    user_message = f"Investigate the failure of DAG `{dag_id}`"
    if task_id:
        user_message += f", specifically task `{task_id}`"
    if run_id:
        user_message += f" (run_id: {run_id})"
    if context:
        user_message += f". Additional context: {context}"

    messages = [{"role": "user", "content": user_message}]

    console.print(f"\n[dim]Starting investigation for [bold]{dag_id}[/bold]...[/dim]\n")

    max_iterations = 10
    for iteration in range(max_iterations):
        response = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # Collect tool calls and text
        tool_uses = [b for b in response.content if b.type == "tool_use"]
        text_blocks = [b for b in response.content if b.type == "text"]

        for text_block in text_blocks:
            if text_block.text.strip():
                console.print(f"[dim]{text_block.text}[/dim]")

        if response.stop_reason == "end_turn" or not tool_uses:
            # Final response
            final_text = "\n".join(b.text for b in text_blocks if b.type == "text")
            _render_output(final_text, output_format)
            return final_text

        # Execute tools
        messages.append({"role": "assistant", "content": response.content})
        tool_results = []

        for tool_use in tool_uses:
            console.print(f"  [cyan]→[/cyan] [bold]{tool_use.name}[/bold]({json.dumps(tool_use.input, ensure_ascii=False)})")
            try:
                result = _dispatch_tool(tool_use.name, tool_use.input, airflow, bq, dbt)
                result_str = json.dumps(result, default=str, ensure_ascii=False)
            except Exception as e:
                result_str = json.dumps({"error": str(e)})

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tool_use.id,
                "content": result_str,
            })

        messages.append({"role": "user", "content": tool_results})

    return "Investigation reached maximum iterations without conclusion."


def _render_output(text: str, output_format: str):
    console.print()
    if output_format == "terminal":
        console.print(Panel(Markdown(text), title="[bold red]dara — RCA Report[/bold]", border_style="red"))
    elif output_format == "markdown":
        print(text)
    elif output_format == "json":
        print(json.dumps({"report": text}))
