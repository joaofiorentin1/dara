# dara — Data Anomaly Root-cause Agent

> Stop reading walls of Kubernetes logs. Let the agent investigate for you.

**dara** is an autonomous AI agent that investigates data pipeline failures across **Apache Airflow**, **dbt**, and **BigQuery** — and delivers a root-cause analysis report in seconds.

---

## The problem

You get 8 emails like this at 3am:

```
❌ DAG dag_motorista_tech_tests_dbt falhou na task test_company_driver_block
Try: 3/2
Log: Pod dbt-motorista-tech-... returned a failure.
remote_pod: {'api_version': 'v1', 'kind': 'Pod', 'metadata': {'annotations': ...
[200 lines of Kubernetes metadata]
```

To find the actual root cause, you open Airflow, read task logs, check dbt test output, query BigQuery job history, trace lineage — all manually.

**dara does all of that for you.**

---

## What you get instead

```
🔴 INVESTIGAÇÃO CONCLUÍDA — dag_motorista_tech_tests_dbt

DIAGNÓSTICO
  Task test_company_driver_block falhou (try 3/3) em 2026-04-20 00:47
  Tipo: dbt test — not_null_company_driver_block_id

CAUSA RAIZ (85% de confiança)
  847 linhas com driver_id nulo na tabela raw.events
  → DAG dag_ingest_events também falhou às 23h14 (sem alerta configurado)
  → A ingestão incompleta alimentou o modelo stg_company_drivers com nulos

IMPACTO
  → tabela motorista_tech.company_driver_block_gold desatualizada
  → 2 modelos downstream afetados: fct_driver_blocks, dim_active_drivers

AÇÃO SUGERIDA
  1. Corrigir falha na ingestão (dag_ingest_events, task upload_to_gcs)
  2. Rerodar dag_motorista_tech_tests_dbt após ingestão completa
```

---

## How it works

```
Airflow on_failure_callback
         │
         ▼
    dara investigate
         │
    ┌────┴─────────────────────────┐
    │    Claude (tool use loop)     │
    │  ┌──────────────────────────┐ │
    │  │ get_dag_run_summary      │─┼──► Airflow REST API
    │  │ get_task_log             │─┼──► Airflow REST API
    │  │ get_dbt_model_for_task   │─┼──► manifest.json
    │  │ get_upstream_lineage     │─┼──► manifest.json
    │  │ get_bq_recent_failures   │─┼──► BigQuery INFORMATION_SCHEMA
    │  │ check_table_freshness    │─┼──► BigQuery
    │  └──────────────────────────┘ │
    └────────────────┬──────────────┘
                     │
                     ▼
              RCA Report (terminal / markdown / webhook)
```

dara runs a **ReAct loop** using Claude's native tool use: it collects evidence from all three systems, forms hypotheses, and iterates until it reaches a conclusion.

---

## Installation

```bash
pip install dara-agent
```

Or with `uv`:

```bash
uv tool install dara-agent
```

---

## Quick start

### 1. Configure

```bash
cp .env.example .env
# Fill in ANTHROPIC_API_KEY, AIRFLOW_BASE_URL, BIGQUERY_PROJECT, DBT_MANIFEST_PATH
```

### 2. Check connectivity

```bash
dara check
```

### 3. Investigate a failure

```bash
dara investigate dag_motorista_tech_tests_dbt
dara investigate dag_motorista_tech_tests_dbt --task test_company_driver_block
dara investigate dag_xyz --context "números 30% abaixo do esperado"
```

### 4. Auto-trigger via webhook

Start the webhook server:

```bash
dara serve --port 9000
```

Configure `on_failure_callback` in your Airflow DAG:

```python
import requests

def notify_dara(context):
    requests.post("http://your-dara-server:9000/airflow/failure", json={
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        "run_id": context["run_id"],
    })

default_args = {
    "on_failure_callback": notify_dara,
}
```

---

## Stack

| Layer | Technology |
|---|---|
| Agent brain | Claude Sonnet (Anthropic API) |
| Airflow | REST API v1 (Airflow 2.x / 3.x) |
| dbt | manifest.json + run_results.json |
| BigQuery | INFORMATION_SCHEMA + google-cloud-bigquery |
| CLI | Typer + Rich |
| Webhook | FastAPI + uvicorn |

---

## Roadmap

- [ ] Slack/email notification with report
- [ ] Multi-DAG correlation (find related failures)
- [ ] dbt Cloud API support
- [ ] Kubernetes pod log fetcher (GKE)
- [ ] GitHub issue auto-creation on unknown failures
- [ ] Web UI dashboard

---

## Contributing

PRs welcome. Open an issue first for large changes.

```bash
git clone https://github.com/fiorentinjoao/dara
cd dara
uv sync --extra dev
```

---

## License

MIT
