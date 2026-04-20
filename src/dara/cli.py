import typer
from rich.console import Console
from rich.panel import Panel

app = typer.Typer(
    name="dara",
    help="Data Anomaly Root-cause Agent — autonomous RCA for Airflow + dbt + BigQuery",
    no_args_is_help=True,
)
console = Console()


@app.command()
def investigate(
    dag_id: str = typer.Argument(..., help="DAG ID to investigate"),
    task_id: str = typer.Option(None, "--task", "-t", help="Specific task ID"),
    run_id: str = typer.Option(None, "--run", "-r", help="Specific run ID (latest if omitted)"),
    context: str = typer.Option(None, "--context", "-c", help="Extra context (e.g. 'numbers 30% below')"),
    output: str = typer.Option("terminal", "--output", "-o", help="Output format: terminal | markdown | json"),
):
    """Investigate a DAG failure and produce a root-cause analysis report."""
    from dara.agent import run_investigation

    console.print(Panel(f"[bold]Investigating[/bold] {dag_id}", style="yellow"))
    run_investigation(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        context=context,
        output_format=output,
    )


@app.command()
def serve(
    port: int = typer.Option(9000, "--port", "-p", help="Webhook server port"),
    host: str = typer.Option("0.0.0.0", "--host", help="Host to bind"),
):
    """Start the webhook server to auto-trigger investigations on Airflow failure callbacks."""
    import uvicorn
    from dara.webhook import create_app

    console.print(Panel(f"[bold]dara webhook[/bold] listening on {host}:{port}", style="green"))
    uvicorn.run(create_app(), host=host, port=port)


@app.command()
def check():
    """Verify connectivity to Airflow, BigQuery, and dbt manifest."""
    from dara.tools.airflow import AirflowClient
    from dara.tools.bigquery import BigQueryClient
    from dara.tools.dbt import DbtManifest
    from dara.config import settings

    console.print("[bold]Checking connections...[/bold]\n")

    checks = [
        ("Airflow", lambda: AirflowClient(settings).health()),
        ("BigQuery", lambda: BigQueryClient(settings).health()),
        ("dbt manifest", lambda: DbtManifest(settings).health()),
    ]

    for name, fn in checks:
        try:
            fn()
            console.print(f"  [green]✓[/green] {name}")
        except Exception as e:
            console.print(f"  [red]✗[/red] {name}: {e}")


if __name__ == "__main__":
    app()
