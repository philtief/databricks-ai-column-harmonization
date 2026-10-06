#!/usr/bin/env python3
# ruff: noqa: E501  (SQL statements read better unwrapped)
"""Write execution evidence as plain text (markdown) under evidence/.

Usage: collect_evidence.py --runs <run_id> [<run_id> ...]
Reads job runs (task states, notebook exit summaries, cell outputs), SQL results from the warehouse,
the Lakebase review state, and the app status. Everything is text so a reviewer can read it without images.
"""

import argparse
import base64
import datetime as dt
import json
import re
import urllib.parse
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import ViewsToExport

from harmonization import review_store

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "evidence"
CAT, SCH = "agent_marketplace_catalog", "halvard_harmonization"
DB = f"{CAT}.{SCH}"
ENDPOINT = "projects/halvard-harmonization/branches/production/endpoints/primary"
APP = "halvard-harmonization-review"

SQL = {
    "pipeline_ingest.md": [
        (
            "Rows per bronze table",
            f"SELECT 'es' AS country, count(*) AS rows, count(DISTINCT _source_file) AS files FROM {DB}.bronze_property_monthly_es UNION ALL SELECT 'it', count(*), count(DISTINCT _source_file) FROM {DB}.bronze_property_monthly_it",
        ),
        (
            "Expectation results per update (event log)",
            f"""SELECT origin.update_id, timestamp, details:flow_progress.data_quality.expectations AS expectations,
              details:flow_progress.metrics.num_output_rows AS output_rows, origin.flow_name
            FROM event_log(TABLE({DB}.bronze_property_monthly_es)) WHERE event_type = 'flow_progress'
              AND details:flow_progress.data_quality IS NOT NULL ORDER BY timestamp DESC LIMIT 10""",
        ),
    ],
    "ai_mapping_proposals.md": [
        (
            "Model proposals per country (before review)",
            f"""SELECT source_system, local_column_name, proposed_global_column_name, proposed_match_type, confidence,
              review_status, final_global_column_name, left(mapping_rationale, 120) AS rationale
            FROM {DB}.column_mapping_candidates ORDER BY source_system, local_column_name""",
        ),
        ("Model usage", f"SELECT * FROM {DB}.ai_mapping_usage_metrics ORDER BY source_system"),
    ],
    "mapping_evaluation.md": [
        (
            "Evaluation against the answer keys (latest run per country)",
            f"""SELECT source_system, slice, metric_name, round(metric_value, 3) AS value, run_id AS mlflow_run_id, evaluated_at
            FROM {DB}.mapping_eval_results QUALIFY dense_rank() OVER (PARTITION BY source_system ORDER BY evaluated_at DESC) = 1
            ORDER BY source_system, metric_name, slice""",
        ),
    ],
    "harmonized_kpis.md": [
        (
            "Group KPIs by country (metric view)",
            f"""SELECT `Country`, MEASURE(`Gross Written Premium`) AS gwp_eur, MEASURE(`Gross Claims Incurred`) AS claims_eur,
              round(MEASURE(`Loss Ratio`), 3) AS loss_ratio, round(MEASURE(`Expense Ratio`), 3) AS expense_ratio,
              round(MEASURE(`Combined Ratio`), 3) AS combined_ratio FROM {DB}.mv_group_property_kpis GROUP BY ALL ORDER BY 1""",
        ),
        (
            "Combined ratio by country and channel",
            f"""SELECT `Country`, `Distribution Channel`, round(MEASURE(`Combined Ratio`), 3) AS combined_ratio
            FROM {DB}.mv_group_property_kpis GROUP BY ALL ORDER BY 1, 2""",
        ),
        (
            "Harmonized rows per country",
            f"SELECT source_country, count(*) AS rows FROM {DB}.harmonized_property_monthly GROUP BY 1 ORDER BY 1",
        ),
    ],
    "dq_results.md": [
        (
            "Data quality checks (latest run per country)",
            f"""SELECT source_system, check_name, check_status, metric_value, left(details, 120) AS details, recorded_at
            FROM {DB}.data_quality_results QUALIFY dense_rank() OVER (PARTITION BY source_system ORDER BY recorded_at DESC) = 1
            ORDER BY source_system, check_status, check_name""",
        ),
        (
            "Value translations",
            f"SELECT source_system, source_field, raw_value, harmonized_value, approval_status, approved_by FROM {DB}.value_mapping_dictionary ORDER BY 1, 2, 3",
        ),
    ],
    "uc_governance.md": [
        (
            "Table tags",
            f"SELECT table_name, tag_name, tag_value FROM {CAT}.information_schema.table_tags WHERE schema_name = '{SCH}' ORDER BY 1, 2",
        ),
        (
            "Column tags",
            f"SELECT table_name, column_name, tag_name, tag_value FROM {CAT}.information_schema.column_tags WHERE schema_name = '{SCH}' ORDER BY 1, 2",
        ),
        ("Grants on the schema", f"SHOW GRANTS ON SCHEMA {DB}"),
        (
            "Row filter on the harmonized table",
            f"SELECT table_name, filter_name, target_columns FROM {CAT}.information_schema.row_filters WHERE table_schema = '{SCH}'",
        ),
        ("Row filter function", f"DESCRIBE FUNCTION EXTENDED {DB}.country_row_filter"),
        (
            "Lineage (system.access.table_lineage)",
            f"""SELECT DISTINCT source_table_full_name, target_table_full_name, entity_type
            FROM system.access.table_lineage WHERE target_table_full_name LIKE '{DB}.%' AND source_table_full_name IS NOT NULL
              AND event_date >= current_date() - 2 ORDER BY 2, 1""",
        ),
    ],
}


def md_table(columns: list[str], rows: list[list]) -> str:
    def cell(value) -> str:
        return str("" if value is None else value).replace("|", "\\|").replace("\n", " ")

    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def run_sql(w: WorkspaceClient, warehouse_id: str, statement: str) -> str:
    r = w.statement_execution.execute_statement(statement=statement, warehouse_id=warehouse_id, wait_timeout="50s")
    if r.status.error:
        return f"Query failed: `{r.status.error.message}`"
    columns = [c.name for c in r.manifest.schema.columns]
    return md_table(columns, (r.result.data_array or []) if r.result else [])


def notebook_text(html: str) -> str:
    """Cell source and text outputs from an exported run (the notebook model is embedded as base64)."""
    match = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", html)
    if not match:
        return "(no notebook model in export)"
    model = json.loads(urllib.parse.unquote(base64.b64decode(match.group(1)).decode()))
    parts = []
    for command in model.get("commands", []):
        source = command.get("command", "").strip()
        if not source or source.startswith("%md"):
            continue
        parts.append(f"```python\n{source}\n```")
        results = command.get("results") or {}
        data = results.get("data")
        texts = (
            [d.get("data") for d in data if isinstance(d, dict) and isinstance(d.get("data"), str)]
            if isinstance(data, list)
            else [data]
        )
        output = "\n".join(t for t in texts if isinstance(t, str) and t.strip())
        if command.get("error") or results.get("cause"):
            output += f"\nERROR: {command.get('error') or results.get('cause')}"
        if output.strip():
            parts.append(f"Output:\n```text\n{output.strip()[:6000]}\n```")
    return "\n\n".join(parts)


def job_run(w: WorkspaceClient, run_id: int) -> None:
    run = w.jobs.get_run(run_id)
    job = w.jobs.get(run.job_id).settings.name
    params = {p.name: p.value or p.default for p in run.job_parameters or []}
    lines = [
        f"# {job} run {run_id}",
        "",
        f"- State: {run.state.result_state.value if run.state.result_state else run.state.life_cycle_state.value}",
        f"- Started: {dt.datetime.fromtimestamp(run.start_time / 1000, dt.UTC):%Y-%m-%d %H:%M:%S} UTC",
        f"- Duration: {(run.end_time - run.start_time) / 60000:.1f} min" if run.end_time else "- Duration: running",
        f"- Parameters: `{json.dumps(params)}`",
        f"- URL: {run.run_page_url}",
        "",
        "| Task | State | Minutes | Exit summary |",
        "|---|---|---|---|",
    ]
    for task in sorted(run.tasks, key=lambda t: t.start_time or 0):
        state = task.state.result_state.value if task.state.result_state else task.state.life_cycle_state.value
        minutes = f"{(task.end_time - task.start_time) / 60000:.1f}" if task.end_time and task.start_time else ""
        summary = ""
        if task.notebook_task:
            out = w.jobs.get_run_output(task.run_id)
            summary = (out.notebook_output.result if out.notebook_output else "") or (out.error or "")
            html = w.jobs.export_run(task.run_id, views_to_export=ViewsToExport.CODE).views[0].content
            nb = OUT / "notebooks" / f"{job}_{run_id}_{task.task_key}.md"
            nb.parent.mkdir(parents=True, exist_ok=True)
            nb.write_text(
                f"# {task.task_key} (job {job}, run {run_id}, task run {task.run_id}, {state})\n\n{notebook_text(html)}\n"
            )
        lines.append(f"| {task.task_key} | {state} | {minutes} | `{summary[:400].replace('|', '/')}` |")
    path = OUT / "jobs" / f"{job}_{run_id}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    print(path)


def lakebase(w: WorkspaceClient) -> None:
    conn = review_store.connect(w, ENDPOINT)
    try:
        summary = review_store.status_summary(conn)
        with conn.cursor() as cur:
            cur.execute("""SELECT audit_id, source_system, local_column_name, old_status, new_status, new_global_column_name,
                                  action_by, action_source, action_at FROM harmonization_review.review_audit ORDER BY audit_id DESC LIMIT 40""")
            audit = cur.fetchall()
    finally:
        conn.close()
    text = [
        "# Lakebase review state",
        "",
        f"Endpoint `{ENDPOINT}`, schema `harmonization_review`.",
        "",
        "## Queue status",
        "",
        md_table(list(summary[0]) if summary else ["empty"], [list(r.values()) for r in summary]),
        "",
        "## Last 40 audit rows",
        "",
        md_table(list(audit[0]) if audit else ["empty"], [list(r.values()) for r in audit]),
    ]
    (OUT / "lakebase_review_state.md").write_text("\n".join(text) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default="pt")
    parser.add_argument("--warehouse-id", default="41754a8563a43a49")
    parser.add_argument("--runs", nargs="*", type=int, default=[])
    args = parser.parse_args()
    w = WorkspaceClient(profile=args.profile)
    OUT.mkdir(exist_ok=True)
    stamp = f"_Collected {dt.datetime.now(dt.UTC):%Y-%m-%d %H:%M} UTC by scripts/collect_evidence.py from workspace {w.config.host}._"
    for run_id in args.runs:
        job_run(w, run_id)
    for name, queries in SQL.items():
        body = [f"# {name.removesuffix('.md').replace('_', ' ').capitalize()}", "", stamp]
        for title, statement in queries:
            body += [
                "",
                f"## {title}",
                "",
                f"```sql\n{' '.join(statement.split())}\n```",
                "",
                run_sql(w, args.warehouse_id, statement),
            ]
        (OUT / name).write_text("\n".join(body) + "\n")
        print(OUT / name)
    lakebase(w)
    app = w.apps.get(APP)
    deployment = app.active_deployment
    (OUT / "app_status.md").write_text(
        "\n".join(
            [
                "# App status",
                "",
                stamp,
                "",
                f"- App: `{APP}`",
                f"- URL: {app.url}",
                f"- App state: {app.app_status.state.value if app.app_status else ''}, compute: {app.compute_status.state.value if app.compute_status else ''}",
                f"- Active deployment: {deployment.deployment_id if deployment else 'none'} ({deployment.status.state.value if deployment and deployment.status else ''})",
                f"- Resources: {', '.join(r.name for r in app.resources or [])}",
                "",
            ]
        )
    )


if __name__ == "__main__":
    main()
