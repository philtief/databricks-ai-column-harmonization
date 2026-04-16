"""
deploy_workflow.py
==================
Deploy or update the Databricks workflow:
  Column_Mapping_To_Global_Model

Uses the Databricks SDK (databricks-sdk).

Usage:
    export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
    export DATABRICKS_TOKEN=<your-pat-or-sp-token>
    python workflow/deploy_workflow.py

Alternatively, configure ~/.databrickscfg with a profile and the
SDK will pick it up automatically.
"""

import sys

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import (
        Task,
        NotebookTask,
        JobParameterDefinition,
        TaskDependency,
        Source,
    )
except ImportError:
    print("ERROR: databricks-sdk is not installed.")
    print("       Run: pip install databricks-sdk")
    sys.exit(1)


# ----------------------------------------------------------------
# Configuration — update NOTEBOOK_BASE_PATH to match your deployment
# ----------------------------------------------------------------
WORKFLOW_NAME      = "Column_Mapping_To_Global_Model"
NOTEBOOK_BASE_PATH = "/Workspace/Users/<your-user>/databricks-column-harmonization/files/notebooks"

JOB_PARAMETERS = [
    JobParameterDefinition(name="catalog_name",    default="my_catalog"),
    JobParameterDefinition(name="schema_name",     default="harmonizing_agent"),
    JobParameterDefinition(name="country_code",    default="ES"),
    JobParameterDefinition(name="source_country",  default="Spain"),
    JobParameterDefinition(name="ai_endpoint",     default="databricks-gpt-5-2"),
    JobParameterDefinition(name="mapping_version", default="v1"),
]

JOB_TAGS = {
    "project": "column-harmonization",
    "domain":  "insurance",
    "country": "spain",
    "pattern": "column-mapping-with-app-review",
}

# ----------------------------------------------------------------
# Task definitions
# Serverless notebook tasks: no job_cluster_key, no new_cluster.
# ----------------------------------------------------------------
TASKS = [
    Task(
        task_key="bootstrap_catalog",
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/00_bootstrap_catalog.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="generate_spain_raw_data",
        depends_on=[TaskDependency(task_key="bootstrap_catalog")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/01_generate_spain_raw_data.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="create_global_model_and_control_tables",
        depends_on=[TaskDependency(task_key="bootstrap_catalog")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/02_create_global_model_and_control_tables.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="inventory_source_columns",
        depends_on=[
            TaskDependency(task_key="generate_spain_raw_data"),
            TaskDependency(task_key="create_global_model_and_control_tables"),
        ],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/03_inventory_source_columns.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="ai_propose_column_mappings",
        depends_on=[TaskDependency(task_key="inventory_source_columns")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/04_ai_propose_column_mappings.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="prepare_app_review_views",
        depends_on=[TaskDependency(task_key="ai_propose_column_mappings")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/05_prepare_app_review_views.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="column_mapping_review_gate",
        depends_on=[TaskDependency(task_key="prepare_app_review_views")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/06_column_mapping_review_gate.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="build_column_mapping_dictionary",
        depends_on=[TaskDependency(task_key="column_mapping_review_gate")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/07_build_column_mapping_dictionary.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="apply_approved_column_mappings",
        depends_on=[TaskDependency(task_key="build_column_mapping_dictionary")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/08_apply_approved_column_mappings.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="optional_value_mapping",
        depends_on=[TaskDependency(task_key="apply_approved_column_mappings")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/09_optional_value_mapping.py",
            source=Source.WORKSPACE,
        ),
    ),
    Task(
        task_key="validate_and_monitor",
        depends_on=[TaskDependency(task_key="optional_value_mapping")],
        notebook_task=NotebookTask(
            notebook_path=f"{NOTEBOOK_BASE_PATH}/10_validate_and_monitor.py",
            source=Source.WORKSPACE,
        ),
    ),
]


# ----------------------------------------------------------------
# Deploy function
# ----------------------------------------------------------------
def deploy():
    print("Connecting to Databricks workspace ...")
    w = WorkspaceClient()

    host = w.config.host
    print(f"  Host: {host}")

    existing_job = None
    print(f"  Searching for existing job '{WORKFLOW_NAME}' ...")

    for job in w.jobs.list():
        if job.settings and job.settings.name == WORKFLOW_NAME:
            existing_job = job
            break

    if existing_job is not None:
        job_id = existing_job.job_id
        print(f"  Found existing job: id={job_id}  name='{WORKFLOW_NAME}'")
        print(f"  Updating (reset) existing job ...")

        w.jobs.reset(
            job_id=job_id,
            new_settings={
                "name":                WORKFLOW_NAME,
                "tasks":               TASKS,
                "parameters":          JOB_PARAMETERS,
                "tags":                JOB_TAGS,
                "max_concurrent_runs": 1,
            },
        )

        print(f"  Job updated successfully.")
        action = "updated"

    else:
        print(f"  No existing job found. Creating new job ...")

        created = w.jobs.create(
            name=WORKFLOW_NAME,
            tasks=TASKS,
            parameters=JOB_PARAMETERS,
            tags=JOB_TAGS,
            max_concurrent_runs=1,
        )
        job_id = created.job_id
        print(f"  Job created successfully. id={job_id}")
        action = "created"

    host_clean = host.rstrip("/")
    job_url = f"{host_clean}/#job/{job_id}"

    print()
    print("=" * 70)
    print(f"  Workflow {action.upper()} successfully.")
    print(f"  Name   : {WORKFLOW_NAME}")
    print(f"  Job ID : {job_id}")
    print(f"  URL    : {job_url}")
    print("=" * 70)
    print()
    print("  Tasks (11 total):")
    for task in TASKS:
        deps = ", ".join(d.task_key for d in (task.depends_on or []))
        deps_str = f" (depends: {deps})" if deps else " (no dependencies)"
        print(f"    {task.task_key}{deps_str}")
    print()
    print("  Parameters:")
    for p in JOB_PARAMETERS:
        print(f"    {p.name} = {p.default}")
    print()
    print("  Next steps:")
    print("    1. Run the workflow from the Databricks UI or via:")
    print(f"       databricks jobs run-now --job-id {job_id}")
    print("    2. After task 'prepare_app_review_views' completes,")
    print("       review pending column mappings in the Databricks App.")
    print("    3. Re-run from task 'column_mapping_review_gate' once all")
    print("       14 mandatory columns are approved in the App.")

    return job_id, job_url


if __name__ == "__main__":
    deploy()
