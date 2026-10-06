"""
Deploy notebooks + shared module to the Databricks workspace and create/update
the lakehouse job. Safe to re-run (idempotent).

Run:  python databricks/deploy.py
Needs: DATABRICKS_HOST, DATABRICKS_TOKEN (or a `databricks auth login` profile)

Steps:
    1. connect and find the current user's workspace folder
    2. upload every file in databricks/notebooks/ (notebooks + transforms.py)
    3. build the job definition (5 serverless tasks in a chain)
    4. create the job, or reset it in place if it already exists

WHY DEPLOY FROM CODE instead of clicking in the UI:
    The job definition (task order, parameters, concurrency) is versioned in
    git next to the notebooks. A fresh workspace is one command away, and the
    README never says "click here, then here".

WHY SERVERLESS (no cluster spec on tasks):
    Free Edition is serverless-only. Leaving out cluster config makes Databricks
    run each task on serverless compute. No idle clusters, no cluster tuning.

Edge cases handled:
    - no credentials at all -> clear message (the SDK error is cryptic)
    - several jobs with the same name -> the first is updated and a warning is
      printed, because the Airflow operator looks the job up BY NAME and would
      fail on duplicates
"""

import io
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace

JOB_NAME = "payflow-lakehouse"
CATALOG = os.getenv("PAYFLOW_CATALOG") or "payflow"
NB_DIR = Path(__file__).resolve().parent / "notebooks"

# task_key -> (notebook file stem, upstream task)
TASKS = [
    ("setup", "00_setup", None),
    ("bronze", "01_bronze", "setup"),
    ("silver", "02_silver", "bronze"),
    ("quality", "03_quality", "silver"),
    ("gold", "04_gold", "quality"),
]


def main() -> None:
    if not os.getenv("DATABRICKS_HOST") and not (Path.home() / ".databrickscfg").exists():
        sys.exit("Set DATABRICKS_HOST/DATABRICKS_TOKEN in .env (or run `databricks auth login`).")

    # 1. Who am I? Notebooks go under the user's own folder.
    w = WorkspaceClient()  # reads DATABRICKS_HOST / DATABRICKS_TOKEN from env
    me = w.current_user.me().user_name
    base = f"/Workspace/Users/{me}/payflow"
    w.workspace.mkdirs(base)

    # 2. ImportFormat.AUTO: files starting with "# Databricks notebook source"
    #    become notebooks (extension dropped); transforms.py becomes a plain
    #    workspace file that the notebooks can `import`.
    for f in sorted(NB_DIR.glob("*.py")):
        w.workspace.upload(f"{base}/{f.name}", io.BytesIO(f.read_bytes()),
                           format=workspace.ImportFormat.AUTO, overwrite=True)
        print(f"uploaded {f.name}")

    # 3. Job definition.
    settings = jobs.JobSettings(
        name=JOB_NAME,
        # One run at a time. Silver's high-watermark relies on it, and it keeps
        # Free Edition's 5-concurrent-task limit far away.
        max_concurrent_runs=1,
        # Job parameters are pushed to every notebook's widgets.
        parameters=[
            jobs.JobParameterDefinition(name="catalog", default=CATALOG),
            jobs.JobParameterDefinition(name="full_refresh", default="false"),
        ],
        tasks=[
            jobs.Task(
                task_key=key,
                notebook_task=jobs.NotebookTask(notebook_path=f"{base}/{stem}"),
                depends_on=[jobs.TaskDependency(task_key=up)] if up else None,
                max_retries=1,
                timeout_seconds=1800,
            )
            for key, stem, up in TASKS
        ],
    )

    # 4. Create or update in place (job id stays stable across deploys).
    existing = list(w.jobs.list(name=JOB_NAME))
    if len(existing) > 1:
        print(f"WARNING: {len(existing)} jobs named {JOB_NAME}; Airflow needs exactly one. Delete the extras.")
    if existing:
        job_id = existing[0].job_id
        w.jobs.reset(job_id=job_id, new_settings=settings)
        print(f"updated job {JOB_NAME} ({job_id})")
    else:
        job_id = w.jobs.create(**settings.as_shallow_dict()).job_id
        print(f"created job {JOB_NAME} ({job_id})")


if __name__ == "__main__":
    main()
