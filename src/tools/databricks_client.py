"""Import and execute notebooks in Databricks via Workspace API and Jobs Runs Submit API."""

import base64
import logging
import time

import requests

from config import settings, yaml_config
from src.models import RunContext

logger = logging.getLogger(__name__)

_NOTEBOOK_BASE = yaml_config.get("pipeline", {}).get(
    "databricks_notebook_base_path", "/data_squad"
)
_POLL_INTERVAL = 5
_MAX_WAIT = 300


def upload_csv(ctx: RunContext) -> RunContext:
    host = settings.databricks_host.rstrip("/")
    token = settings.databricks_token
    table_name = ctx.data_dict.table_name if ctx.data_dict else ctx.run_id
    volume_path = f"/Volumes/prd/bronze/landing/{table_name}/raw.csv"

    resp = requests.put(
        f"{host}/api/2.0/fs/files{volume_path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        },
        data=ctx.csv_content.encode("utf-8"),
        timeout=30,
    )
    resp.raise_for_status()
    ctx.csv_dbfs_path = volume_path
    logger.info("CSV uploaded to Volume: %s", ctx.csv_dbfs_path)
    return ctx


def deploy_and_run(ctx: RunContext) -> RunContext:
    host = settings.databricks_host.rstrip("/")
    token = settings.databricks_token

    bronze_path = f"{_NOTEBOOK_BASE}/{ctx.run_id}/bronze_ingest"
    silver_path = f"{_NOTEBOOK_BASE}/{ctx.run_id}/silver_transform"

    logger.info("Importing notebooks to Databricks workspace — host=%s", host)
    _ensure_folder(host, token, _NOTEBOOK_BASE)
    _ensure_folder(host, token, f"{_NOTEBOOK_BASE}/{ctx.run_id}")
    _import_notebook(host, token, bronze_path, ctx.bronze_notebook)
    _import_notebook(host, token, silver_path, ctx.silver_notebook)

    logger.info("Executing bronze_ingest via Jobs Runs Submit")
    _run_notebook_and_wait(host, token, bronze_path, f"data-squad-bronze-{ctx.run_id}")
    logger.info("Bronze table created: %s", ctx.table_bronze)

    logger.info("Executing silver_transform via Jobs Runs Submit")
    _run_notebook_and_wait(host, token, silver_path, f"data-squad-silver-{ctx.run_id}")
    logger.info("Silver table created: %s", ctx.table_silver)

    return ctx


def _ensure_folder(host: str, token: str, path: str) -> None:
    requests.post(
        f"{host}/api/2.0/workspace/mkdirs",
        headers={"Authorization": f"Bearer {token}"},
        json={"path": path},
        timeout=30,
    ).raise_for_status()


def _import_notebook(host: str, token: str, path: str, content: str) -> None:
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    resp = requests.post(
        f"{host}/api/2.0/workspace/import",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "path": path,
            "content": encoded,
            "format": "SOURCE",
            "language": "PYTHON",
            "overwrite": True,
        },
        timeout=30,
    )
    resp.raise_for_status()


def _run_notebook_and_wait(host: str, token: str, notebook_path: str, run_name: str) -> None:
    resp = requests.post(
        f"{host}/api/2.0/jobs/runs/submit",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "run_name": run_name,
            "tasks": [
                {
                    "task_key": "run",
                    "notebook_task": {"notebook_path": notebook_path},
                }
            ],
        },
        timeout=30,
    )
    resp.raise_for_status()
    logger.info("Jobs API response: %s %s", resp.status_code, resp.text[:500])
    run_id = resp.json()["run_id"]

    elapsed = 0
    while elapsed < _MAX_WAIT:
        status_resp = requests.get(
            f"{host}/api/2.0/jobs/runs/get",
            headers={"Authorization": f"Bearer {token}"},
            params={"run_id": run_id},
            timeout=30,
        )
        status_resp.raise_for_status()
        state = status_resp.json().get("state", {})
        lifecycle = state.get("life_cycle_state", "PENDING")

        if lifecycle == "TERMINATED":
            result = state.get("result_state", "FAILED")
            if result != "SUCCESS":
                raise RuntimeError(f"Notebook {notebook_path} failed: {result}")
            return
        if lifecycle in ("INTERNAL_ERROR", "SKIPPED"):
            state_msg = state.get("state_message", "no details")
            logger.error("Notebook failed — state: %s, message: %s", lifecycle, state_msg)
            raise RuntimeError(f"Notebook {notebook_path} lifecycle: {lifecycle} — {state_msg}")

        time.sleep(_POLL_INTERVAL)
        elapsed += _POLL_INTERVAL

    raise TimeoutError(f"Notebook {notebook_path} did not finish within {_MAX_WAIT}s")
