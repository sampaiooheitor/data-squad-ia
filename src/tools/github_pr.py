"""Create a GitHub branch, commit all artefacts, and open a pull request."""

import logging
from pathlib import Path

from github import Github, GithubException

from config import settings
from src.models import RunContext

logger = logging.getLogger(__name__)


def create_pr(ctx: RunContext) -> RunContext:
    gh = Github(settings.github_token)
    repo = gh.get_repo(settings.github_repo)

    branch_name = f"data-squad/{ctx.run_id}"
    base_sha = repo.get_branch(repo.default_branch).commit.sha

    logger.info("Creating branch %s on %s", branch_name, settings.github_repo)
    try:
        repo.create_git_ref(ref=f"refs/heads/{branch_name}", sha=base_sha)
    except GithubException as e:
        if e.status == 422:
            logger.warning("Branch %s already exists, reusing", branch_name)
        else:
            raise

    artefacts = _collect_artefacts(ctx)

    for path, content in artefacts.items():
        logger.info("Committing %s", path)
        try:
            existing = repo.get_contents(path, ref=branch_name)
            repo.update_file(
                path=path,
                message=f"data-squad/{ctx.run_id}: update {Path(path).name}",
                content=content,
                sha=existing.sha,
                branch=branch_name,
            )
        except GithubException:
            repo.create_file(
                path=path,
                message=f"data-squad/{ctx.run_id}: add {Path(path).name}",
                content=content,
                branch=branch_name,
            )

    table_name = ctx.schema.table_name if ctx.schema else "dataset"
    pr = repo.create_pull(
        title=f"[Data Squad] {ctx.tema}/{table_name} — run {ctx.run_id}",
        body=_pr_body(ctx),
        head=branch_name,
        base=repo.default_branch,
    )

    ctx.pr_url = pr.html_url
    logger.info("PR created: %s", ctx.pr_url)
    return ctx


def _collect_artefacts(ctx: RunContext) -> dict[str, str]:
    base = f"data_squad_runs/{ctx.run_id}"
    artefacts: dict[str, str] = {}

    artefacts[f"{base}/dataset.csv"] = ctx.csv_content

    if ctx.data_dict:
        artefacts[f"{base}/data_dict.json"] = ctx.data_dict.model_dump_json(indent=2)

    if ctx.schema:
        artefacts[f"{base}/schema.json"] = ctx.schema.model_dump_json(indent=2)

    if ctx.dq_rules:
        artefacts[f"{base}/dq_rules.json"] = ctx.dq_rules.model_dump_json(indent=2)

    if ctx.governance:
        artefacts[f"{base}/governance_metadata.json"] = ctx.governance.model_dump_json(indent=2)

    if ctx.bronze_notebook:
        artefacts[f"{base}/notebooks/bronze_ingest.py"] = ctx.bronze_notebook

    if ctx.silver_notebook:
        artefacts[f"{base}/notebooks/silver_transform.py"] = ctx.silver_notebook

    return artefacts


def _pr_body(ctx: RunContext) -> str:
    col_count = len(ctx.schema.columns) if ctx.schema else 0
    dq_count = len(ctx.dq_rules.rules) if ctx.dq_rules else 0
    pii_count = sum(1 for c in ctx.governance.columns if c.pii) if ctx.governance else 0

    return f"""## Data Squad — Automated Ingestion Run

| Field | Value |
|-------|-------|
| Run ID | `{ctx.run_id}` |
| Tema | {ctx.tema} |
| Table | `{ctx.schema.table_name if ctx.schema else "unknown"}` |
| Columns | {col_count} |
| DQ Rules | {dq_count} |
| PII Columns | {pii_count} |
| Bronze Table | `{ctx.table_bronze}` |
| Silver Table | `{ctx.table_silver}` |

> Generated automatically by [Data Squad](https://github.com/{settings.github_repo})
"""
