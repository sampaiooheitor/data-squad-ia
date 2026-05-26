"""Data Squad orchestrator — coordinates all pipeline phases."""

import asyncio
import copy
import logging
import os
import time

from dotenv import load_dotenv
load_dotenv()

_SKIP_PLAYWRIGHT = os.getenv("SKIP_PLAYWRIGHT", "false").lower() == "true"

from src.agents import (
    data_generator,
    dq_agent,
    governance_agent,
    pipeline_agent,
    reporter_agent,
    schema_agent,
)
from src.models import RunContext
from src.tools import databricks_client, github_pr, playwright_runner, sheets_reader, slack_notifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("orchestrator")


async def run_pipeline() -> RunContext:
    ctx = RunContext()
    timings: dict[str, float] = {}

    logger.info("=" * 60)
    logger.info("DATA SQUAD — run %s", ctx.run_id)
    logger.info("=" * 60)

    # ── Phase 1: Generate data ────────────────────────────────────────────
    ctx, timings["data_generator"] = await _timed(data_generator.run, ctx)
    logger.info("[1/7] Data generated — tema=%s, table=%s", ctx.tema, ctx.data_dict.table_name if ctx.data_dict else "?")

    # ── Phase 1b: Submit to Google Form ──────────────────────────────────
    loop = asyncio.get_event_loop()
    if _SKIP_PLAYWRIGHT:
        logger.info("[2/7] Playwright skipped — triggered by Google Forms")
    else:
        ctx, timings["playwright"] = await _timed_sync(loop, playwright_runner.submit_form, ctx)
        logger.info("[2/7] Form submitted via Playwright")

    # ── Phase 1c: Upload CSV to DBFS ─────────────────────────────────────
    ctx, timings["upload_csv"] = await _timed_sync(loop, databricks_client.upload_csv, ctx)
    logger.info("[3/7] CSV uploaded to DBFS — %s", ctx.csv_dbfs_path)

    # ── Phase 2: Schema inference ─────────────────────────────────────────
    ctx, timings["schema_agent"] = await _timed(schema_agent.run, ctx)
    logger.info("[4/7] Schema inferred — %d columns", len(ctx.schema.columns) if ctx.schema else 0)

    # ── Phase 3: DQ + Governance (parallel) ──────────────────────────────
    ctx, timings["dq_governance"] = await _timed_parallel_phase3(ctx)
    logger.info(
        "[5/7] DQ + Governance done — %d rules, %d classified columns",
        len(ctx.dq_rules.rules) if ctx.dq_rules else 0,
        len(ctx.governance.columns) if ctx.governance else 0,
    )

    # ── Phase 4: Pipeline generation ─────────────────────────────────────
    ctx, timings["pipeline_agent"] = await _timed(pipeline_agent.run, ctx)
    logger.info("[6/7] Notebooks generated — bronze=%s, silver=%s", ctx.table_bronze, ctx.table_silver)

    # ── Phase 5a: Create GitHub PR (sequential — needed before Slack) ────
    ctx, timings["github_pr"] = await _timed_sync(loop, github_pr.create_pr, ctx)
    logger.info("PR created: %s", ctx.pr_url)

    # ── Phase 5b: Reporter → Slack (async, pre-generate message) ─────────
    ctx = await reporter_agent.run(ctx)

    # ── Phase 5c: Databricks + Slack (parallel) ───────────────────────────
    ctx, timings["databricks_slack"] = await _timed_parallel_phase5(ctx, loop)
    logger.info("[7/7] Databricks executed + Slack notified (slack_sent=%s)", ctx.slack_sent)

    _print_summary(ctx, timings)
    return ctx


async def _timed_parallel_phase3(ctx: RunContext) -> tuple[RunContext, float]:
    t0 = time.perf_counter()
    dq_ctx, gov_ctx = await asyncio.gather(
        dq_agent.run(copy.deepcopy(ctx)),
        governance_agent.run(copy.deepcopy(ctx)),
    )
    ctx.dq_rules = dq_ctx.dq_rules
    ctx.governance = gov_ctx.governance
    return ctx, time.perf_counter() - t0


async def _timed_parallel_phase5(ctx: RunContext, loop: asyncio.AbstractEventLoop) -> tuple[RunContext, float]:
    t0 = time.perf_counter()

    db_ctx, slack_ctx = await asyncio.gather(
        loop.run_in_executor(None, databricks_client.deploy_and_run, copy.deepcopy(ctx)),
        loop.run_in_executor(None, slack_notifier.notify, copy.deepcopy(ctx)),
    )

    ctx.table_bronze = db_ctx.table_bronze
    ctx.table_silver = db_ctx.table_silver
    ctx.slack_sent = slack_ctx.slack_sent
    return ctx, time.perf_counter() - t0


async def _timed(fn, ctx: RunContext) -> tuple[RunContext, float]:
    t0 = time.perf_counter()
    result = await fn(ctx)
    return result, time.perf_counter() - t0


async def _timed_sync(loop, fn, ctx: RunContext) -> tuple[RunContext, float]:
    t0 = time.perf_counter()
    result = await loop.run_in_executor(None, fn, ctx)
    return result, time.perf_counter() - t0


def _print_summary(ctx: RunContext, timings: dict[str, float]) -> None:
    total = sum(timings.values())
    print("\n" + "=" * 60)
    print(f"  DATA SQUAD — run {ctx.run_id} COMPLETE")
    print("=" * 60)
    print(f"  Tema:         {ctx.tema}")
    print(f"  Table:        {ctx.schema.table_name if ctx.schema else 'N/A'}")
    print(f"  Bronze:       {ctx.table_bronze}")
    print(f"  Silver:       {ctx.table_silver}")
    print(f"  PR:           {ctx.pr_url}")
    print(f"  Slack sent:   {ctx.slack_sent}")
    print("-" * 60)
    for step, secs in timings.items():
        print(f"  {step:<25} {secs:>6.1f}s")
    print(f"  {'TOTAL':<25} {total:>6.1f}s")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
