import json

import anthropic

from config import settings, yaml_config
from src.models import PipelineOutput, RunContext

_client = anthropic.Anthropic()

_BRONZE_PREFIX = yaml_config.get("pipeline", {}).get("bronze_table_prefix", "bronze")
_SILVER_PREFIX = yaml_config.get("pipeline", {}).get("silver_table_prefix", "silver")


async def run(ctx: RunContext) -> RunContext:
    table_name = ctx.schema.table_name if ctx.schema else "dataset"
    ctx.table_bronze = f"{_BRONZE_PREFIX}_{table_name}_{ctx.run_id}"
    ctx.table_silver = f"{_SILVER_PREFIX}_{table_name}_{ctx.run_id}"

    context_summary = _build_context(ctx)

    response = _client.messages.create(
        model=settings.claude_model,
        max_tokens=8192,
        system="""You are a senior data engineer specializing in Databricks and Delta Lake.
Generate two Python notebooks for Databricks (PySpark).
Return ONLY valid JSON with two fields: "bronze_notebook" and "silver_notebook".
Each field contains the complete Python code as a string.

bronze_notebook requirements:
- Read CSV data from the embedded string (include the full CSV in the code as a multi-line string)
- Use pandas to parse, then spark.createDataFrame()
- Write to Delta table using saveAsTable() with mode="overwrite"
- Log row count after write

silver_notebook requirements:
- Read from the Bronze Delta table using spark.read.table()
- Apply schema casting (cast each column to its correct type)
- Apply DQ rules: rows failing "error" severity rules → write to _rejected table with _dq_error column
- Apply masking: columns with masking_strategy != "none" get masked/hashed/encrypted
- Write clean rows to Silver Delta table using saveAsTable() with mode="overwrite"
- Log counts: total, accepted, rejected

Use: from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate()
Do NOT use dbutils or magic commands. Plain Python only.""",
        messages=[
            {
                "role": "user",
                "content": context_summary,
            }
        ],
    )

    raw = response.content[0].text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    parsed = json.loads(raw)
    output = PipelineOutput(**parsed)

    ctx.bronze_notebook = output.bronze_notebook
    ctx.silver_notebook = output.silver_notebook
    return ctx


def _build_context(ctx: RunContext) -> str:
    parts = [
        f"Bronze table name: {ctx.table_bronze}",
        f"Silver table name: {ctx.table_silver}",
        "",
        "Schema:",
    ]
    if ctx.schema:
        for col in ctx.schema.columns:
            parts.append(f"  {col.name}: {col.type} (nullable={col.nullable}, pk={col.is_pk})")

    parts += ["", "DQ Rules:"]
    if ctx.dq_rules:
        for rule in ctx.dq_rules.rules:
            parts.append(f"  [{rule.severity}] {rule.column}: {rule.rule_type} — {rule.expression}")

    parts += ["", "Governance:"]
    if ctx.governance:
        for col in ctx.governance.columns:
            parts.append(
                f"  {col.name}: pii={col.pii}, sensitivity={col.sensitivity}, "
                f"masking={col.masking_strategy}"
            )

    parts += ["", "CSV data (embed this in bronze_notebook):"]
    parts.append(ctx.csv_content[:3000] + ("..." if len(ctx.csv_content) > 3000 else ""))

    return "\n".join(parts)
