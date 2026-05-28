import json

import anthropic

from config import settings, yaml_config
from src.models import RunContext

_client = anthropic.Anthropic()

_CATALOG = yaml_config.get("pipeline", {}).get("catalog", "prd")
_BRONZE_SCHEMA = yaml_config.get("pipeline", {}).get("bronze_schema", "bronze")
_SILVER_SCHEMA = yaml_config.get("pipeline", {}).get("silver_schema", "silver")


def _build_bronze_notebook(source_path: str, table_name: str, source_system: str) -> str:
    return f"""\
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

SOURCE_PATH   = "{source_path}"
TABLE_NAME    = "{table_name}"
SOURCE_SYSTEM = "{source_system}"

df = spark.read.option("header", "true").option("sep", "|").option("mergeSchema", "true").csv(SOURCE_PATH)

df = (
    df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
)

(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_NAME)
)
print(f"Bronze: {{df.count()}} rows appended to {{TABLE_NAME}}")
"""


def _build_silver_notebook(
    bronze_table: str, silver_table: str, rejected_table: str,
    casting_block: str, dq_block: str, masking_block: str,
) -> str:
    return f"""\
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE   = "{bronze_table}"
SILVER_TABLE   = "{silver_table}"
REJECTED_TABLE = "{rejected_table}"

df = spark.read.table(BRONZE_TABLE)
total = df.count()

# Schema casting
{casting_block}

# DQ filtering
{dq_block}

# Masking
{masking_block}

df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTED_TABLE)
print(f"Silver: {{df_clean.count()}} accepted, {{df_rejected.count()}} rejected / {{total}} total")
print(f"  Clean    -> {{SILVER_TABLE}}")
print(f"  Rejected -> {{REJECTED_TABLE}}")
"""


async def run(ctx: RunContext) -> RunContext:
    table_name = ctx.schema.table_name if ctx.schema else "dataset"
    ctx.table_bronze = f"{_CATALOG}.{_BRONZE_SCHEMA}.{table_name}"
    ctx.table_silver = f"{_CATALOG}.{_SILVER_SCHEMA}.{table_name}"
    rejected_table = f"{_CATALOG}.{_SILVER_SCHEMA}.{table_name}_rejected"

    response = _client.messages.create(
        model=settings.claude_model,
        max_tokens=4096,
        system="""You are a senior data engineer. Generate ONLY PySpark code blocks for the Silver notebook.
Return valid JSON with exactly three string fields: "casting_block", "dq_block", "masking_block".

casting_block: Cast each column from StringType (as read from Bronze) to its correct type.
  Ignore metadata columns (_ingested_at, _source_file, _source_system) — leave them as-is.
  - string  → no cast needed (skip)
  - integer → df = df.withColumn("COL", F.col("COL").cast("integer"))
  - float   → df = df.withColumn("COL", F.col("COL").cast("double"))
  - boolean → df = df.withColumn("COL", F.col("COL").cast("boolean"))
  - date    → df = df.withColumn("COL", F.to_date(F.col("COL"), "yyyy-MM-dd"))
  - timestamp → df = df.withColumn("COL", F.to_timestamp(F.col("COL"), "yyyy-MM-dd HH:mm:ss"))

dq_block: Split df into df_clean and df_rejected using error-severity DQ rules.
  Rows passing all conditions → df_clean.
  Rows failing → df_rejected with column _dq_error.
  IMPORTANT: Use ONLY simple column-level conditions — isNotNull(), >, <, ==, isin(), rlike().
  NEVER use window functions, groupBy, intermediate withColumn, or multi-step computations.
  NEVER reference a column you did not read from the original df.
  Example:
    error_cond = (F.col("X").isNotNull()) & (F.col("Y") > 0)
    df_clean    = df.filter(error_cond)
    df_rejected = df.filter(~error_cond).withColumn("_dq_error", F.lit("DQ rule failed"))
  If no error-severity rules exist:
    df_clean = df
    df_rejected = df.filter(F.lit(False)).withColumn("_dq_error", F.lit(""))

masking_block: Mask/hash columns on df_clean. No pandas, no UDFs.
  - hash    → df_clean = df_clean.withColumn("COL", F.sha2(F.col("COL").cast("string"), 256))
  - mask    → df_clean = df_clean.withColumn("COL", F.regexp_replace(F.col("COL").cast("string"), ".", "*"))
  - encrypt → df_clean = df_clean.withColumn("COL", F.sha2(F.col("COL").cast("string"), 512))
  - none    → skip

Return ONLY the JSON, no markdown fences.""",
        messages=[{"role": "user", "content": _build_context(ctx)}],
    )

    raw = response.content[0].text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    blocks = json.loads(raw)

    ctx.bronze_notebook = _build_bronze_notebook(
        source_path=ctx.csv_dbfs_path,
        table_name=ctx.table_bronze,
        source_system="data_squad",
    )
    ctx.silver_notebook = _build_silver_notebook(
        bronze_table=ctx.table_bronze,
        silver_table=ctx.table_silver,
        rejected_table=rejected_table,
        casting_block=blocks["casting_block"],
        dq_block=blocks["dq_block"],
        masking_block=blocks["masking_block"],
    )
    return ctx


def _build_context(ctx: RunContext) -> str:
    parts = ["Schema (columns to cast in Silver):"]
    if ctx.schema:
        for col in ctx.schema.columns:
            parts.append(f"  {col.name}: {col.type} (nullable={col.nullable}, pk={col.is_pk})")

    parts += ["", "DQ Rules:"]
    if ctx.dq_rules:
        for rule in ctx.dq_rules.rules:
            parts.append(f"  [{rule.severity}] {rule.column}: {rule.rule_type} — {rule.expression}")

    parts += ["", "Masking (column → strategy):"]
    if ctx.governance:
        for col in ctx.governance.columns:
            if col.masking_strategy != "none":
                parts.append(f"  {col.name}: {col.masking_strategy}")

    return "\n".join(parts)
