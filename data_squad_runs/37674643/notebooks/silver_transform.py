from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, DateType, StringType
import hashlib

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "bronze_logistica_entregas_37674643"
SILVER_TABLE = "silver_logistica_entregas_37674643"
REJECTED_TABLE = "silver_logistica_entregas_37674643_rejected"

# ---------------------------------------------------------------------------
# 1. Read from Bronze
# ---------------------------------------------------------------------------
print(f"[Silver] Reading from Bronze table: {BRONZE_TABLE}")
df_bronze = spark.read.table(BRONZE_TABLE)
total_count = df_bronze.count()
print(f"[Silver] Total rows read from Bronze: {total_count}")

# ---------------------------------------------------------------------------
# 2. Schema casting
# ---------------------------------------------------------------------------
print("[Silver] Applying schema casts...")
df_cast = (
    df_bronze
    .withColumn("id_entrega",      F.col("id_entrega").cast(StringType()))
    .withColumn("nome_cliente",    F.col("nome_cliente").cast(StringType()))
    .withColumn("cpf_cliente",     F.col("cpf_cliente").cast(StringType()))
    .withColumn("email_cliente",   F.col("email_cliente").cast(StringType()))
    .withColumn("cidade_destino",  F.col("cidade_destino").cast(StringType()))
    .withColumn("status_entrega",  F.col("status_entrega").cast(StringType()))
    .withColumn("data_envio",      F.col("data_envio").cast(DateType()))
    .withColumn("peso_kg",         F.col("peso_kg").cast(FloatType()))
)

# ---------------------------------------------------------------------------
# 3. DQ — uniqueness helper: mark duplicate id_entrega
# ---------------------------------------------------------------------------
print("[Silver] Computing id_entrega uniqueness window...")
window_count = (
    df_cast
    .groupBy("id_entrega")
    .agg(F.count("*").alias("_id_count"))
)
df_with_count = df_cast.join(window_count, on="id_entrega", how="left")

# ---------------------------------------------------------------------------
# 4. Build error flag expressions  (ERROR severity only → rejection)
# ---------------------------------------------------------------------------
# Each condition below is TRUE when the row VIOLATES the rule.

cond_id_not_null      = F.col("id_entrega").isNull()
cond_id_unique        = F.col("_id_count") > 1
cond_id_regex         = ~F.col("id_entrega").rlike(r'^[A-Za-z0-9\-_]{1,50}$')

cond_nome_not_null    = F.col("nome_cliente").isNull()
# nome regex is WARNING → not used for rejection

cond_cpf_not_null     = F.col("cpf_cliente").isNull()
cond_cpf_regex        = ~F.col("cpf_cliente").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')

cond_email_not_null   = F.col("email_cliente").isNull()
cond_email_regex      = ~F.col("email_cliente").rlike(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$')

cond_cidade_not_null  = F.col("cidade_destino").isNull()
# cidade regex is WARNING → not used for rejection

cond_status_not_null  = F.col("status_entrega").isNull()
cond_status_ref       = ~F.col("status_entrega").isin('PENDENTE', 'EM_TRANSITO', 'ENTREGUE', 'CANCELADO', 'DEVOLVIDO')

cond_data_not_null    = F.col("data_envio").isNull()
cond_data_range       = ~F.col("data_envio").between('2000-01-01', F.current_date())

cond_peso_not_null    = F.col("peso_kg").isNull()
cond_peso_range       = (F.col("peso_kg") < 0.01) | (F.col("peso_kg") > 10000.00)

# Compose error messages as an array, then concatenate non-null entries
error_checks = [
    (cond_id_not_null,    "id_entrega: not_null violation"),
    (cond_id_unique,      "id_entrega: unique violation"),
    (cond_id_regex,       "id_entrega: regex violation"),
    (cond_nome_not_null,  "nome_cliente: not_null violation"),
    (cond_cpf_not_null,   "cpf_cliente: not_null violation"),
    (cond_cpf_regex,      "cpf_cliente: regex violation"),
    (cond_email_not_null, "email_cliente: not_null violation"),
    (cond_email_regex,    "email_cliente: regex violation"),
    (cond_cidade_not_null,"cidade_destino: not_null violation"),
    (cond_status_not_null,"status_entrega: not_null violation"),
    (cond_status_ref,     "status_entrega: referential violation"),
    (cond_data_not_null,  "data_envio: not_null violation"),
    (cond_data_range,     "data_envio: range violation"),
    (cond_peso_not_null,  "peso_kg: not_null violation"),
    (cond_peso_range,     "peso_kg: range violation"),
]

# Build _dq_error column: concatenate all triggered error messages
error_col = F.array(
    *[
        F.when(cond, F.lit(msg)).otherwise(F.lit(None).cast(StringType()))
        for cond, msg in error_checks
    ]
)

df_checked = df_with_count.withColumn("_dq_errors_array", error_col)

# _dq_error = pipe-separated string of all violations; NULL if none
df_checked = df_checked.withColumn(
    "_dq_error",
    F.concat_ws(" | ", F.expr("filter(_dq_errors_array, x -> x is not null)"))
)
# Convert empty string to null
df_checked = df_checked.withColumn(
    "_dq_error",
    F.when(F.col("_dq_error") == "", F.lit(None).cast(StringType())).otherwise(F.col("_dq_error"))
)

# ---------------------------------------------------------------------------
# 5. Split into accepted vs rejected
# ---------------------------------------------------------------------------
df_rejected = df_checked.filter(F.col("_dq_error").isNotNull())
df_accepted  = df_checked.filter(F.col("_dq_error").isNull())

rejected_count = df_rejected.count()
accepted_count  = df_accepted.count()

print(f"[Silver] Total rows   : {total_count}")
print(f"[Silver] Accepted rows: {accepted_count}")
print(f"[Silver] Rejected rows: {rejected_count}")

# ---------------------------------------------------------------------------
# 6. Write rejected table (keep _dq_error, drop helper cols)
# ---------------------------------------------------------------------------
rejected_cols_to_drop = ["_id_count", "_dq_errors_array"]
df_rejected_final = df_rejected.drop(*rejected_cols_to_drop)

print(f"[Silver] Writing rejected rows to: {REJECTED_TABLE}")
df_rejected_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTED_TABLE)
print(f"[Silver] Rejected table written: {REJECTED_TABLE}")

# ---------------------------------------------------------------------------
# 7. Masking on accepted rows
# ---------------------------------------------------------------------------
print("[Silver] Applying masking on accepted rows...")

def mask_value(col_expr):
    """Replace all characters except first 2 and last 2 with '*'."""
    return F.when(
        col_expr.isNull(), F.lit(None).cast(StringType())
    ).when(
        F.length(col_expr) <= 4,
        F.regexp_replace(col_expr, ".", "*")
    ).otherwise(
        F.concat(
            F.substring(col_expr, 1, 2),
            F.regexp_replace(F.substring(col_expr, 3, F.length(col_expr) - 4), ".", "*"),
            F.substring(col_expr, F.length(col_expr) - 1, 2)
        )
    )

# Columns with masking_strategy = 'mask': nome_cliente, cpf_cliente, email_cliente
df_masked = (
    df_accepted
    .withColumn("nome_cliente",  mask_value(F.col("nome_cliente")))
    .withColumn("cpf_cliente",   mask_value(F.col("cpf_cliente")))
    .withColumn("email_cliente", mask_value(F.col("email_cliente")))
)

# ---------------------------------------------------------------------------
# 8. Drop helper columns and write Silver table
# ---------------------------------------------------------------------------
silver_cols_to_drop = ["_id_count", "_dq_errors_array", "_dq_error"]
df_silver = df_masked.drop(*silver_cols_to_drop)

print(f"[Silver] Writing clean rows to Silver table: {SILVER_TABLE}")
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)

silver_written = spark.read.table(SILVER_TABLE).count()
print(f"[Silver] Rows confirmed in Silver table: {silver_written}")
print("[Silver] Notebook completed successfully.")
print(f"[Silver] Summary — Total: {total_count} | Accepted: {accepted_count} | Rejected: {rejected_count}")
