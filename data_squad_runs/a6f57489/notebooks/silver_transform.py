from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, DateType, StringType
import hashlib

spark = SparkSession.builder.getOrCreate()

bronze_table_name = "bronze_logistica_entregas_a6f57489"
silver_table_name = "silver_logistica_entregas_a6f57489"
rejected_table_name = "silver_logistica_entregas_a6f57489_rejected"

# ── 1. Read Bronze ──────────────────────────────────────────────────────────
df = spark.read.table(bronze_table_name)
total_count = df.count()
print(f"[SILVER] Total rows read from bronze: {total_count}")

# ── 2. Schema Casting ───────────────────────────────────────────────────────
df = (
    df
    .withColumn("id_entrega",      F.col("id_entrega").cast(StringType()))
    .withColumn("nome_motorista",  F.col("nome_motorista").cast(StringType()))
    .withColumn("cpf_motorista",   F.col("cpf_motorista").cast(StringType()))
    .withColumn("email_cliente",   F.col("email_cliente").cast(StringType()))
    .withColumn("cidade_destino",  F.col("cidade_destino").cast(StringType()))
    .withColumn("peso_kg",         F.col("peso_kg").cast(FloatType()))
    .withColumn("status_entrega",  F.col("status_entrega").cast(StringType()))
    .withColumn("data_entrega",    F.to_date(F.col("data_entrega"), "yyyy-MM-dd"))
)

# ── 3. DQ Rules (ERROR severity only → rejected) ───────────────────────────
# Build individual error condition columns
df = df.withColumn("_err_id_entrega_not_null",
        F.col("id_entrega").isNull())

df = df.withColumn("_err_id_entrega_regex",
        ~F.col("id_entrega").rlike(r'^[A-Za-z0-9_-]{1,50}$') & F.col("id_entrega").isNotNull())

df = df.withColumn("_err_nome_motorista_not_null",
        F.col("nome_motorista").isNull())

df = df.withColumn("_err_cpf_motorista_not_null",
        F.col("cpf_motorista").isNull())

df = df.withColumn("_err_cpf_motorista_regex",
        ~F.col("cpf_motorista").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$') & F.col("cpf_motorista").isNotNull())

df = df.withColumn("_err_email_cliente_not_null",
        F.col("email_cliente").isNull())

df = df.withColumn("_err_email_cliente_regex",
        ~F.col("email_cliente").rlike(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$') & F.col("email_cliente").isNotNull())

df = df.withColumn("_err_cidade_destino_not_null",
        F.col("cidade_destino").isNull())

df = df.withColumn("_err_peso_kg_not_null",
        F.col("peso_kg").isNull())

df = df.withColumn("_err_peso_kg_range",
        ~F.col("peso_kg").between(0.1, 50000.0) & F.col("peso_kg").isNotNull())

df = df.withColumn("_err_status_entrega_not_null",
        F.col("status_entrega").isNull())

df = df.withColumn("_err_status_entrega_referential",
        ~F.col("status_entrega").isin("PENDENTE", "EM_TRANSITO", "ENTREGUE", "CANCELADO", "DEVOLVIDO") & F.col("status_entrega").isNotNull())

df = df.withColumn("_err_data_entrega_not_null",
        F.col("data_entrega").isNull())

# Uniqueness check for id_entrega (error)
from pyspark.sql.window import Window
window_id = Window.partitionBy("id_entrega")
df = df.withColumn("_id_entrega_cnt", F.count("id_entrega").over(window_id))
df = df.withColumn("_err_id_entrega_unique", F.col("_id_entrega_cnt") > 1)
df = df.drop("_id_entrega_cnt")

# Collect all error flags into a single _dq_error string column
error_flag_cols = [
    "_err_id_entrega_not_null",
    "_err_id_entrega_unique",
    "_err_id_entrega_regex",
    "_err_nome_motorista_not_null",
    "_err_cpf_motorista_not_null",
    "_err_cpf_motorista_regex",
    "_err_email_cliente_not_null",
    "_err_email_cliente_regex",
    "_err_cidade_destino_not_null",
    "_err_peso_kg_not_null",
    "_err_peso_kg_range",
    "_err_status_entrega_not_null",
    "_err_status_entrega_referential",
    "_err_data_entrega_not_null",
]

error_messages = {
    "_err_id_entrega_not_null":          "id_entrega:not_null",
    "_err_id_entrega_unique":            "id_entrega:unique",
    "_err_id_entrega_regex":             "id_entrega:regex",
    "_err_nome_motorista_not_null":      "nome_motorista:not_null",
    "_err_cpf_motorista_not_null":       "cpf_motorista:not_null",
    "_err_cpf_motorista_regex":          "cpf_motorista:regex",
    "_err_email_cliente_not_null":       "email_cliente:not_null",
    "_err_email_cliente_regex":          "email_cliente:regex",
    "_err_cidade_destino_not_null":      "cidade_destino:not_null",
    "_err_peso_kg_not_null":             "peso_kg:not_null",
    "_err_peso_kg_range":                "peso_kg:range(0.1-50000.0)",
    "_err_status_entrega_not_null":      "status_entrega:not_null",
    "_err_status_entrega_referential":   "status_entrega:referential",
    "_err_data_entrega_not_null":        "data_entrega:not_null",
}

# Build _dq_error column by concatenating triggered error messages
dq_error_expr = F.concat_ws(", ", *[
    F.when(F.col(flag), F.lit(msg)).otherwise(F.lit(None))
    for flag, msg in error_messages.items()
])

df = df.withColumn("_dq_error", dq_error_expr)

# A row is rejected if ANY error flag is True
any_error_condition = F.lit(False)
for flag in error_flag_cols:
    any_error_condition = any_error_condition | F.col(flag)

df = df.withColumn("_has_error", any_error_condition)

# ── 4. Split into rejected / accepted ──────────────────────────────────────
business_cols = ["id_entrega", "nome_motorista", "cpf_motorista", "email_cliente",
                 "cidade_destino", "peso_kg", "status_entrega", "data_entrega"]

df_rejected = df.filter(F.col("_has_error") == True).select(
    *business_cols,
    F.col("_dq_error")
)

df_accepted = df.filter(F.col("_has_error") == False).select(*business_cols)

rejected_count = df_rejected.count()
accepted_count = df_accepted.count()

print(f"[SILVER] Total rows:    {total_count}")
print(f"[SILVER] Accepted rows: {accepted_count}")
print(f"[SILVER] Rejected rows: {rejected_count}")

# ── 5. Write Rejected table ─────────────────────────────────────────────────
if rejected_count > 0:
    df_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(rejected_table_name)
    print(f"[SILVER] Rejected rows written to '{rejected_table_name}'.")
else:
    print(f"[SILVER] No rejected rows. Skipping write to '{rejected_table_name}'.")

# ── 6. Apply Masking on accepted rows ──────────────────────────────────────
# nome_motorista  → mask  (keep first char + asterisks)
# cpf_motorista   → mask  (keep first 3 chars + asterisks)
# email_cliente   → hash  (SHA-256 hex digest)

# UDF for masking nome_motorista: show first word, mask the rest
@F.udf(StringType())
def mask_name(value):
    if value is None:
        return None
    parts = value.strip().split()
    if len(parts) == 0:
        return "***"
    masked_parts = [parts[0]] + ["*" * len(p) for p in parts[1:]]
    return " ".join(masked_parts)

# UDF for masking cpf_motorista: keep XXX.*** *** -**
@F.udf(StringType())
def mask_cpf(value):
    if value is None:
        return None
    # Keep first segment (3 digits) visible, mask the rest
    parts = value.split(".")
    if len(parts) >= 1:
        return parts[0] + ".***.***.** "
    return "***.***.***-**"

# UDF for hashing email_cliente: SHA-256
@F.udf(StringType())
def hash_email(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

df_masked = (
    df_accepted
    .withColumn("nome_motorista", mask_name(F.col("nome_motorista")))
    .withColumn("cpf_motorista",  mask_cpf(F.col("cpf_motorista")))
    .withColumn("email_cliente",  hash_email(F.col("email_cliente")))
)

# ── 7. Write Silver table ───────────────────────────────────────────────────
df_masked.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table_name)

print(f"[SILVER] Clean rows written to '{silver_table_name}'.")
print(f"[SILVER] Pipeline complete. Total={total_count}, Accepted={accepted_count}, Rejected={rejected_count}")
