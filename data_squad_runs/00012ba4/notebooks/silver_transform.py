from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, DateType, StringType
import hashlib

spark = SparkSession.builder.getOrCreate()

bronze_table_name = "bronze_entregas_logistica_00012ba4"
silver_table_name = "silver_entregas_logistica_00012ba4"
rejected_table_name = "silver_entregas_logistica_00012ba4_rejected"

# ── 1. Read from Bronze ───────────────────────────────────────────────────────
df = spark.read.table(bronze_table_name)
total_count = df.count()
print(f"[SILVER] Total rows read from Bronze: {total_count}")

# ── 2. Schema casting ─────────────────────────────────────────────────────────
df = (
    df
    .withColumn("id_entrega",         F.col("id_entrega").cast(StringType()))
    .withColumn("nome_destinatario",  F.col("nome_destinatario").cast(StringType()))
    .withColumn("cpf_destinatario",   F.col("cpf_destinatario").cast(StringType()))
    .withColumn("email_destinatario", F.col("email_destinatario").cast(StringType()))
    .withColumn("cidade_destino",     F.col("cidade_destino").cast(StringType()))
    .withColumn("status_entrega",     F.col("status_entrega").cast(StringType()))
    .withColumn("peso_kg",            F.col("peso_kg").cast(FloatType()))
    .withColumn("data_entrega",       F.col("data_entrega").cast(DateType()))
)

# ── 3. DQ Rules (error severity only → rejected) ─────────────────────────────
# Build a list of error conditions; each will contribute a message to _dq_error.

# [error] id_entrega: not_null
cond_id_not_null = F.col("id_entrega").isNull()

# [error] id_entrega: regex
cond_id_regex = ~F.col("id_entrega").rlike(r'^[A-Za-z0-9\-_]{1,50}$')

# [error] nome_destinatario: not_null
cond_nome_not_null = F.col("nome_destinatario").isNull()

# [error] cpf_destinatario: not_null
cond_cpf_not_null = F.col("cpf_destinatario").isNull()

# [error] cpf_destinatario: regex
cond_cpf_regex = ~F.col("cpf_destinatario").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')

# [error] email_destinatario: not_null
cond_email_not_null = F.col("email_destinatario").isNull()

# [error] email_destinatario: regex
cond_email_regex = ~F.col("email_destinatario").rlike(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$')

# [error] cidade_destino: not_null
cond_cidade_not_null = F.col("cidade_destino").isNull()

# [error] status_entrega: not_null
cond_status_not_null = F.col("status_entrega").isNull()

# [error] status_entrega: referential
cond_status_ref = ~F.col("status_entrega").isin('PENDENTE', 'EM_TRANSITO', 'ENTREGUE', 'DEVOLVIDO', 'CANCELADO')

# [error] peso_kg: not_null
cond_peso_not_null = F.col("peso_kg").isNull()

# [error] peso_kg: range
cond_peso_range = ~F.col("peso_kg").between(0.01, 1000.0)

# [error] data_entrega: not_null
cond_data_not_null = F.col("data_entrega").isNull()

# [error] data_entrega: range  (cast back to date for comparison)
cond_data_range = ~F.col("data_entrega").between(
    F.lit("2000-01-01").cast(DateType()),
    F.lit("2100-12-31").cast(DateType())
)

# [error] data_entrega: regex — applied on original string before cast;
# We re-read the raw string value from bronze to apply regex on data_entrega
# Since after cast it's a date, we cast back to string for regex check
cond_data_regex = ~F.col("data_entrega").cast(StringType()).rlike(r'^\d{4}-\d{2}-\d{2}$')

# Build _dq_error column: concatenate all violated error messages
df = df.withColumn(
    "_dq_error",
    F.concat_ws("; ",
        F.when(cond_id_not_null,    F.lit("id_entrega: not_null violated")),
        F.when(
            F.col("id_entrega").isNotNull() & cond_id_regex,
            F.lit("id_entrega: regex violated")
        ),
        F.when(cond_nome_not_null,  F.lit("nome_destinatario: not_null violated")),
        F.when(cond_cpf_not_null,   F.lit("cpf_destinatario: not_null violated")),
        F.when(
            F.col("cpf_destinatario").isNotNull() & cond_cpf_regex,
            F.lit("cpf_destinatario: regex violated")
        ),
        F.when(cond_email_not_null, F.lit("email_destinatario: not_null violated")),
        F.when(
            F.col("email_destinatario").isNotNull() & cond_email_regex,
            F.lit("email_destinatario: regex violated")
        ),
        F.when(cond_cidade_not_null, F.lit("cidade_destino: not_null violated")),
        F.when(cond_status_not_null, F.lit("status_entrega: not_null violated")),
        F.when(
            F.col("status_entrega").isNotNull() & cond_status_ref,
            F.lit("status_entrega: referential violated")
        ),
        F.when(cond_peso_not_null,  F.lit("peso_kg: not_null violated")),
        F.when(
            F.col("peso_kg").isNotNull() & cond_peso_range,
            F.lit("peso_kg: range violated")
        ),
        F.when(cond_data_not_null,  F.lit("data_entrega: not_null violated")),
        F.when(
            F.col("data_entrega").isNotNull() & cond_data_range,
            F.lit("data_entrega: range violated")
        ),
        F.when(
            F.col("data_entrega").isNotNull() & cond_data_regex,
            F.lit("data_entrega: regex violated")
        ),
    )
)

# [error] id_entrega: unique — detect duplicates and flag them
window_count = df.groupBy("id_entrega").agg(F.count("*").alias("_id_count"))
df = df.join(window_count, on="id_entrega", how="left")
df = df.withColumn(
    "_dq_error",
    F.when(
        (F.col("_id_count") > 1) & (F.col("_dq_error") != ""),
        F.concat_ws("; ", F.col("_dq_error"), F.lit("id_entrega: unique violated"))
    ).when(
        F.col("_id_count") > 1,
        F.lit("id_entrega: unique violated")
    ).otherwise(F.col("_dq_error"))
).drop("_id_count")

# ── 4. Split into accepted and rejected ───────────────────────────────────────
df_rejected = df.filter((F.col("_dq_error").isNotNull()) & (F.col("_dq_error") != ""))
df_accepted = df.filter((F.col("_dq_error").isNull()) | (F.col("_dq_error") == "")).drop("_dq_error")

rejected_count = df_rejected.count()
accepted_count = df_accepted.count()

print(f"[SILVER] Total rows   : {total_count}")
print(f"[SILVER] Accepted rows: {accepted_count}")
print(f"[SILVER] Rejected rows: {rejected_count}")

# ── 5. Masking ────────────────────────────────────────────────────────────────
# nome_destinatario  → masking=mask  : show first char + asterisks
# cpf_destinatario   → masking=mask  : replace digits with '*' keeping format
# email_destinatario → masking=hash  : SHA-256 hex digest

# UDF for masking name (first word initial + masked rest)
@F.udf(StringType())
def mask_name(value):
    if value is None:
        return None
    parts = value.split()
    masked_parts = []
    for part in parts:
        if len(part) <= 1:
            masked_parts.append(part)
        else:
            masked_parts.append(part[0] + "*" * (len(part) - 1))
    return " ".join(masked_parts)

# UDF for masking CPF: keep format separators, replace digits
@F.udf(StringType())
def mask_cpf(value):
    if value is None:
        return None
    return "".join("*" if c.isdigit() else c for c in value)

# UDF for hashing email (SHA-256)
@F.udf(StringType())
def hash_email(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

df_accepted = (
    df_accepted
    .withColumn("nome_destinatario",  mask_name(F.col("nome_destinatario")))
    .withColumn("cpf_destinatario",   mask_cpf(F.col("cpf_destinatario")))
    .withColumn("email_destinatario", hash_email(F.col("email_destinatario")))
)

# ── 6. Write Silver (accepted) table ─────────────────────────────────────────
df_accepted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table_name)
print(f"[SILVER] Clean data written to '{silver_table_name}'. Rows: {accepted_count}")

# ── 7. Write Rejected table ───────────────────────────────────────────────────
if rejected_count > 0:
    df_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(rejected_table_name)
    print(f"[SILVER] Rejected data written to '{rejected_table_name}'. Rows: {rejected_count}")
else:
    print(f"[SILVER] No rejected rows. Table '{rejected_table_name}' not written.")

# ── 8. Final summary ─────────────────────────────────────────────────────────
print("\n===== SILVER LAYER SUMMARY =====")
print(f"  Bronze source  : {bronze_table_name}")
print(f"  Silver target  : {silver_table_name}")
print(f"  Rejected target: {rejected_table_name}")
print(f"  Total rows     : {total_count}")
print(f"  Accepted rows  : {accepted_count}")
print(f"  Rejected rows  : {rejected_count}")
print("================================")
