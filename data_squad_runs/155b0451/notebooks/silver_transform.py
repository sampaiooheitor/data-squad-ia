from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, TimestampType, StringType
import hashlib

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "bronze_transacoes_financeiras_155b0451"
SILVER_TABLE = "silver_transacoes_financeiras_155b0451"
REJECTED_TABLE = "silver_transacoes_financeiras_155b0451_rejected"

# ── 1. Read Bronze ──────────────────────────────────────────────────────────
df_bronze = spark.read.table(BRONZE_TABLE)
total_count = df_bronze.count()
print(f"[SILVER] Total rows read from bronze: {total_count}")

# ── 2. Schema Casting ───────────────────────────────────────────────────────
df_cast = (
    df_bronze
    .withColumn("id_transacao",   F.col("id_transacao").cast(StringType()))
    .withColumn("nome_completo",  F.col("nome_completo").cast(StringType()))
    .withColumn("cpf",            F.col("cpf").cast(StringType()))
    .withColumn("email",          F.col("email").cast(StringType()))
    .withColumn("tipo_transacao", F.col("tipo_transacao").cast(StringType()))
    .withColumn("valor",          F.col("valor").cast(FloatType()))
    .withColumn("data_transacao", F.col("data_transacao").cast(TimestampType()))
    .withColumn("status",         F.col("status").cast(StringType()))
)

# ── 3. DQ – Error Rules ─────────────────────────────────────────────────────
# Each error condition returns True when the row FAILS the rule.

# id_transacao: not_null
cond_id_not_null = F.col("id_transacao").isNull()

# id_transacao: regex
cond_id_regex = ~F.col("id_transacao").rlike(r'^[a-zA-Z0-9\-_]{1,64}$')

# nome_completo: not_null
cond_nome_not_null = F.col("nome_completo").isNull()

# cpf: not_null
cond_cpf_not_null = F.col("cpf").isNull()

# cpf: regex
cond_cpf_regex = ~F.col("cpf").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')

# email: not_null
cond_email_not_null = F.col("email").isNull()

# email: regex
cond_email_regex = ~F.col("email").rlike(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')

# tipo_transacao: not_null
cond_tipo_not_null = F.col("tipo_transacao").isNull()

# tipo_transacao: referential
cond_tipo_ref = ~F.col("tipo_transacao").isin('CREDITO', 'DEBITO', 'TRANSFERENCIA', 'PIX', 'TED', 'DOC', 'BOLETO')

# valor: not_null
cond_valor_not_null = F.col("valor").isNull()

# valor: range [0.01, 1000000.00]
cond_valor_range = ~F.col("valor").between(0.01, 1000000.00)

# data_transacao: not_null
cond_data_not_null = F.col("data_transacao").isNull()

# data_transacao: range
cond_data_range = ~(
    (F.col("data_transacao") >= F.lit("2000-01-01 00:00:00").cast(TimestampType())) &
    (F.col("data_transacao") <= F.current_timestamp())
)

# status: not_null
cond_status_not_null = F.col("status").isNull()

# status: referential
cond_status_ref = ~F.col("status").isin('PENDENTE', 'APROVADO', 'REPROVADO', 'CANCELADO', 'EM_PROCESSAMENTO', 'ESTORNADO')

# Build _dq_error column: collect all failing error rule names into a single string
df_dq = df_cast.withColumn(
    "_dq_errors",
    F.concat_ws("; ",
        F.when(cond_id_not_null,    F.lit("id_transacao:not_null")),
        F.when(cond_id_regex,       F.lit("id_transacao:regex")),
        F.when(cond_nome_not_null,  F.lit("nome_completo:not_null")),
        F.when(cond_cpf_not_null,   F.lit("cpf:not_null")),
        F.when(cond_cpf_regex,      F.lit("cpf:regex")),
        F.when(cond_email_not_null, F.lit("email:not_null")),
        F.when(cond_email_regex,    F.lit("email:regex")),
        F.when(cond_tipo_not_null,  F.lit("tipo_transacao:not_null")),
        F.when(cond_tipo_ref,       F.lit("tipo_transacao:referential")),
        F.when(cond_valor_not_null, F.lit("valor:not_null")),
        F.when(cond_valor_range,    F.lit("valor:range")),
        F.when(cond_data_not_null,  F.lit("data_transacao:not_null")),
        F.when(cond_data_range,     F.lit("data_transacao:range")),
        F.when(cond_status_not_null,F.lit("status:not_null")),
        F.when(cond_status_ref,     F.lit("status:referential"))
    )
)

# id_transacao uniqueness check – flag duplicates as error
window_id = df_dq.groupBy("id_transacao").agg(F.count("*").alias("_id_cnt"))
df_dq = df_dq.join(window_id, on="id_transacao", how="left")
df_dq = df_dq.withColumn(
    "_dq_errors",
    F.when(
        (F.col("_id_cnt") > 1) & (F.length(F.col("_dq_errors")) > 0),
        F.concat(F.col("_dq_errors"), F.lit("; id_transacao:unique"))
    ).when(
        F.col("_id_cnt") > 1,
        F.lit("id_transacao:unique")
    ).otherwise(F.col("_dq_errors"))
).drop("_id_cnt")

# ── 4. Split accepted / rejected ────────────────────────────────────────────
df_rejected = df_dq.filter(F.length(F.col("_dq_errors")) > 0) \
                   .withColumnRenamed("_dq_errors", "_dq_error")

df_accepted = df_dq.filter(F.length(F.col("_dq_errors")) == 0) \
                   .drop("_dq_errors")

rejected_count = df_rejected.count()
accepted_count = df_accepted.count()

print(f"[SILVER] Total   : {total_count}")
print(f"[SILVER] Accepted: {accepted_count}")
print(f"[SILVER] Rejected: {rejected_count}")

# ── 5. Write Rejected Table ─────────────────────────────────────────────────
df_rejected.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(REJECTED_TABLE)
print(f"[SILVER] Rejected rows written to '{REJECTED_TABLE}'")

# ── 6. Masking / Hashing / Encryption on accepted rows ──────────────────────
# nome_completo  → mask  : keep first char of each word, replace rest with '*'
# cpf            → mask  : show only last 4 chars (e.g. ***-XX)
# email          → hash  : SHA-256 hex digest
# valor          → encrypt: base64-encoded AES-like simulation using SHA-256 prefix (deterministic pseudonymisation)

# -- nome_completo: mask – show initials + asterisks per word
# Simple masking: replace all chars after first letter in each token with '***'
mask_nome_udf = F.udf(
    lambda name: " ".join(
        [w[0] + "***" if len(w) > 1 else w for w in name.split()] 
    ) if name else None,
    StringType()
)

# -- cpf: mask – show only the last segment (2 digits after dash)
mask_cpf_udf = F.udf(
    lambda cpf: "***.***.***-" + cpf.split("-")[-1] if cpf and "-" in cpf else "***MASKED***",
    StringType()
)

# -- email: hash – SHA-256
hash_email_udf = F.udf(
    lambda email: hashlib.sha256(email.encode("utf-8")).hexdigest() if email else None,
    StringType()
)

# -- valor: encrypt – deterministic pseudonymisation: prefix + SHA-256(str(valor))[:16]
encrypt_valor_udf = F.udf(
    lambda val: "ENC:" + hashlib.sha256(str(val).encode("utf-8")).hexdigest()[:32] if val is not None else None,
    StringType()
)

df_silver = df_accepted \
    .withColumn("nome_completo", mask_nome_udf(F.col("nome_completo"))) \
    .withColumn("cpf",           mask_cpf_udf(F.col("cpf"))) \
    .withColumn("email",         hash_email_udf(F.col("email"))) \
    .withColumn("valor",         encrypt_valor_udf(F.col("valor")).cast(StringType()))

# ── 7. Write Silver Table ───────────────────────────────────────────────────
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

silver_count = spark.read.table(SILVER_TABLE).count()
print(f"[SILVER] Clean rows written to '{SILVER_TABLE}'. Row count: {silver_count}")
print(f"[SILVER] Summary → Total: {total_count} | Accepted: {accepted_count} | Rejected: {rejected_count}")
