from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, DateType, StringType
import hashlib

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "bronze_transacoes_financeiras_1c97c357"
SILVER_TABLE = "silver_transacoes_financeiras_1c97c357"
REJECTED_TABLE = "silver_transacoes_financeiras_1c97c357_rejected"

# ---------------------------------------------------------------------------
# 1. READ FROM BRONZE
# ---------------------------------------------------------------------------
print(f"[Silver] Reading from Bronze table: {BRONZE_TABLE}")
df_bronze = spark.read.table(BRONZE_TABLE)
total_count = df_bronze.count()
print(f"[Silver] Total rows read from Bronze: {total_count}")

# ---------------------------------------------------------------------------
# 2. SCHEMA CASTING
# ---------------------------------------------------------------------------
print("[Silver] Applying schema casting...")
df_casted = (
    df_bronze
    .withColumn("id_transacao",   F.col("id_transacao").cast(StringType()))
    .withColumn("nome_completo",  F.col("nome_completo").cast(StringType()))
    .withColumn("cpf",            F.col("cpf").cast(StringType()))
    .withColumn("email",          F.col("email").cast(StringType()))
    .withColumn("tipo_transacao", F.col("tipo_transacao").cast(StringType()))
    .withColumn("valor",          F.col("valor").cast(FloatType()))
    .withColumn("data_transacao", F.col("data_transacao").cast(DateType()))
    .withColumn("status",         F.col("status").cast(StringType()))
)

# ---------------------------------------------------------------------------
# 3. DQ RULES — ERROR SEVERITY
#    Each failing condition is collected into an array; non-empty => rejected
# ---------------------------------------------------------------------------
print("[Silver] Evaluating DQ rules (error severity)...")

# Uniqueness for id_transacao: mark duplicates
window_id = F.count("id_transacao").over(
    __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy("id_transacao")
)
df_with_dup = df_casted.withColumn("_id_dup_count", window_id)

df_dq = df_with_dup.withColumn(
    "_dq_errors",
    F.array(
        # id_transacao not_null
        F.when(F.col("id_transacao").isNull(),
               F.lit("id_transacao: not_null violation")).otherwise(F.lit(None).cast(StringType())),
        # id_transacao unique
        F.when(F.col("_id_dup_count") > 1,
               F.lit("id_transacao: unique violation")).otherwise(F.lit(None).cast(StringType())),
        # id_transacao regex
        F.when(
            F.col("id_transacao").isNotNull() &
            (~F.col("id_transacao").rlike(r'^[a-zA-Z0-9_-]{1,64}$')),
            F.lit("id_transacao: regex violation")
        ).otherwise(F.lit(None).cast(StringType())),
        # nome_completo not_null
        F.when(F.col("nome_completo").isNull(),
               F.lit("nome_completo: not_null violation")).otherwise(F.lit(None).cast(StringType())),
        # cpf not_null
        F.when(F.col("cpf").isNull(),
               F.lit("cpf: not_null violation")).otherwise(F.lit(None).cast(StringType())),
        # cpf regex
        F.when(
            F.col("cpf").isNotNull() &
            (~F.col("cpf").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')),
            F.lit("cpf: regex violation")
        ).otherwise(F.lit(None).cast(StringType())),
        # email not_null
        F.when(F.col("email").isNull(),
               F.lit("email: not_null violation")).otherwise(F.lit(None).cast(StringType())),
        # email regex
        F.when(
            F.col("email").isNotNull() &
            (~F.col("email").rlike(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$')),
            F.lit("email: regex violation")
        ).otherwise(F.lit(None).cast(StringType())),
        # tipo_transacao not_null
        F.when(F.col("tipo_transacao").isNull(),
               F.lit("tipo_transacao: not_null violation")).otherwise(F.lit(None).cast(StringType())),
        # tipo_transacao referential
        F.when(
            F.col("tipo_transacao").isNotNull() &
            (~F.col("tipo_transacao").isin('CREDITO','DEBITO','TRANSFERENCIA','PIX','BOLETO','ESTORNO')),
            F.lit("tipo_transacao: referential violation")
        ).otherwise(F.lit(None).cast(StringType())),
        # valor not_null
        F.when(F.col("valor").isNull(),
               F.lit("valor: not_null violation")).otherwise(F.lit(None).cast(StringType())),
        # valor range (error)
        F.when(
            F.col("valor").isNotNull() &
            ((F.col("valor") < 0.01) | (F.col("valor") > 1000000.00)),
            F.lit("valor: range violation (0.01 - 1000000.00)")
        ).otherwise(F.lit(None).cast(StringType())),
        # data_transacao not_null
        F.when(F.col("data_transacao").isNull(),
               F.lit("data_transacao: not_null violation")).otherwise(F.lit(None).cast(StringType())),
        # data_transacao range
        F.when(
            F.col("data_transacao").isNotNull() &
            ((F.col("data_transacao") < F.lit("2000-01-01").cast(DateType())) |
             (F.col("data_transacao") > F.current_date())),
            F.lit("data_transacao: range violation")
        ).otherwise(F.lit(None).cast(StringType())),
        # status not_null
        F.when(F.col("status").isNull(),
               F.lit("status: not_null violation")).otherwise(F.lit(None).cast(StringType())),
        # status referential
        F.when(
            F.col("status").isNotNull() &
            (~F.col("status").isin('PENDENTE','APROVADA','RECUSADA','CANCELADA','ESTORNADA','EM_PROCESSAMENTO')),
            F.lit("status: referential violation")
        ).otherwise(F.lit(None).cast(StringType()))
    )
).withColumn(
    "_dq_errors",
    F.array_compact(F.col("_dq_errors"))  # remove nulls
).drop("_id_dup_count")

# Identify failed rows
df_failed = df_dq.filter(F.size(F.col("_dq_errors")) > 0)
df_passed = df_dq.filter(F.size(F.col("_dq_errors")) == 0).drop("_dq_errors")

# ---------------------------------------------------------------------------
# 4. WRITE REJECTED ROWS
# ---------------------------------------------------------------------------
df_rejected = df_failed.withColumn(
    "_dq_error",
    F.concat_ws(" | ", F.col("_dq_errors"))
).drop("_dq_errors")

rejected_count = df_rejected.count()
print(f"[Silver] Rejected rows: {rejected_count}")

if rejected_count > 0:
    print(f"[Silver] Writing rejected rows to: {REJECTED_TABLE}")
    df_rejected.write.format("delta").mode("overwrite").saveAsTable(REJECTED_TABLE)
    print(f"[Silver] Rejected table write complete.")
else:
    print("[Silver] No rejected rows to write.")

# ---------------------------------------------------------------------------
# 5. MASKING / HASHING / ENCRYPTION on clean rows
# ---------------------------------------------------------------------------
print("[Silver] Applying data governance masking...")

# Helper UDFs

# mask strategy: keep first char, replace rest with '*'
def mask_string(s):
    if s is None:
        return None
    parts = s.split(" ")
    masked = [p[0] + "*" * (len(p) - 1) if len(p) > 1 else "*" for p in parts]
    return " ".join(masked)

mask_udf = F.udf(mask_string, StringType())

# hash strategy: SHA-256 hex digest
def hash_string(s):
    if s is None:
        return None
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

hash_udf = F.udf(hash_string, StringType())

# encrypt strategy: simple reversible base64 encoding (representative placeholder)
import base64

def encrypt_value(v):
    if v is None:
        return None
    return base64.b64encode(str(v).encode("utf-8")).decode("utf-8")

encrypt_udf = F.udf(encrypt_value, StringType())

# Apply masking:
#   nome_completo -> mask
#   cpf           -> mask
#   email         -> hash
#   valor         -> encrypt (store as string in silver)
df_masked = (
    df_passed
    .withColumn("nome_completo", mask_udf(F.col("nome_completo")))
    .withColumn("cpf",           mask_udf(F.col("cpf")))
    .withColumn("email",         hash_udf(F.col("email")))
    .withColumn("valor",         encrypt_udf(F.col("valor")).cast(StringType()))
)

# ---------------------------------------------------------------------------
# 6. WRITE SILVER TABLE
# ---------------------------------------------------------------------------
accepted_count = df_masked.count()
print(f"[Silver] Accepted (clean) rows: {accepted_count}")
print(f"[Silver] Writing clean rows to Silver table: {SILVER_TABLE}")

df_masked.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)

print(f"[Silver] Write complete.")
print(f"[Silver] ---- Summary ----")
print(f"[Silver]   Total rows from Bronze : {total_count}")
print(f"[Silver]   Accepted (Silver)      : {accepted_count}")
print(f"[Silver]   Rejected               : {rejected_count}")
