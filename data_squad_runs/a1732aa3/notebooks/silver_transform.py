from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DateType, FloatType, BooleanType
import hashlib

spark = SparkSession.builder.getOrCreate()

bronze_table = "bronze_TBLOG_ENTREGAS_a1732aa3"
silver_table = "silver_TBLOG_ENTREGAS_a1732aa3"
rejected_table = "silver_TBLOG_ENTREGAS_a1732aa3_rejected"

print(f"[SILVER] Reading from Bronze table: {bronze_table}")
df = spark.read.table(bronze_table)
total_count = df.count()
print(f"[SILVER] Total rows read from bronze: {total_count}")

# ── Schema Casting ──────────────────────────────────────────────────────────
print("[SILVER] Applying schema casting...")
df = (
    df
    .withColumn("ID_ENTREGA",           F.col("ID_ENTREGA").cast(IntegerType()))
    .withColumn("NM_DESTINATARIO",      F.col("NM_DESTINATARIO").cast(StringType()))
    .withColumn("CD_CPF_DESTINATARIO",  F.col("CD_CPF_DESTINATARIO").cast(StringType()))
    .withColumn("DT_POSTAGEM",          F.col("DT_POSTAGEM").cast(DateType()))
    .withColumn("DT_ENTREGA_PREVISTA",  F.col("DT_ENTREGA_PREVISTA").cast(DateType()))
    .withColumn("VL_FRETE",             F.col("VL_FRETE").cast(FloatType()))
    .withColumn("CD_STATUS_ENTREGA",    F.col("CD_STATUS_ENTREGA").cast(StringType()))
    .withColumn("FL_ENTREGA_REALIZADA", F.col("FL_ENTREGA_REALIZADA").cast(BooleanType()))
)
print("[SILVER] Schema casting applied.")

# ── DQ Rules (ERROR severity only → rejected) ───────────────────────────────
print("[SILVER] Evaluating DQ rules (error severity)...")

# Collect DISTINCT ID_ENTREGA counts to evaluate uniqueness
id_counts = df.groupBy("ID_ENTREGA").agg(F.count("*").alias("_id_cnt"))
df = df.join(id_counts, on="ID_ENTREGA", how="left")

valid_statuses = ['PENDENTE', 'EM_TRANSITO', 'SAIU_PARA_ENTREGA', 'ENTREGUE', 'DEVOLVIDO', 'EXTRAVIADO', 'CANCELADO']

# Build individual error flag columns
df = (
    df
    # ID_ENTREGA rules
    .withColumn("_err_id_not_null",
        F.col("ID_ENTREGA").isNull())
    .withColumn("_err_id_unique",
        F.col("_id_cnt") > 1)
    .withColumn("_err_id_range",
        F.col("ID_ENTREGA").isNull() | ~F.col("ID_ENTREGA").between(1, 9999999999))

    # NM_DESTINATARIO rules
    .withColumn("_err_nm_not_null",
        F.col("NM_DESTINATARIO").isNull())

    # CD_CPF_DESTINATARIO rules
    .withColumn("_err_cpf_not_null",
        F.col("CD_CPF_DESTINATARIO").isNull())
    .withColumn("_err_cpf_regex",
        F.col("CD_CPF_DESTINATARIO").isNull() |
        ~F.col("CD_CPF_DESTINATARIO").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$'))

    # DT_POSTAGEM rules
    .withColumn("_err_dtpost_not_null",
        F.col("DT_POSTAGEM").isNull())
    .withColumn("_err_dtpost_range",
        F.col("DT_POSTAGEM").isNull() |
        ~F.col("DT_POSTAGEM").between(F.lit("2000-01-01").cast(DateType()), F.current_date()))

    # DT_ENTREGA_PREVISTA rules
    .withColumn("_err_dtprev_not_null",
        F.col("DT_ENTREGA_PREVISTA").isNull())
    .withColumn("_err_dtprev_regex",
        F.col("DT_ENTREGA_PREVISTA").isNull() |
        ~F.col("DT_ENTREGA_PREVISTA").cast(StringType()).rlike(r'^\d{4}-\d{2}-\d{2}$'))

    # VL_FRETE rules
    .withColumn("_err_frete_not_null",
        F.col("VL_FRETE").isNull())
    .withColumn("_err_frete_range",
        F.col("VL_FRETE").isNull() | ~F.col("VL_FRETE").between(0.01, 99999.99))

    # CD_STATUS_ENTREGA rules
    .withColumn("_err_status_not_null",
        F.col("CD_STATUS_ENTREGA").isNull())
    .withColumn("_err_status_ref",
        F.col("CD_STATUS_ENTREGA").isNull() |
        ~F.col("CD_STATUS_ENTREGA").isin(valid_statuses))

    # FL_ENTREGA_REALIZADA rules
    .withColumn("_err_fl_not_null",
        F.col("FL_ENTREGA_REALIZADA").isNull())
    .withColumn("_err_fl_ref",
        F.col("FL_ENTREGA_REALIZADA").isNull())
)

# Build combined error message
df = df.withColumn(
    "_dq_error",
    F.concat_ws("; ",
        F.when(F.col("_err_id_not_null"),   F.lit("[error] ID_ENTREGA: not_null")).otherwise(F.lit(None)),
        F.when(F.col("_err_id_unique"),     F.lit("[error] ID_ENTREGA: unique")).otherwise(F.lit(None)),
        F.when(F.col("_err_id_range"),      F.lit("[error] ID_ENTREGA: range")).otherwise(F.lit(None)),
        F.when(F.col("_err_nm_not_null"),   F.lit("[error] NM_DESTINATARIO: not_null")).otherwise(F.lit(None)),
        F.when(F.col("_err_cpf_not_null"),  F.lit("[error] CD_CPF_DESTINATARIO: not_null")).otherwise(F.lit(None)),
        F.when(F.col("_err_cpf_regex"),     F.lit("[error] CD_CPF_DESTINATARIO: regex")).otherwise(F.lit(None)),
        F.when(F.col("_err_dtpost_not_null"),F.lit("[error] DT_POSTAGEM: not_null")).otherwise(F.lit(None)),
        F.when(F.col("_err_dtpost_range"),  F.lit("[error] DT_POSTAGEM: range")).otherwise(F.lit(None)),
        F.when(F.col("_err_dtprev_not_null"),F.lit("[error] DT_ENTREGA_PREVISTA: not_null")).otherwise(F.lit(None)),
        F.when(F.col("_err_dtprev_regex"),  F.lit("[error] DT_ENTREGA_PREVISTA: regex")).otherwise(F.lit(None)),
        F.when(F.col("_err_frete_not_null"),F.lit("[error] VL_FRETE: not_null")).otherwise(F.lit(None)),
        F.when(F.col("_err_frete_range"),   F.lit("[error] VL_FRETE: range")).otherwise(F.lit(None)),
        F.when(F.col("_err_status_not_null"),F.lit("[error] CD_STATUS_ENTREGA: not_null")).otherwise(F.lit(None)),
        F.when(F.col("_err_status_ref"),    F.lit("[error] CD_STATUS_ENTREGA: referential")).otherwise(F.lit(None)),
        F.when(F.col("_err_fl_not_null"),   F.lit("[error] FL_ENTREGA_REALIZADA: not_null")).otherwise(F.lit(None)),
        F.when(F.col("_err_fl_ref"),        F.lit("[error] FL_ENTREGA_REALIZADA: referential")).otherwise(F.lit(None))
    )
)

# Flag rows that have at least one error
df = df.withColumn("_has_error", F.length(F.col("_dq_error")) > 0)

# Internal column names used for DQ evaluation
dq_internal_cols = [
    "_id_cnt",
    "_err_id_not_null", "_err_id_unique", "_err_id_range",
    "_err_nm_not_null",
    "_err_cpf_not_null", "_err_cpf_regex",
    "_err_dtpost_not_null", "_err_dtpost_range",
    "_err_dtprev_not_null", "_err_dtprev_regex",
    "_err_frete_not_null", "_err_frete_range",
    "_err_status_not_null", "_err_status_ref",
    "_err_fl_not_null", "_err_fl_ref",
    "_has_error"
]

business_cols = [
    "ID_ENTREGA", "NM_DESTINATARIO", "CD_CPF_DESTINATARIO",
    "DT_POSTAGEM", "DT_ENTREGA_PREVISTA", "VL_FRETE",
    "CD_STATUS_ENTREGA", "FL_ENTREGA_REALIZADA"
]

# ── Split accepted vs rejected ───────────────────────────────────────────────
df_rejected = (
    df.filter(F.col("_has_error"))
      .select(business_cols + ["_dq_error"])
)

df_accepted = (
    df.filter(~F.col("_has_error"))
      .select(business_cols)
)

print(f"[SILVER] Rows accepted (pre-masking): {df_accepted.count()}")
print(f"[SILVER] Rows rejected: {df_rejected.count()}")

# ── Masking ──────────────────────────────────────────────────────────────────
print("[SILVER] Applying masking strategies...")

# NM_DESTINATARIO → masking=mask  → show first char + asterisks per word
mask_name_udf = F.udf(
    lambda name: " ".join(
        (w[0] + "*" * (len(w) - 1)) if len(w) > 1 else w
        for w in (name or "").split()
    ) if name else None,
    StringType()
)

# CD_CPF_DESTINATARIO → masking=mask → keep format, mask middle digits
mask_cpf_udf = F.udf(
    lambda cpf: (
        cpf[:4] + "***" + cpf[7:11] + "-**"
        if cpf and len(cpf) == 14 else ("***masked***" if cpf else None)
    ),
    StringType()
)

# VL_FRETE → masking=encrypt → deterministic SHA-256 hex (stored as string)
encrypt_float_udf = F.udf(
    lambda v: hashlib.sha256(str(v).encode()).hexdigest() if v is not None else None,
    StringType()
)

df_accepted = (
    df_accepted
    .withColumn("NM_DESTINATARIO",     mask_name_udf(F.col("NM_DESTINATARIO")))
    .withColumn("CD_CPF_DESTINATARIO", mask_cpf_udf(F.col("CD_CPF_DESTINATARIO")))
    .withColumn("VL_FRETE",            encrypt_float_udf(F.col("VL_FRETE")).cast(StringType()))
)

print("[SILVER] Masking applied.")

# ── Write Rejected ───────────────────────────────────────────────────────────
print(f"[SILVER] Writing rejected rows to: {rejected_table}")
df_rejected.write.format("delta").mode("overwrite").saveAsTable(rejected_table)
rejected_count = spark.read.table(rejected_table).count()
print(f"[SILVER] Rejected table row count: {rejected_count}")

# ── Write Silver ─────────────────────────────────────────────────────────────
print(f"[SILVER] Writing clean rows to Silver table: {silver_table}")
df_accepted.write.format("delta").mode("overwrite").saveAsTable(silver_table)
accepted_count = spark.read.table(silver_table).count()
print(f"[SILVER] Silver table row count: {accepted_count}")

# ── Summary ──────────────────────────────────────────────────────────────────
print("\n========== SILVER LAYER SUMMARY ==========")
print(f"  Total rows from Bronze : {total_count}")
print(f"  Accepted rows (Silver) : {accepted_count}")
print(f"  Rejected rows          : {rejected_count}")
print("===========================================")
