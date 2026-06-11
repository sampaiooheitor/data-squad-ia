from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, FloatType, DateType, BooleanType
import hashlib

spark = SparkSession.builder.getOrCreate()

# ─────────────────────────────────────────────
# 1. READ FROM BRONZE
# ─────────────────────────────────────────────
bronze_table = "prd.bronze.TBFIN_TRANSACOES"
silver_table = "prd.silver.TBFIN_TRANSACOES"
rejected_table = "prd.silver.TBFIN_TRANSACOES_rejected"

df_bronze = spark.read.table(bronze_table)
print("[SILVER] Bronze table loaded.")
df_bronze.printSchema()

total_count = df_bronze.count()
print(f"[SILVER] Total rows read from bronze: {total_count}")

# ─────────────────────────────────────────────
# 2. SCHEMA CASTING
# ─────────────────────────────────────────────
df_casted = (
    df_bronze
    .withColumn("ID_TRANSACAO",      F.col("ID_TRANSACAO").cast(IntegerType()))
    .withColumn("NM_CLIENTE",        F.col("NM_CLIENTE").cast(StringType()))
    .withColumn("CD_CPF",            F.col("CD_CPF").cast(StringType()))
    .withColumn("CD_TIPO_TRANSACAO", F.col("CD_TIPO_TRANSACAO").cast(StringType()))
    .withColumn("VL_TRANSACAO",      F.col("VL_TRANSACAO").cast(FloatType()))
    .withColumn("DT_TRANSACAO",      F.col("DT_TRANSACAO").cast(DateType()))
    .withColumn("FL_ATIVO",          F.col("FL_ATIVO").cast(BooleanType()))
    .withColumn("CD_AGENCIA",        F.col("CD_AGENCIA").cast(StringType()))
)

print("[SILVER] Schema casting applied.")
df_casted.printSchema()

# ─────────────────────────────────────────────
# 3. DATA QUALITY — ERROR RULES
#    (warning rules are logged but do NOT reject rows)
# ─────────────────────────────────────────────

# ── 3a. Uniqueness pre-check for ID_TRANSACAO (error) ──
# We flag duplicates so each individual row can be marked
window_id = F.count("ID_TRANSACAO").over(
    __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy("ID_TRANSACAO")
)
from pyspark.sql.window import Window

df_with_id_dup = df_casted.withColumn(
    "_id_dup_count",
    F.count("ID_TRANSACAO").over(Window.partitionBy("ID_TRANSACAO"))
)

# ── 3b. Build individual error flag columns ──
df_dq = (
    df_with_id_dup
    # ID_TRANSACAO: not_null (error)
    .withColumn("_err_id_not_null",
        F.col("ID_TRANSACAO").isNull())
    # ID_TRANSACAO: unique (error) — row is duplicate if count > 1
    .withColumn("_err_id_unique",
        F.col("_id_dup_count") > 1)
    # NM_CLIENTE: not_null (error)
    .withColumn("_err_nm_not_null",
        F.col("NM_CLIENTE").isNull())
    # NM_CLIENTE: regex (warning — not an error, skip for rejection)
    # CD_CPF: not_null (error)
    .withColumn("_err_cpf_not_null",
        F.col("CD_CPF").isNull())
    # CD_CPF: regex (error)
    .withColumn("_err_cpf_regex",
        ~F.col("CD_CPF").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$') |
        F.col("CD_CPF").isNull())
    # CD_CPF: unique (warning — skip for rejection)
    # CD_TIPO_TRANSACAO: not_null (error)
    .withColumn("_err_tipo_not_null",
        F.col("CD_TIPO_TRANSACAO").isNull())
    # CD_TIPO_TRANSACAO: referential (error)
    .withColumn("_err_tipo_ref",
        ~F.col("CD_TIPO_TRANSACAO").isin('CREDITO','DEBITO','PIX','TED','DOC','BOLETO') |
        F.col("CD_TIPO_TRANSACAO").isNull())
    # VL_TRANSACAO: not_null (error)
    .withColumn("_err_vl_not_null",
        F.col("VL_TRANSACAO").isNull())
    # VL_TRANSACAO: range error (0.01 to 1000000.00)
    .withColumn("_err_vl_range",
        (F.col("VL_TRANSACAO") < 0.01) | (F.col("VL_TRANSACAO") > 1000000.00) |
        F.col("VL_TRANSACAO").isNull())
    # VL_TRANSACAO: range warning (0.01 to 50000.00) — skip for rejection
    # DT_TRANSACAO: not_null (error)
    .withColumn("_err_dt_not_null",
        F.col("DT_TRANSACAO").isNull())
    # DT_TRANSACAO: range (error)
    .withColumn("_err_dt_range",
        (F.col("DT_TRANSACAO") < F.lit("2000-01-01").cast(DateType())) |
        (F.col("DT_TRANSACAO") > F.current_date()) |
        F.col("DT_TRANSACAO").isNull())
    # DT_TRANSACAO: regex (error) — after casting to DateType the format is guaranteed;
    #   we validate on the string representation to be safe
    .withColumn("_err_dt_regex",
        ~F.date_format(F.col("DT_TRANSACAO"), "yyyy-MM-dd").rlike(
            r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$'
        ) | F.col("DT_TRANSACAO").isNull())
    # FL_ATIVO: not_null (error)
    .withColumn("_err_fl_not_null",
        F.col("FL_ATIVO").isNull())
    # FL_ATIVO: referential (error) — BooleanType only accepts true/false so any non-null is valid
    .withColumn("_err_fl_ref",
        F.col("FL_ATIVO").isNull())  # after cast, only null means invalid
    # CD_AGENCIA: not_null (error)
    .withColumn("_err_ag_not_null",
        F.col("CD_AGENCIA").isNull())
    # CD_AGENCIA: regex (error)  — pattern: 4 digits, optional hyphen, 1 digit
    .withColumn("_err_ag_regex",
        ~F.col("CD_AGENCIA").rlike(r'^\d{4}-?\d{1}$') |
        F.col("CD_AGENCIA").isNull())
)

# ── 3c. Aggregate all error flags into a single boolean and a description string ──
error_flag_cols = [
    ("_err_id_not_null",   "ID_TRANSACAO: not_null violation"),
    ("_err_id_unique",     "ID_TRANSACAO: unique violation"),
    ("_err_nm_not_null",   "NM_CLIENTE: not_null violation"),
    ("_err_cpf_not_null",  "CD_CPF: not_null violation"),
    ("_err_cpf_regex",     "CD_CPF: regex violation"),
    ("_err_tipo_not_null", "CD_TIPO_TRANSACAO: not_null violation"),
    ("_err_tipo_ref",      "CD_TIPO_TRANSACAO: referential violation"),
    ("_err_vl_not_null",   "VL_TRANSACAO: not_null violation"),
    ("_err_vl_range",      "VL_TRANSACAO: range violation"),
    ("_err_dt_not_null",   "DT_TRANSACAO: not_null violation"),
    ("_err_dt_range",      "DT_TRANSACAO: range violation"),
    ("_err_dt_regex",      "DT_TRANSACAO: regex violation"),
    ("_err_fl_not_null",   "FL_ATIVO: not_null violation"),
    ("_err_fl_ref",        "FL_ATIVO: referential violation"),
    ("_err_ag_not_null",   "CD_AGENCIA: not_null violation"),
    ("_err_ag_regex",      "CD_AGENCIA: regex violation"),
]

# Build _dq_error column as a concatenated string of all active violations
error_messages_expr = F.concat_ws("; ", *[
    F.when(F.col(flag_col), F.lit(msg))
    for flag_col, msg in error_flag_cols
])

df_dq = df_dq.withColumn("_dq_error", error_messages_expr)

# A row fails if ANY error flag is True
any_error_expr = F.lit(False)
for flag_col, _ in error_flag_cols:
    any_error_expr = any_error_expr | F.col(flag_col)

df_dq = df_dq.withColumn("_has_error", any_error_expr)

# ── 3d. Warning rules — log only, no rejection ──
df_warnings = df_dq.filter(
    ~F.col("_err_nm_not_null") &  # only check regex if not already null
    ~F.col("NM_CLIENTE").rlike(r'^[A-Za-z\u00C0-\u00FF ]{2,100}$')
)
warn_nm_count = df_warnings.count()
print(f"[WARNING] NM_CLIENTE regex violations: {warn_nm_count}")

df_warn_cpf = df_dq.filter(
    F.col("CD_CPF").isNotNull() &
    (F.count("CD_CPF").over(Window.partitionBy("CD_CPF")) > 1)
)
# We count using groupBy to avoid re-computing window
df_warn_cpf_count = (
    df_dq.groupBy("CD_CPF").count()
    .filter(F.col("count") > 1)
    .count()
)
print(f"[WARNING] CD_CPF unique violations (distinct CPFs with duplicates): {df_warn_cpf_count}")

df_warn_vl = df_dq.filter(
    F.col("VL_TRANSACAO").isNotNull() &
    ((F.col("VL_TRANSACAO") < 0.01) | (F.col("VL_TRANSACAO") > 50000.00))
)
warn_vl_count = df_warn_vl.count()
print(f"[WARNING] VL_TRANSACAO range (0.01-50000.00) violations: {warn_vl_count}")

# ─────────────────────────────────────────────
# 4. SPLIT INTO ACCEPTED / REJECTED
# ─────────────────────────────────────────────
business_cols = [
    "ID_TRANSACAO", "NM_CLIENTE", "CD_CPF", "CD_TIPO_TRANSACAO",
    "VL_TRANSACAO", "DT_TRANSACAO", "FL_ATIVO", "CD_AGENCIA"
]

df_rejected = df_dq.filter(F.col("_has_error")).select(
    *business_cols, "_dq_error"
)

df_accepted = df_dq.filter(~F.col("_has_error")).select(*business_cols)

rejected_count = df_rejected.count()
accepted_count = df_accepted.count()

print(f"[SILVER] Total rows    : {total_count}")
print(f"[SILVER] Accepted rows : {accepted_count}")
print(f"[SILVER] Rejected rows : {rejected_count}")

# ─────────────────────────────────────────────
# 5. MASKING / ENCRYPTION
#    NM_CLIENTE  → mask  (keep first char + asterisks per word)
#    CD_CPF      → mask  (show only last 2 digits of numeric part)
#    VL_TRANSACAO → encrypt (SHA-256 hex digest stored as string)
# ─────────────────────────────────────────────

# ── 5a. Masking UDFs ──

@F.udf(StringType())
def mask_name(name):
    """Mask each word keeping only the first letter: 'Mariana Souza' -> 'M******* S****'"""
    if name is None:
        return None
    parts = name.split()
    masked = [p[0] + '*' * (len(p) - 1) if len(p) > 1 else p for p in parts]
    return ' '.join(masked)

@F.udf(StringType())
def mask_cpf(cpf):
    """Mask CPF keeping only last segment visible: '312.456.789-01' -> '***.***.**-01'"""
    if cpf is None:
        return None
    # Expected format: ddd.ddd.ddd-dd
    parts = cpf.split('-')
    if len(parts) == 2:
        suffix = parts[1]
        return '***.***.***-' + suffix
    return '***.***.***-**'

@F.udf(StringType())
def encrypt_float(value):
    """Pseudo-encrypt a float value using SHA-256 hex digest."""
    if value is None:
        return None
    raw = str(value).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()

# ── 5b. Apply masking to accepted rows ──
df_masked = (
    df_accepted
    .withColumn("NM_CLIENTE",  mask_name(F.col("NM_CLIENTE")))
    .withColumn("CD_CPF",      mask_cpf(F.col("CD_CPF")))
    .withColumn("VL_TRANSACAO", encrypt_float(F.col("VL_TRANSACAO").cast(StringType())))
)

print("[SILVER] Masking/encryption applied to accepted rows.")

# ─────────────────────────────────────────────
# 6. WRITE SILVER & REJECTED TABLES
# ─────────────────────────────────────────────

# Write clean (masked) rows to Silver table
df_masked.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
print(f"[SILVER] Clean data written to '{silver_table}'. Row count: {accepted_count}")

# Write rejected rows to rejected table
df_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(rejected_table)
print(f"[SILVER] Rejected data written to '{rejected_table}'. Row count: {rejected_count}")

# ─────────────────────────────────────────────
# 7. FINAL SUMMARY LOG
# ─────────────────────────────────────────────
print("=" * 50)
print("[SILVER] Pipeline Summary")
print(f"  Bronze table   : {bronze_table}")
print(f"  Silver table   : {silver_table}")
print(f"  Rejected table : {rejected_table}")
print(f"  Total rows     : {total_count}")
print(f"  Accepted rows  : {accepted_count}")
print(f"  Rejected rows  : {rejected_count}")
print("=" * 50)
