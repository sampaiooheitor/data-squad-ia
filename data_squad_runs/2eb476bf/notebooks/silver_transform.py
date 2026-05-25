from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import hashlib

spark = SparkSession.builder.getOrCreate()

# ─────────────────────────────────────────────
# 1. READ FROM BRONZE
# ─────────────────────────────────────────────
bronze_table = "bronze_ecommerce_orders_2eb476bf"
silver_table = "silver_ecommerce_orders_2eb476bf"
rejected_table = "silver_ecommerce_orders_2eb476bf_rejected"

df_bronze = spark.read.table(bronze_table)
total_count = df_bronze.count()
print(f"Total rows read from bronze: {total_count}")

# ─────────────────────────────────────────────
# 2. SCHEMA CASTING
# ─────────────────────────────────────────────
df_casted = (
    df_bronze
    .withColumn("order_id",      F.col("order_id").cast("string"))
    .withColumn("nome_completo", F.col("nome_completo").cast("string"))
    .withColumn("email",         F.col("email").cast("string"))
    .withColumn("cpf",           F.col("cpf").cast("string"))
    .withColumn("produto",       F.col("produto").cast("string"))
    .withColumn("categoria",     F.col("categoria").cast("string"))
    .withColumn("valor_total",   F.col("valor_total").cast("float"))
    .withColumn("status_pedido", F.col("status_pedido").cast("string"))
)

# ─────────────────────────────────────────────
# 3. DATA QUALITY — ERROR RULES
# Each failing row is tagged with a _dq_error message.
# Rows that fail ANY error rule go to the rejected table.
# ─────────────────────────────────────────────

# --- 3a. Row-level error conditions ---
cond_order_id_not_null   = F.col("order_id").isNull()
cond_order_id_regex      = ~F.col("order_id").rlike(r'^[A-Za-z0-9_-]+$')
cond_nome_not_null       = F.col("nome_completo").isNull()
cond_email_not_null      = F.col("email").isNull()
cond_email_regex         = ~F.col("email").rlike(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$')
cond_cpf_not_null        = F.col("cpf").isNull()
cond_cpf_regex           = ~F.col("cpf").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')
cond_produto_not_null    = F.col("produto").isNull()
cond_categoria_not_null  = F.col("categoria").isNull()
cond_valor_not_null      = F.col("valor_total").isNull()
cond_valor_range         = ~F.col("valor_total").between(0.01, 999999.99)
cond_status_not_null     = F.col("status_pedido").isNull()
cond_status_ref          = ~F.col("status_pedido").isin(
                               'pendente', 'confirmado', 'em_transporte',
                               'entregue', 'cancelado', 'devolvido'
                           )

# --- 3b. Uniqueness errors (dataset-level) ---
# order_id must be unique (error)
total_rows   = df_casted.count()
distinct_oid = df_casted.select("order_id").distinct().count()
order_id_is_unique = (distinct_oid == total_rows)

# Mark duplicated order_ids
window_oid = Window.partitionBy("order_id")
df_flagged = df_casted.withColumn("_oid_cnt", F.count("order_id").over(window_oid))
cond_order_id_unique = F.col("_oid_cnt") > 1

# --- 3c. Build _dq_error column ---
df_flagged = (
    df_flagged
    .withColumn(
        "_dq_error",
        F.concat_ws("; ",
            F.when(cond_order_id_not_null,  F.lit("[error] order_id: not_null")),
            F.when(cond_order_id_unique,    F.lit("[error] order_id: unique")),
            F.when(
                F.col("order_id").isNotNull() & cond_order_id_regex,
                F.lit("[error] order_id: regex")
            ),
            F.when(cond_nome_not_null,      F.lit("[error] nome_completo: not_null")),
            F.when(cond_email_not_null,     F.lit("[error] email: not_null")),
            F.when(
                F.col("email").isNotNull() & cond_email_regex,
                F.lit("[error] email: regex")
            ),
            F.when(cond_cpf_not_null,       F.lit("[error] cpf: not_null")),
            F.when(
                F.col("cpf").isNotNull() & cond_cpf_regex,
                F.lit("[error] cpf: regex")
            ),
            F.when(cond_produto_not_null,   F.lit("[error] produto: not_null")),
            F.when(cond_categoria_not_null, F.lit("[error] categoria: not_null")),
            F.when(cond_valor_not_null,     F.lit("[error] valor_total: not_null")),
            F.when(
                F.col("valor_total").isNotNull() & cond_valor_range,
                F.lit("[error] valor_total: range")
            ),
            F.when(cond_status_not_null,    F.lit("[error] status_pedido: not_null")),
            F.when(
                F.col("status_pedido").isNotNull() & cond_status_ref,
                F.lit("[error] status_pedido: referential")
            )
        )
    )
)

# Split into rejected (error) and accepted
df_rejected = df_flagged.filter(
    (F.col("_dq_error").isNotNull()) & (F.col("_dq_error") != "")
).drop("_oid_cnt")

df_accepted = df_flagged.filter(
    (F.col("_dq_error").isNull()) | (F.col("_dq_error") == "")
).drop("_oid_cnt", "_dq_error")

rejected_count = df_rejected.count()
accepted_count = df_accepted.count()

print(f"Total rows   : {total_count}")
print(f"Accepted rows: {accepted_count}")
print(f"Rejected rows: {rejected_count}")

# ─────────────────────────────────────────────
# 4. MASKING / ENCRYPTION
# nome_completo : mask  → show first char + '***'
# email         : mask  → mask local part, keep domain
# cpf           : mask  → show last 2 digits, mask rest
# valor_total   : encrypt → SHA-256 hex (stored as string)
# ─────────────────────────────────────────────

# UDFs
@F.udf("string")
def mask_nome(nome):
    if nome is None:
        return None
    parts = nome.strip().split()
    masked = [p[0] + "***" if len(p) > 0 else "***" for p in parts]
    return " ".join(masked)

@F.udf("string")
def mask_email(email):
    if email is None:
        return None
    if "@" not in email:
        return "***"
    local, domain = email.split("@", 1)
    masked_local = local[0] + "***" if len(local) > 1 else "***"
    return masked_local + "@" + domain

@F.udf("string")
def mask_cpf(cpf):
    if cpf is None:
        return None
    # Keep last 2 digits of the numeric part, mask the rest
    # Format: XXX.XXX.XXX-XX  → ***.***.***-XX
    parts = cpf.split("-")
    if len(parts) == 2:
        return "***.***.***-" + parts[1]
    return "***.***.***-**"

@F.udf("string")
def encrypt_valor(valor):
    if valor is None:
        return None
    return hashlib.sha256(str(valor).encode("utf-8")).hexdigest()

df_masked = (
    df_accepted
    .withColumn("nome_completo", mask_nome(F.col("nome_completo")))
    .withColumn("email",         mask_email(F.col("email")))
    .withColumn("cpf",           mask_cpf(F.col("cpf")))
    .withColumn("valor_total",   encrypt_valor(F.col("valor_total")).cast("string"))
)

# ─────────────────────────────────────────────
# 5. WRITE SILVER & REJECTED TABLES
# ─────────────────────────────────────────────

# Silver clean table
df_masked.write.format("delta").mode("overwrite").saveAsTable(silver_table)
print(f"Silver table '{silver_table}' written with {accepted_count} rows.")

# Rejected table
if rejected_count > 0:
    df_rejected.write.format("delta").mode("overwrite").saveAsTable(rejected_table)
    print(f"Rejected table '{rejected_table}' written with {rejected_count} rows.")
else:
    print("No rejected rows — skipping rejected table write.")

# ─────────────────────────────────────────────
# 6. FINAL SUMMARY
# ─────────────────────────────────────────────
silver_count   = spark.read.table(silver_table).count()
print("\n===== Silver Pipeline Summary =====")
print(f"  Bronze rows read : {total_count}")
print(f"  Accepted (silver): {silver_count}")
print(f"  Rejected         : {rejected_count}")
print("===================================")
