from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE   = "prd.bronze.TBFIN_LANCAMENTOS"
SILVER_TABLE   = "prd.silver.TBFIN_LANCAMENTOS"
REJECTED_TABLE = "prd.silver.TBFIN_LANCAMENTOS_rejected"

df = spark.read.table(BRONZE_TABLE)
total = df.count()

# Schema casting
df = df.withColumn("id_lancamento", F.col("id_lancamento").cast("integer"))
df = df.withColumn("valor", F.col("valor").cast("double"))
df = df.withColumn("dt_referencia", F.to_date(F.col("dt_referencia"), "yyyy-MM-dd"))

# DQ filtering
error_cond = (
    F.col("id_lancamento").isNotNull() &
    F.col("dt_referencia").isNotNull() &
    F.col("cpf_cliente").isNotNull()
)
df_clean    = df.filter(error_cond)
df_rejected = df.filter(~error_cond).withColumn("_dq_error", F.lit("DQ rule failed: one or more error-severity conditions violated (id_lancamento not_null, dt_referencia not_null, cpf_cliente not_null)"))

# Masking
df_clean = df_clean.withColumn("valor", F.sha2(F.col("valor").cast("string"), 512))
df_clean = df_clean.withColumn("cpf_cliente", F.regexp_replace(F.col("cpf_cliente").cast("string"), ".", "*"))

df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
