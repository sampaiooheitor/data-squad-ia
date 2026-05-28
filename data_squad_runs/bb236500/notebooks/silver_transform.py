from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE   = "prd.bronze.TBFIN_LANCAMENTOS_RH"
SILVER_TABLE   = "prd.silver.TBFIN_LANCAMENTOS_RH"
REJECTED_TABLE = "prd.silver.TBFIN_LANCAMENTOS_RH_rejected"

df = spark.read.table(BRONZE_TABLE)
total = df.count()

# Schema casting
df = df.withColumn("ID_LANCAMENTO", F.col("ID_LANCAMENTO").cast("integer"))
df = df.withColumn("DT_LANCAMENTO", F.to_date(F.col("DT_LANCAMENTO"), "yyyy-MM-dd"))
df = df.withColumn("VL_LANCAMENTO", F.col("VL_LANCAMENTO").cast("double"))

# DQ filtering
error_cond = (
    F.col("ID_LANCAMENTO").isNotNull() &
    F.col("DT_LANCAMENTO").isNotNull() &
    F.col("DT_LANCAMENTO").cast("string").rlike(r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$') &
    F.col("VL_LANCAMENTO").isNotNull() &
    F.col("NM_FORNECEDOR").isNotNull() &
    F.col("NM_FORNECEDOR").rlike(r'^\s*(\S+.*\S+|\S+)\s*$')
)
df_clean    = df.filter(error_cond)
df_rejected = df.filter(~error_cond).withColumn("_dq_error", F.lit("DQ rule failed"))

# Masking
df_clean = df_clean.withColumn("VL_LANCAMENTO", F.sha2(F.col("VL_LANCAMENTO").cast("string"), 512))

df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
