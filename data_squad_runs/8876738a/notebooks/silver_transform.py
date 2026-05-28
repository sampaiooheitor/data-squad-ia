from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE   = "prd.bronze.TBFIN_LANCAMENTOS_RH"
SILVER_TABLE   = "prd.silver.TBFIN_LANCAMENTOS_RH"
REJECTED_TABLE = "prd.silver.TBFIN_LANCAMENTOS_RH_rejected"

df = spark.read.table(BRONZE_TABLE)
total = df.count()

# Schema casting
# No casts needed — all non-metadata columns are string type

# DQ filtering
error_cond = F.col("`1WJpYXJm0-sgF1886jKz9ICJ1A9hzBlqT`").isNotNull()
df_clean    = df.filter(error_cond)
df_rejected = df.filter(~error_cond).withColumn("_dq_error", F.lit("DQ rule failed: 1WJpYXJm0-sgF1886jKz9ICJ1A9hzBlqT is null"))

# Masking
# No masking strategies defined — nothing to mask

df_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
