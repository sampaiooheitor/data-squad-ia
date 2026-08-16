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
from pyspark.sql import Window

window_id = Window.partitionBy("ID_LANCAMENTO")
df = df.withColumn("_id_count", F.count("ID_LANCAMENTO").over(window_id))

error_cond = (
    (F.col("ID_LANCAMENTO").isNotNull()) &
    (F.col("_id_count") == 1) &
    (F.col("DT_LANCAMENTO").isNotNull()) &
    (F.regexp_extract(F.col("DT_LANCAMENTO").cast("string"), r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$', 0) != "") &
    (F.col("VL_LANCAMENTO").isNotNull()) &
    (F.col("VL_LANCAMENTO").between(-999999999.99, 999999999.99)) &
    (F.col("NM_FORNECEDOR").isNotNull())
)

df_clean    = df.filter(error_cond).drop("_id_count")
df_rejected = df.filter(~error_cond).drop("_id_count").withColumn(
    "_dq_error",
    F.concat_ws("; ",
        F.when(F.col("ID_LANCAMENTO").isNull(), F.lit("ID_LANCAMENTO is null")).otherwise(F.lit(None)),
        F.when(F.col("_id_count") > 1, F.lit("ID_LANCAMENTO is not unique")).otherwise(F.lit(None)),
        F.when(F.col("DT_LANCAMENTO").isNull(), F.lit("DT_LANCAMENTO is null")).otherwise(F.lit(None)),
        F.when(F.regexp_extract(F.col("DT_LANCAMENTO").cast("string"), r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$', 0) == "", F.lit("DT_LANCAMENTO failed regex")).otherwise(F.lit(None)),
        F.when(F.col("VL_LANCAMENTO").isNull(), F.lit("VL_LANCAMENTO is null")).otherwise(F.lit(None)),
        F.when(~F.col("VL_LANCAMENTO").between(-999999999.99, 999999999.99), F.lit("VL_LANCAMENTO out of range")).otherwise(F.lit(None)),
        F.when(F.col("NM_FORNECEDOR").isNull(), F.lit("NM_FORNECEDOR is null")).otherwise(F.lit(None))
    )
)

# Masking
df_clean = df_clean.withColumn("VL_LANCAMENTO", F.sha2(F.col("VL_LANCAMENTO").cast("string"), 512))

df_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
