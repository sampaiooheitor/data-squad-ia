from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE   = "prd.bronze.TBFIN_LANCAMENTOS"
SILVER_TABLE   = "prd.silver.TBFIN_LANCAMENTOS"
REJECTED_TABLE = "prd.silver.TBFIN_LANCAMENTOS_rejected"

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
    (F.col("ID_LANCAMENTO").between(1, 9999999999)) &
    (F.col("DT_LANCAMENTO").isNotNull()) &
    (F.col("DT_LANCAMENTO").between(F.lit("2000-01-01").cast("date"), F.lit("2099-12-31").cast("date"))) &
    (F.col("VL_LANCAMENTO").isNotNull()) &
    (F.col("VL_LANCAMENTO").between(-999999999.99, 999999999.99)) &
    (F.col("CD_CENTRO_CUSTO").isNotNull()) &
    (F.col("NM_FORNECEDOR").isNotNull()) &
    (F.col("NR_CPF_CNPJ").isNotNull()) &
    (F.col("NR_CPF_CNPJ").rlike("^([0-9]{11}|[0-9]{14})$")) &
    (F.col("DS_HISTORICO").isNotNull()) &
    (F.col("CD_STATUS").isNotNull()) &
    (F.col("CD_STATUS").isin("ATIVO", "CANCELADO", "PENDENTE", "PROCESSADO", "ESTORNADO"))
)

df_clean    = df.filter(error_cond).drop("_id_count")
df_rejected = df.filter(~error_cond).drop("_id_count").withColumn("_dq_error", F.lit("DQ rule failed"))

# Masking
df_clean = df_clean.withColumn("VL_LANCAMENTO", F.sha2(F.col("VL_LANCAMENTO").cast("string"), 512))
df_clean = df_clean.withColumn("NM_FORNECEDOR", F.regexp_replace(F.col("NM_FORNECEDOR").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("NR_CPF_CNPJ", F.regexp_replace(F.col("NR_CPF_CNPJ").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("DS_HISTORICO", F.sha2(F.col("DS_HISTORICO").cast("string"), 512))

df_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
