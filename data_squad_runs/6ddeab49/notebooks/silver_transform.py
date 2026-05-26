from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE   = "prd.bronze.TBECOM_PEDIDOS"
SILVER_TABLE   = "prd.silver.TBECOM_PEDIDOS"
REJECTED_TABLE = "prd.silver.TBECOM_PEDIDOS_rejected"

df = spark.read.table(BRONZE_TABLE)
total = df.count()

# Schema casting
df = df.withColumn("ID_PEDIDO", F.col("ID_PEDIDO").cast("integer"))
df = df.withColumn("VL_TOTAL", F.col("VL_TOTAL").cast("double"))
df = df.withColumn("DT_PEDIDO", F.to_date(F.col("DT_PEDIDO"), "yyyy-MM-dd"))

# DQ filtering
error_cond = (
    (F.col("ID_PEDIDO").isNotNull()) &
    (F.col("ID_PEDIDO").between(1, 2147483647)) &
    (F.col("NM_CLIENTE").isNotNull()) &
    (F.col("CD_CPF").isNotNull()) &
    (F.col("CD_CPF").rlike(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')) &
    (F.col("NM_PRODUTO").isNotNull()) &
    (F.col("CD_CATEGORIA").isNotNull()) &
    (F.col("VL_TOTAL").isNotNull()) &
    (F.col("VL_TOTAL").between(0.01, 999999.99)) &
    (F.col("DT_PEDIDO").isNotNull()) &
    (F.col("DT_PEDIDO").between(F.lit("2000-01-01").cast("date"), F.current_date())) &
    (F.col("FL_STATUS").isNotNull()) &
    (F.col("FL_STATUS").isin("PENDENTE", "APROVADO", "ENVIADO", "ENTREGUE", "CANCELADO", "DEVOLVIDO"))
)

_id_pedido_counts = df.groupBy("ID_PEDIDO").count()
_duplicate_ids = _id_pedido_counts.filter(F.col("count") > 1).select("ID_PEDIDO")
_duplicate_ids_list = [row["ID_PEDIDO"] for row in _duplicate_ids.collect()]

uniqueness_cond = ~F.col("ID_PEDIDO").isin(_duplicate_ids_list) if _duplicate_ids_list else F.lit(True)

final_error_cond = error_cond & uniqueness_cond

df_clean = df.filter(final_error_cond)
df_rejected = df.filter(~final_error_cond).withColumn("_dq_error", F.lit("DQ rule failed"))

# Masking
df_clean = df_clean.withColumn("NM_CLIENTE", F.regexp_replace(F.col("NM_CLIENTE").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("CD_CPF", F.regexp_replace(F.col("CD_CPF").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("VL_TOTAL", F.sha2(F.col("VL_TOTAL").cast("string"), 512))

df_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
