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
df = df.withColumn("DT_PEDIDO", F.to_timestamp(F.col("DT_PEDIDO"), "yyyy-MM-dd HH:mm:ss"))

# DQ filtering
error_cond = (
    F.col("ID_PEDIDO").isNotNull() &
    F.col("NM_CLIENTE").isNotNull() &
    F.col("CD_CPF").isNotNull() &
    F.regexp_like(F.col("CD_CPF"), F.lit(r'^\d{3}\.\d{3}\.\d{3}-\d{2}$')) &
    F.col("NM_PRODUTO").isNotNull() &
    F.col("CD_CATEGORIA").isNotNull() &
    F.col("VL_TOTAL").isNotNull() &
    (F.col("VL_TOTAL") >= 0.01) & (F.col("VL_TOTAL") <= 999999.99) &
    F.col("DT_PEDIDO").isNotNull() &
    (F.col("DT_PEDIDO") >= F.to_timestamp(F.lit("2000-01-01 00:00:00"), "yyyy-MM-dd HH:mm:ss")) &
    (F.col("DT_PEDIDO") <= F.current_timestamp()) &
    F.col("FL_STATUS").isNotNull() &
    F.col("FL_STATUS").isin("PENDENTE", "APROVADO", "CANCELADO", "ENTREGUE", "DEVOLVIDO", "EM_TRANSITO") &
    (F.col("ID_PEDIDO") >= 1) & (F.col("ID_PEDIDO") <= 9999999999)
)

df_clean = df.filter(error_cond)
df_rejected = df.filter(~error_cond).withColumn("_dq_error", F.lit("DQ rule failed"))

# Uniqueness check for ID_PEDIDO (error severity)
dupl_ids = df_clean.groupBy("ID_PEDIDO").count().filter(F.col("count") > 1).select("ID_PEDIDO")
df_dupl = df_clean.join(dupl_ids, on="ID_PEDIDO", how="inner").withColumn("_dq_error", F.lit("DQ rule failed: ID_PEDIDO not unique"))
df_clean = df_clean.join(dupl_ids, on="ID_PEDIDO", how="left_anti")
df_rejected = df_rejected.unionByName(df_dupl)

# Masking
df_clean = df_clean.withColumn("NM_CLIENTE", F.regexp_replace(F.col("NM_CLIENTE").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("CD_CPF", F.regexp_replace(F.col("CD_CPF").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("VL_TOTAL", F.sha2(F.col("VL_TOTAL").cast("string"), 512))

df_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
