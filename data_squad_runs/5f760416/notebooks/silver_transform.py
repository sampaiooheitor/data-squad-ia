from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE   = "prd.bronze.TBMKT_CAMPANHAS"
SILVER_TABLE   = "prd.silver.TBMKT_CAMPANHAS"
REJECTED_TABLE = "prd.silver.TBMKT_CAMPANHAS_rejected"

df = spark.read.table(BRONZE_TABLE)
total = df.count()

# Schema casting
df = df.withColumn("ID_CAMPANHA", F.col("ID_CAMPANHA").cast("integer"))
df = df.withColumn("VL_INVESTIMENTO", F.col("VL_INVESTIMENTO").cast("double"))
df = df.withColumn("DT_INICIO_CAMPANHA", F.to_date(F.col("DT_INICIO_CAMPANHA"), "yyyy-MM-dd"))
df = df.withColumn("FL_CONVERTIDO", F.col("FL_CONVERTIDO").cast("boolean"))

# DQ filtering
from pyspark.sql import functions as F

# Uniqueness check for ID_CAMPANHA via window
from pyspark.sql import Window
id_window = Window.partitionBy("ID_CAMPANHA")
df = df.withColumn("_id_campanha_cnt", F.count("ID_CAMPANHA").over(id_window))

error_cond = (
    (F.col("ID_CAMPANHA").isNotNull()) &
    (F.col("_id_campanha_cnt") == 1) &
    (F.col("ID_CAMPANHA").between(1, 9999999999)) &
    (F.col("NM_CLIENTE").isNotNull()) &
    (F.col("CD_CPF").isNotNull()) &
    (F.regexp_extract(F.col("CD_CPF"), r'^\d{3}\.\d{3}\.\d{3}-\d{2}$', 0) != "") &
    (F.col("DS_SEGMENTO").isNotNull()) &
    (F.col("VL_INVESTIMENTO").isNotNull()) &
    (F.col("VL_INVESTIMENTO").between(0.00, 99999999.99)) &
    (F.col("DT_INICIO_CAMPANHA").isNotNull()) &
    (F.col("FL_CONVERTIDO").isNotNull()) &
    (F.col("DS_CANAL").isNotNull())
)

df_clean    = df.filter(error_cond).drop("_id_campanha_cnt")
df_rejected = df.filter(~error_cond).withColumn("_dq_error", F.lit("DQ rule failed")).drop("_id_campanha_cnt")

# Masking
df_clean = df_clean.withColumn("NM_CLIENTE", F.regexp_replace(F.col("NM_CLIENTE").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("CD_CPF", F.regexp_replace(F.col("CD_CPF").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("VL_INVESTIMENTO", F.sha2(F.col("VL_INVESTIMENTO").cast("string"), 512))

df_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
