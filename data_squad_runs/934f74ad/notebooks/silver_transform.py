from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE   = "prd.bronze.TBRH_FUNCIONARIOS"
SILVER_TABLE   = "prd.silver.TBRH_FUNCIONARIOS"
REJECTED_TABLE = "prd.silver.TBRH_FUNCIONARIOS_rejected"

df = spark.read.table(BRONZE_TABLE)
total = df.count()

# Schema casting
df = df.withColumn("ID_FUNCIONARIO", F.col("ID_FUNCIONARIO").cast("integer"))
df = df.withColumn("DT_ADMISSAO", F.to_date(F.col("DT_ADMISSAO"), "yyyy-MM-dd"))
df = df.withColumn("VL_SALARIO", F.col("VL_SALARIO").cast("double"))
df = df.withColumn("FL_ATIVO", F.col("FL_ATIVO").cast("boolean"))

# DQ filtering
from pyspark.sql import Window

id_count_window = df.groupBy("ID_FUNCIONARIO").count().filter(F.col("count") > 1).select("ID_FUNCIONARIO").withColumnRenamed("ID_FUNCIONARIO", "_dup_id")
cpf_count_window = df.groupBy("CD_CPF").count().filter(F.col("count") > 1).select("CD_CPF").withColumnRenamed("CD_CPF", "_dup_cpf")

df = df.join(id_count_window, df["ID_FUNCIONARIO"] == id_count_window["_dup_id"], "left").join(cpf_count_window, df["CD_CPF"] == cpf_count_window["_dup_cpf"], "left")

error_cond = (
    F.col("ID_FUNCIONARIO").isNotNull() &
    F.col("_dup_id").isNull() &
    (F.col("ID_FUNCIONARIO").between(1, 2147483647)) &
    F.col("NM_FUNCIONARIO").isNotNull() &
    F.col("CD_CPF").isNotNull() &
    F.col("_dup_cpf").isNull() &
    F.col("CD_CPF").rlike(r'^[0-9]{3}\.?[0-9]{3}\.?[0-9]{3}\-?[0-9]{2}$') &
    F.col("DT_ADMISSAO").isNotNull() &
    (F.col("DT_ADMISSAO").between(F.lit("1900-01-01").cast("date"), F.current_date())) &
    F.col("NM_CARGO").isNotNull() &
    F.col("NM_DEPARTAMENTO").isNotNull() &
    F.col("VL_SALARIO").isNotNull() &
    (F.col("VL_SALARIO").between(1412.00, 999999.99)) &
    F.col("FL_ATIVO").isNotNull() &
    F.col("FL_ATIVO").isin(True, False)
)

df_clean = df.filter(error_cond).drop("_dup_id", "_dup_cpf")
df_rejected = df.filter(~error_cond).drop("_dup_id", "_dup_cpf").withColumn("_dq_error", F.lit("DQ rule failed"))

# Masking
df_clean = df_clean.withColumn("NM_FUNCIONARIO", F.regexp_replace(F.col("NM_FUNCIONARIO").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("CD_CPF", F.regexp_replace(F.col("CD_CPF").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("DT_ADMISSAO", F.regexp_replace(F.col("DT_ADMISSAO").cast("string"), ".", "*"))
df_clean = df_clean.withColumn("VL_SALARIO", F.sha2(F.col("VL_SALARIO").cast("string"), 512))

df_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
df_rejected.write.format("delta").mode("overwrite").saveAsTable(REJECTED_TABLE)
print(f"Silver: {df_clean.count()} accepted, {df_rejected.count()} rejected / {total} total")
print(f"  Clean    -> {SILVER_TABLE}")
print(f"  Rejected -> {REJECTED_TABLE}")
