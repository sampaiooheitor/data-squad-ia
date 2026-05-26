from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

SOURCE_PATH   = "/Volumes/prd/bronze/landing/TBRH_FUNCIONARIOS/raw.csv"
TABLE_NAME    = "prd.bronze.TBRH_FUNCIONARIOS"
SOURCE_SYSTEM = "data_squad"

df = spark.read.option("header", "true").option("sep", "|").option("mergeSchema", "true").csv(SOURCE_PATH)

df = (
    df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
)

(
    df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(TABLE_NAME)
)
print(f"Bronze: {df.count()} rows appended to {TABLE_NAME}")
