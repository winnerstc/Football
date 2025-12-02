# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("NFLReturnsIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

bronze_table = "nfl_returns"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_returns_output"

bronze_df = spark.sql("SELECT * FROM " + bronze_table)
bronze_count = bronze_df.count()

try:
    silver_df = spark.read.parquet(silver_path)
    silver_count = silver_df.count()
except Exception:
    silver_count = 0

print("[RETURNS] bronze_count=" + str(bronze_count)
      + ", silver_count=" + str(silver_count))

if bronze_count > silver_count:
    print("[RETURNS] New data detected. Rebuilding...")

    df = bronze_df

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    dash_cols = [
        "kick_returns",
        "yards_kick_returned",
        "kick_returns_for_tds",
        "punt_returns",
        "yards_punt_returned",
        "punt_returns_for_tds"
    ]
    df = df.replace("--", "0", subset=dash_cols)
    df = df.replace("", None)

    df = df.dropDuplicates()

    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn(
        "first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
    )
    df = df.filter(col("first_name").isNotNull())

    for c in dash_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, col(c).cast(IntegerType()))

    df = df.filter(col("kick_returns_for_tds").cast(IntegerType()) >= 1)
    df = df.filter(col("punt_returns_for_tds").cast(IntegerType()) >= 1)

    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    df = df.drop(
        "name",
        "name_split"
    )

    df.write.mode("overwrite").parquet(silver_path)
    print("[RETURNS] Silver refreshed.")

else:
    print("[RETURNS] No new data. Skipping.")

spark.stop()
