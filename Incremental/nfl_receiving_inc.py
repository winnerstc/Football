# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLReceivingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

bronze_table = "nfl_receiving"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_receiving_output"

bronze_df = spark.sql("SELECT * FROM " + bronze_table)
bronze_count = bronze_df.count()

try:
    silver_df = spark.read.parquet(silver_path)
    silver_count = silver_df.count()
except Exception:
    silver_count = 0

print("[RECEIVING] bronze_count=" + str(bronze_count)
      + ", silver_count=" + str(silver_count))

if bronze_count > silver_count:
    print("[RECEIVING] New data detected. Rebuilding...")

    df = bronze_df

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.replace("--", "0")
    df = df.replace("", None)

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

    int_cols = [
        "receptions",
        "receiving_yards",
        "receiving_tds",
        "first_down_receptions",
        "fumbles"
    ]
    for c in int_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, col(c).cast(IntegerType()))

    df = df.withColumn("yards_per_reception", col("yards_per_reception").cast(FloatType()))

    df = df.withColumn(
        "longest_reception",
        regexp_replace(col("longest_reception"), "T", "").cast(IntegerType())
    )

    df = df.filter(col("receiving_yards") >= 1000)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    df = df.drop(
        "position",
        "yards_per_reception",
        "yards_per_game",
        "receptions_longer_than_20_yards",
        "receptions_longer_than_40_yards",
        "first_down_receptions",
        "name",
        "name_split"
    )

    df.write.mode("overwrite").parquet(silver_path)
    print("[RECEIVING] Silver refreshed.")

else:
    print("[RECEIVING] No new data. Skipping.")

spark.stop()
