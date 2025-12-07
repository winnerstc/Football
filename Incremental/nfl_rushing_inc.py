# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLRushingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

bronze_table = "nfl_rushing"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_rushing_output"

bronze_df = spark.sql("SELECT * FROM " + bronze_table)
bronze_count = bronze_df.count()

try:
    silver_df = spark.read.parquet(silver_path)
    silver_count = silver_df.count()
except Exception:
    silver_count = 0

print("[RUSHING] bronze_count=" + str(bronze_count)
      + ", silver_count=" + str(silver_count))

if bronze_count > silver_count:
    print("[RUSHING] New data detected. Rebuilding...")

    df = bronze_df

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.replace("--", "0")
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

    int_cols = [
        "rushing_attempts",
        "rushing_yards",
        "rushing_tds",
        "rushing_first_downs",
        "fumbles"
    ]
    for c in int_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, col(c).cast(IntegerType()))

    df = df.withColumn("yards_per_carry", col("yards_per_carry").cast(FloatType()))

    df = df.withColumn(
        "longest_rushing_run",
        regexp_replace(col("longest_rushing_run"), "T", "").cast(IntegerType())
    )

    df = df.filter(col("rushing_yards") >= 1000)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    df = df.drop(
        "position",
        "rushing_attempts_per_game",
        "rushing_yards_per_game",
        "percentage_of_rushing_first_downs",
        "rushing_more_than_20_yards",
        "rushing_more_than_40_yards",
        "name",
        "name_split"
    )

    df.write.mode("overwrite").parquet(silver_path)
    print("[RUSHING] Silver refreshed.")

else:
    print("[RUSHING] No new data. Skipping.")

spark.stop()
