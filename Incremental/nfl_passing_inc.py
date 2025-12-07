# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLPassingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

bronze_table = "nfl_passing"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_passing_output"

bronze_df = spark.sql("SELECT * FROM " + bronze_table)
bronze_count = bronze_df.count()

try:
    silver_df = spark.read.parquet(silver_path)
    silver_count = silver_df.count()
except Exception:
    silver_count = 0

print("[PASSING] bronze_count=" + str(bronze_count)
      + ", silver_count=" + str(silver_count))

if bronze_count > silver_count:
    print("[PASSING] New data detected. Rebuilding silver...")

    df = bronze_df

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.replace("", None)

    df = df.dropDuplicates()

    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split").getItem(0))
    df = df.withColumn(
        "first_name",
        when(size(col("name_split")) > 1, col("name_split").getItem(1)).otherwise(None)
    )

    df = df.filter(col("first_name").isNotNull())

    dash_cols = [
        "passes_attempted",
        "passes_completed",
        "completion_percentage",
        "passing_yards",
        "passing_yards_per_attempt",
        "td_passes",
        "ints",
        "sacks"
    ]

    for c in dash_cols:
        df = df.withColumn(c, when(col(c) == "--", "0").otherwise(col(c)))

    int_cols = [
        "passes_attempted",
        "passes_completed",
        "passing_yards",
        "td_passes",
        "ints"
    ]
    for c in int_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, col(c).cast(IntegerType()))

    float_cols = [
        "completion_percentage",
        "passing_yards_per_attempt",
        "sacks"
    ]
    for c in float_cols:
        df = df.withColumn(c, col(c).cast(FloatType()))

    df = df.filter(col("passing_yards") >= 1000)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    df = df.drop(
        "position",
        "pass_attempts_per_game",
        "passing_yards_per_game",
        "percentage_of_tds_per_attempts",
        "int_rate",
        "longest_pass",
        "passes_longer_than_20_yards",
        "passes_longer_than_40_yards",
        "sacked_yards_lost",
        "name",
        "name_split"
    )

    df.write.mode("overwrite").parquet(silver_path)
    print("[PASSING] Silver refreshed.")

else:
    print("[PASSING] No new data. Skipping rebuild.")

spark.stop()
