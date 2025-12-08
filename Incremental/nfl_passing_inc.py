# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLPassingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# --- Configuration ---
bronze_table = "nfl_passing"
inc_source_table = "nfl_passing_inc" # Corrected variable name and quotes
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_passing_output"

# Load current target count (nfl_passing)
bronze_df = spark.sql(f"SELECT * FROM {bronze_table}")
bronze_count = bronze_df.count()

# Load new source count (nfl_passing_inc)
try:
    inc_df = spark.sql(f"SELECT * FROM {inc_source_table}") # Use corrected variable
    inc_count = inc_df.count()
except Exception:
    inc_count = 0

print(f"[PASSING] bronze_count={bronze_count}, inc_count={inc_count}")

# --- Conditional Processing ---
if bronze_count < inc_count: # Corrected from silver_count to inc_count
    print("[PASSING] New data detected. Rebuilding silver and updating bronze count checkpoint...")

    df = inc_df # Corrected: Start with the incremental source data

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

    # --- FINAL WRITE OPERATIONS ---
    
    # 1. Write the CLEANED data (df) to the Silver Path (final output)
    df.write.mode("overwrite").parquet(silver_path)

    print("[PASSING] Silver refreshed.")

    # 2. Overwrite the Bronze Table (nfl_passing) with the RAW data (inc_df)
    # Using INSERT OVERWRITE TABLE guarantees a full replacement, fixing the count issue.
    spark.sql(f"INSERT OVERWRITE TABLE joepostgres.{bronze_table} SELECT * FROM joepostgres.{inc_source_table}")

else:
    print("[PASSING] No new data. Skipping rebuild.")

spark.stop()
