

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

# --- Spark Session Setup ---
spark = SparkSession.builder \
    .appName("NFLKickingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# --- Configuration ---
bronze_table = "nfl_kicking"         
inc_source_table = "nfl_kicking_inc" 
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_kicking_output" # Final Output Path for Cleaned Data

# Load current target count (nfl_kicking)
bronze_df = spark.sql(f"SELECT * FROM {bronze_table}")
bronze_count = bronze_df.count()

# Load new source count (nfl_kicking_inc)
try:
    inc_df = spark.sql(f"SELECT * FROM {inc_source_table}")
    inc_count = inc_df.count()
except Exception:
    inc_count = 0

print(f"[KICKING] bronze_count={bronze_count}, inc_count={inc_count}")

# --- Conditional Processing ---
if bronze_count < inc_count:
    print("[KICKING] New data detected. Rebuilding silver and updating bronze count checkpoint...")

    df = inc_df # Start with the incremental source data

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
        "kicks_blocked",
        "longest_fg_made",
        "fgs_made",
        "fgs_attempted",
        "fgs_made_2029_yards",
        "fgs_attempted_2029_yards",
        "fgs_made_3039_yards",
        "fgs_attempted_3039_yards",
        "fgs_made_4049_yards",
        "fgs_attempted_4049_yards",
        "fgs_made_50_yards",
        "fgs_attempted_50_yards",
        "extra_points_attempted",
        "extra_points_made"
    ]

    for c in int_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, regexp_replace(col(c), "[^0-9-]", "")) # Safely remove non-numeric chars
        df = df.withColumn(c, col(c).cast(IntegerType()))

    float_cols = [
        "fg_percentage",
        "fgs_percentage_2029_yards",
        "fgs_percentage_3039_yards",
        "fgs_percentage_4049_yards",
        "fgs_percentage_50_yards",
        "percentage_of_extra_points"
    ]

    for c in float_cols:
        df = df.withColumn(c, col(c).cast(FloatType()))

    df = df.filter(col("fgs_made") >= 20)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    df = df.drop(
        "position",
        "kicks_blocked",
        "extra_points_blocked",
        "name",
        "name_split"
    )

    # --- FINAL WRITE OPERATIONS ---
    
    # 1. Write the CLEANED data (df) to the Silver Path (final output)
    df.write.mode("overwrite").parquet(silver_path)

    print("[KICKING] Silver refreshed.")

    # 2. Overwrite the Bronze Table (nfl_kicking) with the RAW data (inc_df)
    # Using INSERT OVERWRITE TABLE guarantees a full replacement, fixing the count issue.
    spark.sql(f"INSERT OVERWRITE TABLE joepostgres.{bronze_table} SELECT * FROM joepostgres.{inc_source_table}")

else:
    print("[KICKING] No new data. Skipping rebuild.")

spark.stop()
