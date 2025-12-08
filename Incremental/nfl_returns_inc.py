

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType

# --- Spark Session Setup ---
spark = SparkSession.builder \
    .appName("NFLReturnsIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# --- Configuration ---
bronze_table = "nfl_returns"
inc_source_table = "nfl_returns_inc"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_returns_output"

# Load current target count (nfl_returns)
bronze_df = spark.sql(f"SELECT * FROM {bronze_table}")
bronze_count = bronze_df.count()

# Load new source count (nfl_returns_inc)
try:
    inc_df = spark.sql(f"SELECT * FROM {inc_source_table}")
    inc_count = inc_df.count()
except Exception:
    inc_count = 0

print(f"[RETURNS] bronze_count={bronze_count}, inc_count={inc_count}")

# --- Conditional Processing ---
if bronze_count < inc_count:
    print("[RETURNS] New data detected. Rebuilding silver and updating bronze count checkpoint...")

    df = inc_df # Start with the incremental source data

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

    # Cast integer columns after replacing non-numeric characters
    for c in dash_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, col(c).cast(IntegerType()))

    # Filters (Ensure filtering logic is correct using the original column types/names)
    df = df.filter(col("kick_returns_for_tds") >= 1) # Already cast to IntegerType above
    df = df.filter(col("punt_returns_for_tds") >= 1) # Already cast to IntegerType above

    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    df = df.drop(
        "name",
        "name_split"
    )

    # --- FINAL WRITE OPERATIONS ---
    
    # 1. Write the CLEANED data (df) to the Silver Path (final output)
    df.write.mode("overwrite").parquet(silver_path)

    print("[RETURNS] Silver refreshed.")

    # 2. Overwrite the Bronze Table (nfl_returns) with the RAW data (inc_df)
    # Using INSERT OVERWRITE TABLE guarantees a full replacement, fixing the count issue.
    spark.sql(f"INSERT OVERWRITE TABLE joepostgres.{bronze_table} SELECT * FROM joepostgres.{inc_source_table}")

else:
    print("[RETURNS] No new data. Skipping.")

spark.stop()
