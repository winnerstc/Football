from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

# --- Spark Session Setup ---
spark = SparkSession.builder \
    .appName("NFLRushingIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE joepostgres")

# --- Configuration ---
bronze_table = "nfl_rushing"        
inc_source_table = "nfl_rushing_inc"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_rushing_output" 
# bronze_path is not needed for the SQL INSERT OVERWRITE approach

# Load current target count (nfl_rushing)
# This will load the raw data from the last run to check its count
bronze_df = spark.sql(f"SELECT * FROM {bronze_table}")
bronze_count = bronze_df.count()

# Load new source count (nfl_rushing_inc)
try:
    inc_df = spark.sql(f"SELECT * FROM {inc_source_table}")
    inc_count = inc_df.count()
except Exception:
    inc_count = 0

print(f"[RUSHING] bronze_count={bronze_count}, incremental_count={inc_count}")

# --- Conditional Processing ---
if bronze_count < inc_count:
    print("[RUSHING] New data detected. Rebuilding silver and updating bronze count checkpoint...")

    df = inc_df # Start with the raw incremental data for cleaning

    # 1. Trim string columns
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    # 2. Replace values
    df = df.replace("--", "0")
    df = df.replace("", None)

    # 3. Remove duplicates
    df = df.dropDuplicates()

    # 4. Clean player_id
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # 5. Split name
    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn(
        "first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
    )
    df = df.filter(col("first_name").isNotNull())

    # 6. Integer columns
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

    # 7. Float columns and Longest Run
    df = df.withColumn("yards_per_carry", col("yards_per_carry").cast(FloatType()))
    df = df.withColumn(
        "longest_rushing_run",
        regexp_replace(col("longest_rushing_run"), "T", "").cast(IntegerType())
    )

    # 8. Filter rows
    df = df.filter(col("rushing_yards") >= 1000)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    # 9. Drop columns
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

    # --- FINAL WRITE OPERATIONS ---
    
    # 1. Write the CLEANED data (df) to the Silver Path (final output)
    df.write.mode("overwrite").parquet(silver_path)
    
    # 2. Overwrite the Bronze Table (nfl_rushing) with the RAW data (inc_df)
    # Using INSERT OVERWRITE TABLE guarantees a full replacement, fixing the count issue.
    spark.sql(f"INSERT OVERWRITE TABLE joepostgres.{bronze_table} SELECT * FROM joepostgres.{inc_source_table}")
    
    print("[RUSHING] Silver refreshed and Bronze count checkpoint updated.")

else:
    print("[RUSHING] No new data. Skipping rebuild.")

spark.stop()
