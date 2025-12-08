from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

# --- Spark Session Setup ---
spark = SparkSession.builder \
    .appName("NFLDefensiveIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()
    
spark.sql("SHOW DATABASES").show(truncate=False)
spark.sql("USE joepostgres")

# --- Configuration ---
bronze_table = "nfl_defensive"         
inc_source_table = "nfl_defensive_inc" 
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_defensive_output" 

# Load current target count (nfl_defensive)
# This will load the raw data from the last run to check its count
bronze_df = spark.sql(f"SELECT * FROM {bronze_table}")
bronze_count = bronze_df.count()

# Load new source count (nfl_defensive_inc)
try:
    inc_df = spark.sql(f"SELECT * FROM {inc_source_table}")
    inc_count = inc_df.count()
except Exception:
    inc_count = 0

print(f"[DEFENSIVE] bronze_count={bronze_count}, inc_count={inc_count}")

# --- Conditional Processing ---
if bronze_count < inc_count:
    print("[DEFENSIVE] New data detected. Rebuilding silver and updating bronze count checkpoint...")

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
        "solo_tackles", "assisted_tackles", "passes_defended", "ints", 
        "ints_for_tds", "int_yards", "safties"
    ]
    for c in int_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, col(c).cast(IntegerType()))

    # 7. Float columns and Filtering
    df = df.withColumn("sacks", col("sacks").cast(FloatType()))
    df = df.withColumn("total_tackles", col("total_tackles").cast(FloatType()))
    df = df.filter(col("total_tackles") >= 100)

    # 8. Drop columns
    df = df.drop(
        "position", "yards_per_int", "longest_int_return", 
        "name", "name_split"
    )

    # --- FINAL WRITE OPERATIONS ---
    
    # 1. Write the CLEANED data (df) to the Silver Path (final output)
    df.write.mode("overwrite").parquet(silver_path)
    
    # 2. Overwrite the Bronze Table (nfl_defensive) with the RAW data (inc_df)
    # Using insertInto is safer for external tables than saveAsTable, as it preserves the DDL.
    inc_df.write.mode("overwrite").insertInto(f"joepostgres.{bronze_table}")
    
    print("[DEFENSIVE] Silver refreshed and Bronze count checkpoint updated.")

else:
    print("[DEFENSIVE] No new data. Skipping rebuild.")

spark.stop()
