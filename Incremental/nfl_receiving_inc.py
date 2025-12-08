
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

# --- Spark Session Setup ---
spark = SparkSession.builder \
   .appName("NFLReceivingIncrementalToSilver") \
   .enableHiveSupport() \
   .getOrCreate()

spark.sql("USE joepostgres")

# --- Configuration ---
bronze_table = "nfl_receiving"       
inc_source_table = "nfl_receiving_inc" 
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_receiving_output" 

# Load current target count (nfl_receiving)
# This will load the raw data from the last run to check its count
bronze_df = spark.sql(f"SELECT * FROM {bronze_table}")
bronze_count = bronze_df.count()

# Load new source count (nfl_receiving_inc)
try:
    inc_df = spark.sql(f"SELECT * FROM {inc_source_table}")
    inc_count = inc_df.count()
except Exception:
    inc_count = 0

print(f"[RECEIVING] bronze_count={bronze_count}, inc_count={inc_count}")

# --- Conditional Processing ---
if bronze_count < inc_count:
   print("[RECEIVING] New data detected. Rebuilding silver and updating bronze count checkpoint...")

    df = inc_df # Start with the raw incremental data for cleaning

    # 1. Trim string columns
    for c, t in df.dtypes:
        if t == "string":
           df = df.withColumn(c, trim(col(c)))

    # 2. Replace values
    df = df.replace("--", "0")
    df = df.replace("", None)

    # 3. Clean player_id and Split name
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

    # 4. Integer columns
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

    # 5. Float columns and Longest Reception
    df = df.withColumn("yards_per_reception", col("yards_per_reception").cast(FloatType()))

    df = df.withColumn(
       "longest_reception",
       regexp_replace(col("longest_reception"), "T", "").cast(IntegerType())
    )

    # 6. Filter rows
    df = df.filter(col("receiving_yards") >= 1000)
    df = df.filter(col("year").cast(IntegerType()) >= 1970)

    # 7. Drop columns
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

    # --- FINAL WRITE OPERATIONS ---
    
    # 1. Write the CLEANED data (df) to the Silver Path (final output)
    df.write.mode("overwrite").parquet(silver_path)

    print("[RECEIVING] Silver refreshed.")

    # 2. Overwrite the Bronze Table (nfl_receiving) with the RAW data (inc_df)
    # Using INSERT OVERWRITE TABLE guarantees a full replacement, fixing the count issue.
    spark.sql(f"INSERT OVERWRITE TABLE joepostgres.{bronze_table} SELECT * FROM joepostgres.{inc_source_table}")

else:
   print("[RECEIVING] No new data. Skipping.")

spark.stop()
