from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLDefensiveIncrementalToSilver") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql("SHOW DATABASES").show(truncate=False)
spark.sql("USE joepostgres")

bronze_table = "nfl_defensive"
silver_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_defensive_output"

# Load bronze
bronze_df = spark.sql("SELECT * FROM " + bronze_table)
bronze_count = bronze_df.count()
# Load silver if exists
try:
    #silver_df = spark.read.parquet(silver_path)
    bronze_inc_df = spark.sql("SELECT * FROM nfl_defensive_inc")
    inc_count = silver_df.count()
except Exception:
    inc_count = 0

print("[DEFENSIVE] bronze_count=" + str(bronze_count)
      + ", bronze_inc_count=" + str(inc_count))

if bronze_count < inc_count:
    print("[DEFENSIVE] New data detected. Rebuilding silver...")

    df = bronze_inc_df

    # Trim string columns
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    # Replace values
    df = df.replace("--", "0")
    df = df.replace("", None)

    # Remove duplicates
    df = df.dropDuplicates()

    # Clean player_id
    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    # Split name
    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn(
        "first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
    )
    df = df.filter(col("first_name").isNotNull())

    # Integer columns
    int_cols = [
        "solo_tackles",
        "assisted_tackles",
        "passes_defended",
        "ints",
        "ints_for_tds",
        "int_yards",
        "safties"
    ]
    for c in int_cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", ""))
        df = df.withColumn(c, col(c).cast(IntegerType()))

    # Float columns
    df = df.withColumn("sacks", col("sacks").cast(FloatType()))
    df = df.withColumn("total_tackles", col("total_tackles").cast(FloatType()))

    # Filter rows
    df = df.filter(col("total_tackles") >= 100)

    # Drop columns
    df = df.drop(
        "position",
        "yards_per_int",
        "longest_int_return",
        "name",
        "name_split"
    )

    df.write.mode("overwrite").parquet(silver_path)
    print("[DEFENSIVE] Silver refreshed.")

else:
    print("[DEFENSIVE] No new data. Skipping rebuild.")

spark.stop()