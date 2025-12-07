from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, split, trim, when, size
)
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLDefensiveToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------------------------
# Load Bronze Table from Hive
# ------------------------------------------------
spark.sql("USE joepostgres")
df = spark.sql("SELECT * FROM nfl_defensive")

# ------------------------------------------------
# Trim all string columns
# ------------------------------------------------
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

# ------------------------------------------------
# Replace "--" with "0" (stringtostring only)
# ------------------------------------------------
df = df.replace("--", "0")

# ------------------------------------------------
# Replace empty strings "" with NULL
# ------------------------------------------------
df = df.replace("", None)

# ------------------------------------------------
# Requirement: remove row if ANY column is null or empty
# ------------------------------------------------
#df = df.dropna("any")

# ------------------------------------------------
# Remove duplicate rows
# ------------------------------------------------
df = df.dropDuplicates()

# ------------------------------------------------
# Extract numeric player_id: fredevans/2513736  2513736
# ------------------------------------------------
df = df.withColumn(
    "player_id",
    regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
)

# ------------------------------------------------
# SAFE name split "Evans, Fred"
# ------------------------------------------------

df = df.withColumn("name_split", split(col("name"), ", "))

df = df.withColumn("last_name", col("name_split")[0])
df = df.withColumn(
    "first_name",
    when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
)

# Remove malformed names
df = df.filter(col("first_name").isNotNull())

# ------------------------------------------------
# Convert integer numeric columns
# ------------------------------------------------
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

# ------------------------------------------------
# Convert float numeric columns
# ------------------------------------------------
df = df.withColumn("sacks", col("sacks").cast(FloatType()))
df = df.withColumn("total_tackles", col("total_tackles").cast(FloatType()))

# ------------------------------------------------
# Remove rows where total_tackles < 100
# ------------------------------------------------
#df = df.filter(col("total_tackles") >= 100)
df = df.filter(
    (col("total_tackles") >= 100) |
    (col("sacks") > 10) |
    (col("ints") > 5)
)
# ------------------------------------------------
# Remove rows before year 1970
# ------------------------------------------------
#df = df.filter(col("year").cast(IntegerType()) >= 1970)

# ------------------------------------------------
# Drop unwanted columns
# ------------------------------------------------

df = df.drop(
    "position",
    "yards_per_int",
    "longest_int_return",
    "name",
    "name_split"
)

# ------------------------------------------------
# Debug Output
# ------------------------------------------------
#print("Final row count:", df.count())
df.show(20, truncate=False)
#df.printSchema()

# ------------------------------------------------
# Write Silver Output
# ------------------------------------------------
output_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_defensive_output"
df.write.mode("overwrite").parquet(output_path)

spark.stop()