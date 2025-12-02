from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, split, trim, when, size
)
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLReceivingToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------------------------
# Load Bronze Table from Hive
# ------------------------------------------------
spark.sql("USE joepostgres")
df = spark.sql("SELECT * FROM nfl_receiving")

# ------------------------------------------------
# Trim whitespace from all string columns
# ------------------------------------------------
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

# ------------------------------------------------
# Replace "--" with STRING "0" (safe)
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
#df = df.dropDuplicates()

# ------------------------------------------------
# Clean player_id (keep numbers after "/")
# fredevans/2513736  2513736
# ------------------------------------------------
df = df.withColumn(
    "player_id",
    regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
)

# ------------------------------------------------
# Split name safely: "Evans, Fred"
# ------------------------------------------------

df = df.withColumn("name_split", split(col("name"), ", "))

df = df.withColumn("last_name", col("name_split")[0])
df = df.withColumn(
    "first_name",
    when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
)

# Ensure valid first_name
df = df.filter(col("first_name").isNotNull())

# ------------------------------------------------
# Replace numeric fields & cast to integer
# ------------------------------------------------
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

# ------------------------------------------------
# Convert float columns
# ------------------------------------------------
df = df.withColumn("yards_per_reception", col("yards_per_reception").cast(FloatType()))

# ------------------------------------------------
# Clean longest_reception  remove "T" suffix
# ------------------------------------------------
df = df.withColumn(
    "longest_reception",
    regexp_replace(col("longest_reception"), "T", "").cast(IntegerType())
)

# ------------------------------------------------
# Remove rows where receiving_yards < 1000
# ------------------------------------------------
df = df.filter(col("receiving_yards") >= 1000)

# ------------------------------------------------
# Remove rows before 1970
# ------------------------------------------------
df = df.filter(col("year").cast(IntegerType()) >= 1970)

# ------------------------------------------------
# Drop unwanted columns
# ------------------------------------------------
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

# ------------------------------------------------
# Debug Output
# ------------------------------------------------
#print("Final row count:", df.count())
#df.show(20, truncate=False)
#df.printSchema()

# ------------------------------------------------
# Write to Silver Layer
# ------------------------------------------------
output_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_receiving_output"
df.write.mode("overwrite").parquet(output_path)

spark.stop()