from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, split, trim, when, size
)
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLRushingToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------------------------
# Load Bronze Table from Hive
# ------------------------------------------------
spark.sql("USE joepostgres")
df = spark.sql("SELECT * FROM nfl_rushing")

# ------------------------------------------------
# Trim whitespace for all string columns
# ------------------------------------------------
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

# ------------------------------------------------
# Replace "--" with NULL BEFORE anything else
# ------------------------------------------------
df = df.replace("--", "0")

# ------------------------------------------------
# Replace empty strings "" with NULL
# ------------------------------------------------
df = df.replace("", None)

# ------------------------------------------------
# Requirement: remove row if ANY column is null
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
# Split name safely: "Evans, Fred"
# ------------------------------------------------

df = df.withColumn("name_split", split(col("name"), ", "))

df = df.withColumn("last_name", col("name_split")[0])
df = df.withColumn(
    "first_name",
    when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
)

# Must have valid first_name
df = df.filter(col("first_name").isNotNull())

# ------------------------------------------------
# Replace "--" no longer needed  handled earlier
# Now convert numeric fields
# ------------------------------------------------
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

# ------------------------------------------------
# Convert float columns
# ------------------------------------------------
df = df.withColumn("yards_per_carry", col("yards_per_carry").cast(FloatType()))

# ------------------------------------------------
# Clean longest_rushing_run  remove "T"
# ------------------------------------------------
df = df.withColumn(
    "longest_rushing_run",
    regexp_replace(col("longest_rushing_run"), "T", "").cast(IntegerType())
)

# ------------------------------------------------
# Remove rows where rushing_yards < 1000
# ------------------------------------------------
df = df.filter(col("rushing_yards") >= 1000)

# ------------------------------------------------
# Remove rows before year 1970
# ------------------------------------------------
df = df.filter(col("year").cast(IntegerType()) >= 1970)

# ------------------------------------------------
# Drop unwanted columns
# ------------------------------------------------
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

# ------------------------------------------------
# Debug Output
# ------------------------------------------------

#print("Final row count:", df.count())
#df.show(20, truncate=False)
#df.printSchema()

# ------------------------------------------------
# Write Silver Output
# ------------------------------------------------
output_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_rushing_output"
df.write.mode("overwrite").parquet(output_path)

spark.stop()