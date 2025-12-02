from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, split, trim, when, size
)
from pyspark.sql.types import IntegerType, FloatType

spark = SparkSession.builder \
    .appName("NFLPlayersToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------------------------
# Load Bronze Table
# ------------------------------------------------
spark.sql("USE joepostgres")
df = spark.sql("SELECT * FROM nfl_kicking")

# ------------------------------------------------
# Trim whitespace for all string columns
# ------------------------------------------------
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

# ------------------------------------------------
# Replace "--" with "0" (string-safe)
# ------------------------------------------------
df = df.replace("--", "0")

# ------------------------------------------------
# Replace empty strings "" with NULL
# ------------------------------------------------
df = df.replace("", None)

# ------------------------------------------------
# player_id cleanup (remove prefix before "/")
# ------------------------------------------------
df = df.withColumn(
    "player_id",
    regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
)

# ------------------------------------------------
# SAFE name split: "Evans, Fred"
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
# Integer fields
# ------------------------------------------------
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
    df = (
        df
        .withColumn(c, regexp_replace(col(c), ",", ""))
        .withColumn(c, regexp_replace(col(c), "[^0-9-]", ""))  # keep only digits
        .withColumn(c, col(c).cast(IntegerType()))
    )

# ------------------------------------------------
# Float fields
# ------------------------------------------------
float_cols = [
    "fg_percentage",                # stays the same
    "fgs_percentage_2029_yards",   # corrected
    "fgs_percentage_3039_yards",   # corrected
    "fgs_percentage_4049_yards",   # corrected
    "fgs_percentage_50_yards",     # corrected
    "percentage_of_extra_points"   # corrected
]


for c in float_cols:
    df = df.withColumn(c, col(c).cast(FloatType()))

# ------------------------------------------------
# Filter rows
# ------------------------------------------------

df = df.filter(col("fgs_made") >= 20)
df = df.filter(col("year").cast(IntegerType()) >= 1970)

# ------------------------------------------------
# Drop unwanted columns
# ------------------------------------------------
df = df.drop(
    "position",
    "kicks_blocked",
    "extra_points_blocked",
    "name",
    "name_split"
)

# ------------------------------------------------
# Write to Silver (FIXED)
# ------------------------------------------------
output_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_kicking_output"

df.write.mode("overwrite").parquet(output_path)

spark.stop()