from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, split, trim, when, size
)
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("NFLReturnsToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------------------------
# Load Bronze Table from Hive
# ------------------------------------------------
spark.sql("USE joepostgres")
df = spark.sql("SELECT * FROM nfl_returns")

# ------------------------------------------------
# Trim whitespace from all string columns
# ------------------------------------------------
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

# ------------------------------------------------
# Replace "--" with "0" for return statistics
# ------------------------------------------------

dash_cols = [
    "kick_returns",
    "yards_kick_returned",
    "kick_returns_for_tds",
    "punt_returns",
    "yards_punt_returned",
    "punt_returns_for_tds"
]

df = df.replace("--", "0", subset=dash_cols)

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
# Extract numeric player_id (fredevans/2513736  2513736)
# ------------------------------------------------

df = df.withColumn(
    "player_id",
    regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
)

# ------------------------------------------------
# Safe name split: "Evans, Fred"
# ------------------------------------------------
df = df.withColumn("name_split", split(col("name"), ", "))

df = df.withColumn("last_name", col("name_split")[0])
df = df.withColumn(
    "first_name",
    when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
)

# Remove invalid names
df = df.filter(col("first_name").isNotNull())

# ------------------------------------------------
# Convert numeric return fields to IntegerType
# ------------------------------------------------

for c in dash_cols:
    df = df.withColumn(c, regexp_replace(col(c), ",", ""))  # remove commas
    df = df.withColumn(c, col(c).cast(IntegerType()))

# ------------------------------------------------
# Remove rows where kick_returns_for_tds < 2 and punt_returns_for_tds < 2
# ------------------------------------------------
df = df.filter(col("kick_returns_for_tds").cast(IntegerType()) >= 1)
df = df.filter(col("punt_returns_for_tds").cast(IntegerType()) >= 1)
# ------------------------------------------------
# Remove rows before year 1970
# ------------------------------------------------
df = df.filter(col("year").cast(IntegerType()) >= 1970)

# ------------------------------------------------
# Drop unnecessary columns
# ------------------------------------------------
df = df.drop(
    "name",
    "name_split"
)

# ------------------------------------------------
# Debug Output
# ------------------------------------------------
print("Final row count:", df.count())
df.show(20, truncate=False)
df.printSchema()

# ------------------------------------------------
# Write Silver Output
# ------------------------------------------------
output_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_returns_output"
df.write.mode("overwrite").parquet(output_path)

spark.stop()