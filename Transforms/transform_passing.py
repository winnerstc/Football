from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, trim, when, size
from pyspark.sql.types import IntegerType, FloatType

# ------------------------------------------------
# Initialize Spark with Hive support
# ------------------------------------------------
spark = SparkSession.builder \
    .appName("NFLPassingToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------------------------
# Load Bronze Table from Hive
# ------------------------------------------------
spark.sql("USE joepostgres")
df = spark.sql("SELECT * FROM nfl_passing")

# ------------------------------------------------
# Trim all string columns
# ------------------------------------------------
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

# ------------------------------------------------
# Replace empty strings with NULL
# ------------------------------------------------
df = df.replace("", None)

# ------------------------------------------------
# Remove duplicates
# ------------------------------------------------
df = df.dropDuplicates()

# ------------------------------------------------
# Extract numeric player_id after slash (if any)
# fredevans/2513736  2513736
# ------------------------------------------------
df = df.withColumn(
    "player_id",
    regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
)

# ------------------------------------------------
# Safe split of name "Evans, Fred"
# ------------------------------------------------
df = df.withColumn("name_split", split(col("name"), ", "))
df = df.withColumn("last_name", col("name_split").getItem(0))
df = df.withColumn(
    "first_name",
    when(size(col("name_split")) > 1, col("name_split").getItem(1)).otherwise(None)
)
# Remove invalid names
df = df.filter(col("first_name").isNotNull())

# ------------------------------------------------
# Replace "--" with "0" in numeric fields
# ------------------------------------------------
dash_cols = [
    "passes_attempted",
    "passes_completed",
    "completion_percentage",
    "passing_yards",
    "passing_yards_per_attempt",
    "td_passes",
    "ints",
    "sacks"
]

for c in dash_cols:
    df = df.withColumn(c, when(col(c) == "--", "0").otherwise(col(c)))

# ------------------------------------------------
# Cast integer columns
# ------------------------------------------------
int_cols = [
    "passes_attempted",
    "passes_completed",
    "passing_yards",
    "td_passes",
    "ints"
]

for c in int_cols:
    df = df.withColumn(c, regexp_replace(col(c), ",", ""))
    df = df.withColumn(c, col(c).cast(IntegerType()))

# ------------------------------------------------
# Cast float columns
# ------------------------------------------------
float_cols = [
    "completion_percentage",
    "passing_yards_per_attempt",
    "sacks"
]

for c in float_cols:
    df = df.withColumn(c, col(c).cast(FloatType()))

# ------------------------------------------------
# Remove rows where passing_yards < 1000
# ------------------------------------------------
df = df.filter(col("passing_yards") >= 1000)

# ------------------------------------------------
# Remove rows before 1970
# ------------------------------------------------
df = df.filter(col("year").cast(IntegerType()) >= 1970)

# ------------------------------------------------
# Drop unwanted columns
# ------------------------------------------------
df = df.drop(
    "position",
    "pass_attempts_per_game",
    "passing_yards_per_game",
    "percentage_of_tds_per_attempts",
    "int_rate",
    "longest_pass",
    "passes_longer_than_20_yards",
    "passes_longer_than_40_yards",
    "sacked_yards_lost",
    "name",
    "name_split"
)

# ------------------------------------------------
# Write to Silver (Parquet) WITHOUT partitioning
# ------------------------------------------------
output_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_passing_output"
df.write.mode("overwrite").parquet(output_path)

# ------------------------------------------------
# Stop Spark
# ------------------------------------------------
spark.stop()