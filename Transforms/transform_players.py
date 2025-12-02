from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, to_date, when, size, trim, lit
from pyspark.sql.types import IntegerType

# ------------------------------------------------
# 1. Create SparkSession with Hive support
# ------------------------------------------------
spark = SparkSession.builder \
    .appName("NFLPlayersToSilver") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------------------------
# 2. Load nfl_players from Hive database joepostgres
# ------------------------------------------------
spark.sql("USE joepostgres")
df = spark.sql("SELECT * FROM nfl_players")

# ------------------------------------------------
# 3. Trim whitespace from string columns
# ------------------------------------------------
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

# ------------------------------------------------
# 4. Convert empty strings to NULL
# ------------------------------------------------
df = df.replace("", None)

# ------------------------------------------------
# 5. Remove duplicates based on player_id and year
# ------------------------------------------------
df = df.dropDuplicates(["player_id", "years_played"])

# ------------------------------------------------
# 6. Clean player_id (remove prefix and cast to int)
# ------------------------------------------------

df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))

# ------------------------------------------------
# 7. Split name into first_name, last_name
# ------------------------------------------------
df = df.withColumn("name_split", split(col("name"), ",\\s*"))
df = df.withColumn("last_name", col("name_split").getItem(0))
df = df.withColumn("first_name", when(size(col("name_split")) > 1, col("name_split").getItem(1)).otherwise(None))

# ------------------------------------------------
# 8. Split birth_place into birth_city, birth_state
# ------------------------------------------------
df = df.withColumn("bp_split", split(col("birth_place"), ",\\s*"))
df = df.withColumn("birth_city", col("bp_split").getItem(0))
df = df.withColumn("birth_state", when(size(col("bp_split")) > 1, col("bp_split").getItem(1)).otherwise(None))

# ------------------------------------------------
# 9. Parse birthday to yyyy-MM-dd
# ------------------------------------------------
df = df.filter(col("birthday").rlike(r"^\d{1,2}/\d{1,2}/\d{2,4}$"))
df = df.withColumn("birthday", to_date(col("birthday"), "M/d/yyyy"))
df = df.filter(col("birthday").isNotNull())

# ------------------------------------------------
# 10. Drop raw/unneeded columns
# ------------------------------------------------
columns_to_drop = [
    "age", "height_inches", "high_school", "number", "position",
    "weight_lbs", "years_played", "birth_place", "name",
    "name_split", "bp_split"
]
df_silver = df.drop(*columns_to_drop)

# ------------------------------------------------
# 11. Write to HDFS as Parquet
# ------------------------------------------------
output_path = "hdfs:///tmp/DE011025/Joe/silver/nfl_players_output"
df_silver.write.mode("overwrite").parquet(output_path)

# ------------------------------------------------
# 12. Optional: show schema and preview
# ------------------------------------------------
df_silver.printSchema()
df_silver.show(20, truncate=False)

# ------------------------------------------------
# 13. Stop Spark
# ------------------------------------------------
spark.stop()