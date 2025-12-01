from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, to_date, when, size, trim, max as spark_max
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("PlayersIncremental").getOrCreate()

raw_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/Joe/raw/nfl_players/"
silver_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/Joe/silver/nfl_players_output/"

# High Watermark
try:
    silver_df = spark.read.parquet(silver_path)
    watermark = silver_df.agg(spark_max("player_id")).collect()[0][0] or 0
    print("Watermark:", watermark)
except:
    watermark = 0

# Read raw
df = spark.read.option("header", True).parquet(raw_path)

# Clean ID before filter
df = df.withColumn(
    "player_id",
    regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
)

df = df.filter(col("player_id") > watermark)
print("New records:", df.count())

if df.count() == 0:
    spark.stop()
    exit()

# Transformations (same as full-load)
for c, t in df.dtypes:
    if t == "string":
        df = df.withColumn(c, trim(col(c)))

df = df.replace("", None).dropDuplicates()

df = df.withColumn("name_split", split(col("name"), ", "))
df = df.withColumn("last_name", col("name_split").getItem(0))
df = df.withColumn(
    "first_name",
    when(size(col("name_split")) > 1, col("name_split").getItem(1)).otherwise(None)
)

df = df.withColumn("bp_split", split(col("birth_place"), ", "))
df = df.withColumn("birth_city", col("bp_split").getItem(0))
df = df.withColumn(
    "birth_state",
    when(size(col("bp_split")) > 1, col("bp_split").getItem(1)).otherwise(None)
)

df = df.filter(col("birthday").rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"))
df = df.withColumn("birthday", to_date(col("birthday"), "M/d/yyyy"))
df = df.filter(col("birthday").isNotNull())

df = df.drop(
    "age","height_inches","high_school","number","position",
    "weight_lbs","years_played","birth_place","name",
    "name_split","bp_split"
)

df.write.mode("append").parquet(silver_path)
spark.stop()
