from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ViewParquet").getOrCreate()

#df = spark.read.parquet("silver/nfl_players")
df = spark.read.parquet("silver/nfl_passing")
df.show(500, truncate=False)
df.printSchema()

spark.stop()