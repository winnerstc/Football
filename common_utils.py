from pyspark.sql.functions import col, trim, regexp_replace, when
from pyspark.sql.types import IntegerType, FloatType

def trim_all(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    return df

def replace_dashes(df, cols):
    return df.na.replace("--", "0", subset=cols)

def safe_int(df, cols):
    for c in cols:
        df = df.withColumn(c, regexp_replace(col(c), ",", "")) \
               .withColumn(c, when(col(c) == "", None).otherwise(col(c))) \
               .withColumn(c, col(c).cast(IntegerType()))
    return df

def safe_float(df, cols):
    for c in cols:
        df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c))) \
               .withColumn(c, col(c).cast(FloatType()))
    return df