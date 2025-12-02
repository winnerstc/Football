import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-test") \
        .getOrCreate()


# ======================================================
# DEFENSIVE TRANSFORM + EXTRA TESTS
# ======================================================

def defensive_transform(df):
    df = df.replace("--", "0").replace("", None)

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.dropDuplicates()

    df = df.withColumn(
        "player_id",
        regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType())
    )

    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn("first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
    )
    df = df.filter(col("first_name").isNotNull())

    df = df.withColumn("sacks", col("sacks").cast(FloatType()))
    df = df.withColumn("total_tackles", col("total_tackles").cast(FloatType()))

    df = df.filter(col("total_tackles") >= 100)

    return df


def test_defensive_valid(spark):
    data = [("abc/123", "Doe, John", "120", "7.0")]
    df = spark.createDataFrame(data, ["player_id", "name", "total_tackles", "sacks"])
    r = defensive_transform(df).collect()[0]
    assert r["player_id"] == 123


def test_defensive_remove_low_tackles(spark):
    data = [("abc/123", "Doe, John", "50", "7.0")]
    df = spark.createDataFrame(data, ["player_id", "name", "total_tackles", "sacks"])
    assert defensive_transform(df).count() == 0


def test_defensive_trim_spaces(spark):
    data = [("abc/123", "  Doe, John ", "120", "7.0")]
    df = spark.createDataFrame(data, ["player_id", "name", "total_tackles", "sacks"])
    r = defensive_transform(df).collect()[0]
    assert r["last_name"] == "Doe"


def test_defensive_invalid_name(spark):
    data = [("abc/123", "Doe", "120", "7.0")]
    df = spark.createDataFrame(data, ["player_id", "name", "total_tackles", "sacks"])
    assert defensive_transform(df).count() == 0


def test_defensive_null_drop(spark):
    data = [("abc/123", None, "120", "7.0")]
    df = spark.createDataFrame(data, ["player_id", "name", "total_tackles", "sacks"])
    assert defensive_transform(df).count() == 0


# ======================================================
# KICKING TRANSFORM + EXTRA TESTS
# ======================================================

def kicking_transform(df):
    df = df.replace("--", "0").replace("", None)

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))

    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None)
    )
    df = df.filter(col("first_name").isNotNull())

    df = df.withColumn("fgs_made", regexp_replace(col("fgs_made"), ",", "").cast(IntegerType()))
    df = df.filter(col("fgs_made") >= 20)

    return df


def test_kicking_valid(spark):
    df = spark.createDataFrame([("kick/55", "Smith, Adam", "22")],
                               ["player_id", "name", "fgs_made"])
    r = kicking_transform(df).collect()[0]
    assert r["player_id"] == 55


def test_kicking_remove_under20(spark):
    df = spark.createDataFrame([("kick/55", "Smith, Adam", "10")],
                               ["player_id", "name", "fgs_made"])
    assert kicking_transform(df).count() == 0


def test_kicking_strip_spaces(spark):
    df = spark.createDataFrame([("kick/55", " Smith, Adam ", "22")],
                               ["player_id", "name", "fgs_made"])
    r = kicking_transform(df).collect()[0]
    assert r["first_name"] == "Adam"


def test_kicking_invalid_name(spark):
    df = spark.createDataFrame([("kick/55", "Smith", "22")],
                               ["player_id", "name", "fgs_made"])
    assert kicking_transform(df).count() == 0


# ======================================================
# PASSING TRANSFORM + EXTRA TESTS
# ======================================================

def passing_transform(df):
    df = df.replace("--", "0").replace("", None)
    df = df.dropDuplicates()

    df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))

    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None))
    df = df.filter(col("first_name").isNotNull())

    df = df.withColumn("passing_yards", regexp_replace(col("passing_yards"), ",", "")
                       .cast(IntegerType()))
    df = df.filter(col("passing_yards") >= 1000)

    return df


def test_passing_valid(spark):
    df = spark.createDataFrame([("abc/900", "Brady, Tom", "1500")],
                               ["player_id", "name", "passing_yards"])
    r = passing_transform(df).collect()[0]
    assert r["passing_yards"] == 1500


def test_passing_low_yards(spark):
    df = spark.createDataFrame([("abc/900", "Brady, Tom", "500")],
                               ["player_id", "name", "passing_yards"])
    assert passing_transform(df).count() == 0


def test_passing_comma_yards(spark):
    df = spark.createDataFrame([("abc/900", "Brady, Tom", "1,500")],
                               ["player_id", "name", "passing_yards"])
    r = passing_transform(df).collect()[0]
    assert r["passing_yards"] == 1500


def test_passing_invalid_name(spark):
    df = spark.createDataFrame([("abc/900", "Brady", "1500")],
                               ["player_id", "name", "passing_yards"])
    assert passing_transform(df).count() == 0


# ======================================================
# PLAYERS TRANSFORM + EXTRA TESTS
# ======================================================

def players_transform(df):
    df = df.replace("", None)
    df = df.dropDuplicates(["player_id", "years_played"])

    df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))
    df = df.withColumn("name_split", split(col("name"), ", "))
    df = df.withColumn("first_name",
        when(size(col("name_split")) > 1, col("name_split")[1]).otherwise(None))
    df = df.filter(col("first_name").isNotNull())

    return df


def test_players_valid(spark):
    df = spark.createDataFrame([("xyz/77", "Moss, Randy", "10")],
                               ["player_id", "name", "years_played"])
    r = players_transform(df).collect()[0]
    assert r["player_id"] == 77


def test_players_duplicate_removal(spark):
    df = spark.createDataFrame([
        ("xyz/77", "Moss, Randy", "10"),
        ("xyz/77", "Moss, Randy", "10")
    ], ["player_id", "name", "years_played"])
    assert players_transform(df).count() == 1


def test_players_invalid_name(spark):
    df = spark.createDataFrame([("xyz/77", "Moss", "10")],
                               ["player_id", "name", "years_played"])
    assert players_transform(df).count() == 0


# ======================================================
# RECEIVING TRANSFORM + EXTRA TESTS
# ======================================================

def receiving_transform(df):
    df = df.replace("--", "0").replace("", None)
    df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))
    df = df.withColumn("receiving_yards", col("receiving_yards").cast(IntegerType()))
    df = df.filter(col("receiving_yards") >= 1000)
    return df


def test_receiving_valid(spark):
    df = spark.createDataFrame([("rec/88", "Rice, Jerry", "1100")],
                               ["player_id", "name", "receiving_yards"])
    r = receiving_transform(df).collect()[0]
    assert r["player_id"] == 88


def test_receiving_low_yards(spark):
    df = spark.createDataFrame([("rec/88", "Rice, Jerry", "300")],
                               ["player_id", "name", "receiving_yards"])
    assert receiving_transform(df).count() == 0


def test_receiving_numeric_cast(spark):
    df = spark.createDataFrame([("rec/88", "Rice, Jerry", "1,500")],
                               ["player_id", "name", "receiving_yards"])
    r = receiving_transform(df).collect()[0]
    assert r["receiving_yards"] == 1500


# ======================================================
# RETURN TRANSFORM + EXTRA TESTS
# ======================================================

def return_transform(df):
    df = df.replace("--", "0").replace("", None)
    df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))
    df = df.withColumn("kick_returns_for_tds", col("kick_returns_for_tds").cast(IntegerType()))
    df = df.filter(col("kick_returns_for_tds") >= 1)
    return df


def test_return_valid(spark):
    df = spark.createDataFrame([("ret/99", "Hester, Devin", "1")],
                               ["player_id", "name", "kick_returns_for_tds"])
    r = return_transform(df).collect()[0]
    assert r["player_id"] == 99


def test_return_zero_tds(spark):
    df = spark.createDataFrame([("ret/99", "Hester, Devin", "0")],
                               ["player_id", "name", "kick_returns_for_tds"])
    assert return_transform(df).count() == 0


def test_return_invalid_name(spark):
    df = spark.createDataFrame([("ret/99", "Hester", "1")],
                               ["player_id", "name", "kick_returns_for_tds"])
    assert return_transform(df).count() == 0


# ======================================================
# RUSHING TRANSFORM + EXTRA TESTS
# ======================================================

def rushing_transform(df):
    df = df.replace("--", "0").replace("", None)
    df = df.withColumn("player_id", regexp_replace(col("player_id"), ".*?/", "").cast(IntegerType()))
    df = df.withColumn("rushing_yards", regexp_replace(col("rushing_yards"), ",", "").cast(IntegerType()))
    df = df.filter(col("rushing_yards") >= 1000)
    return df


def test_rushing_valid(spark):
    df = spark.createDataFrame([("run/34", "Walker, Herschel", "1500")],
                               ["player_id", "name", "rushing_yards"])
    r = rushing_transform(df).collect()[0]
    assert r["player_id"] == 34


def test_rushing_low_yards(spark):
    df = spark.createDataFrame([("run/34", "Walker, Herschel", "200")],
                               ["player_id", "name", "rushing_yards"])
    assert rushing_transform(df).count() == 0


def test_rushing_comma_values(spark):
    df = spark.createDataFrame([("run/34", "Walker, Herschel", "1,200")],
                               ["player_id", "name", "rushing_yards"])
    r = rushing_transform(df).collect()[0]
    assert r["rushing_yards"] == 1200
