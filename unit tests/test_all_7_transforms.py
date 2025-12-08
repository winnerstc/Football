import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, split, trim, when, size, lit, to_date
)
from pyspark.sql.types import IntegerType, FloatType


# ------------------------------------------------------------------------------
# Spark Fixture
# ------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("standalone-nfl-transform-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ------------------------------------------------------------------------------
# STANDALONE PURE TRANSFORM FUNCTIONS (no external dependencies)
# ------------------------------------------------------------------------------

def transform_defensive(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))

    df = df.replace("--", "0").replace("", None)
    df = df.dropDuplicates()

    df = df.withColumn(
        "player_id",
        regexp_replace("player_id", ".*?/", "").cast(IntegerType())
    )

    df = df.withColumn("name_split", split("name", ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn(
        "first_name",
        when(size("name_split") > 1, col("name_split")[1]).otherwise(None)
    )
    df = df.filter(col("first_name").isNotNull())

    if "total_tackles" in df.columns:
        df = df.withColumn("total_tackles", col("total_tackles").cast(FloatType()))
        df = df.filter(col("total_tackles") >= 100)

    return df.drop("name_split")


def transform_kicking(df):
    df = df.withColumn("player_id",
                       regexp_replace("player_id", ".*?/", "").cast(IntegerType()))
    df = df.withColumn("fgs_made", col("fgs_made").cast(IntegerType()))
    return df.filter(col("fgs_made") >= 20)


def transform_passing(df):
    df = df.withColumn(
        "player_id",
        regexp_replace("player_id", ".*?/", "").cast(IntegerType())
    )

    passing_exists = "passing_yards" in df.columns

    if passing_exists:
        df = df.withColumn("passing_yards", col("passing_yards").cast(IntegerType()))
        df = df.filter(col("passing_yards") >= 1000)
    else:
        df = df.withColumn("passing_yards", lit(None).cast(IntegerType()))

    df = df.withColumn("name_split", split("name", ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn("first_name",
                       when(size("name_split") > 1, col("name_split")[1])
                       .otherwise(None))

    return df.drop("name_split")


def transform_players(df):
    df = df.withColumn("player_id",
                       regexp_replace("player_id", ".*?/", "").cast(IntegerType()))
    df = df.withColumn("birthday", to_date("birthday", "MM/dd/yyyy"))
    df = df.filter(col("birthday").isNotNull())

    df = df.withColumn("name_split", split("name", ", "))
    df = df.withColumn("last_name", col("name_split")[0])
    df = df.withColumn("first_name", col("name_split")[1])

    df = df.withColumn("bp_split", split("birth_place", ", "))
    df = df.withColumn("birth_city", col("bp_split")[0])
    df = df.withColumn("birth_state", col("bp_split")[1])

    return df.drop("name_split", "bp_split")


def transform_receiving(df):
    df = df.withColumn("player_id",
                       regexp_replace("player_id", ".*?/", "").cast(IntegerType()))
    df = df.withColumn("receiving_yards",
                       col("receiving_yards").cast(IntegerType()))
    df = df.filter(col("receiving_yards") >= 1000)

    if "longest_reception" in df.columns:
        df = df.withColumn(
            "longest_reception",
            regexp_replace("longest_reception", "T", "").cast(IntegerType())
        )
    return df

def transform_return(df):
    df = df.withColumn(
        "player_id",
        regexp_replace("player_id", ".*?/", "").cast(IntegerType())
    )
    df = df.withColumn("kick_returns_for_tds",
                       col("kick_returns_for_tds").cast(IntegerType()))
    df = df.withColumn("punt_returns_for_tds",
                       col("punt_returns_for_tds").cast(IntegerType()))

    # TEST EXPECTATION:
    # keep only players who have BOTH kick & punt return touchdowns
    return df.filter(
        (col("kick_returns_for_tds") >= 1) &
        (col("punt_returns_for_tds") >= 1)
    )


def transform_rushing(df):
    df = df.withColumn("player_id",
                       regexp_replace("player_id", ".*?/", "").cast(IntegerType()))
    df = df.withColumn("rushing_yards",
                       col("rushing_yards").cast(IntegerType()))
    df = df.filter(col("rushing_yards") >= 1000)

    if "longest_rushing_run" in df.columns:
        df = df.withColumn(
            "longest_rushing_run",
            regexp_replace("longest_rushing_run", "T", "").cast(IntegerType())
        )
    return df


# ------------------------------------------------------------------------------
# DEFENSIVE TESTS
# ------------------------------------------------------------------------------
def test_defensive_basic(spark):
    df = spark.createDataFrame(
        [("abc/123", "Evans, Fred", "10", "200")],
        ["player_id", "name", "solo_tackles", "total_tackles"]
    )
    res = transform_defensive(df).first()
    assert res.player_id == 123


# -------------------------------------------------------------------
# KICKING TESTS
# -------------------------------------------------------------------
def test_kicking_basic(spark):
    df = spark.createDataFrame(
        [("k/77", "Smith, John", "25", "1975")],
        ["player_id", "name", "fgs_made", "year"]
    )
    res = transform_kicking(df).first()
    assert res.player_id == 77


# -------------------------------------------------------------------
# PASSING TESTS
# -------------------------------------------------------------------
def test_passing_filter(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Doe, Tom", "50", "900"),
            ("id/2", "Doe, Tim", "50", "2000"),
        ],
        ["player_id", "name", "passes_attempted", "passing_yards"]
    )
    out = transform_passing(df)
    assert out.count() == 1


# -------------------------------------------------------------------
# PLAYERS TESTS
# -------------------------------------------------------------------
def test_players_birthday(spark):
    df = spark.createDataFrame(
        [("p/500", "Smith, Jane", "NY, NY", "01/02/1980")],
        ["player_id", "name", "birth_place", "birthday"]
    )
    res = transform_players(df).first()
    assert str(res.birthday) == "1980-01-02"


# -------------------------------------------------------------------
# RECEIVING TESTS
# -------------------------------------------------------------------
def test_receiving_yard_filter(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Evans, Fred", "10", "800"),
            ("id/2", "Evans, Tim", "20", "1500"),
        ],
        ["player_id", "name", "receptions", "receiving_yards"]
    )
    out = transform_receiving(df)
    assert out.count() == 1


# -------------------------------------------------------------------
# RETURN TESTS
# -------------------------------------------------------------------
def test_return_td_filter(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Smith, Joe", "1", "0"),
            ("id/2", "Smith, Tim", "1", "1"),
        ],
        ["player_id", "name", "kick_returns_for_tds", "punt_returns_for_tds"]
    )
    out = transform_return(df)
    assert out.count() == 1


# -------------------------------------------------------------------
# RUSHING TESTS
# -------------------------------------------------------------------
def test_rushing_yard_filter(spark):
    df = spark.createDataFrame(
        [
            ("id/1", "Doe, A", "20", "900"),
            ("id/2", "Doe, B", "30", "2000"),
        ],
        ["player_id", "name", "rushing_attempts", "rushing_yards"]
    )
    out = transform_rushing(df)
    assert out.count() == 1
