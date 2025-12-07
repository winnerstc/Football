**Author:** Marc Hanna 
# Project Title
Kick Return Career Stats ETL Process
# Objective
This ETL process extracts NFL kick return career statistics from a CSV file and processes it through multiple stages:

1) Load the raw CSV into a database (DBeaver / PostgreSQL).

2) Import the table into HDFS using Sqoop.

3) Organize data into Raw → Silver → Gold layers in HDFS. **Silver:** Clean and standardize the data. **Gold:** Transform data into a Star Schema with fact and dimension tables. Compute high-value metrics for analysis.

# Database/App Flow
## Database:
1) Opened DBeaver and connected to the mhanna_db database on Host 18.134.163.221.

2) Created a _career_stats_kick_return_ table.

3) Used the “Import CSV File” command to load [the kick return stats football dataset](https://www.kaggle.com/datasets/kendallgillies/nflstatistics?select=Career_Stats_Kick_Return.csv) CSV into the _career_stats_kick_return_ table.

Example Columns:
player_id, last_name, first_name, middle_initial, year, team, games_played, fair_catches, returns, yards_returned, yards_per_return, returns_longer_20, returns_longer_40, returns_for_tds, fumbles, longest_return, position

## Importing with Sqoop
4) **Sqoop Import to HDFS**:

* **full load**:
```
 sqoop import --connect jdbc:postgresql://18.134.163.221:5432/mhanna_db --username Consultants --password WelcomeItc@2022 --table career_stats_kick_return --m 1 --target-dir /tmp/DE011025/marc/sqoop/career_stats_kick_return --fields-terminated-by ',' --lines-terminated-by '\n' --null-string '\\N' --null-non-string '\\N' --as-textfile
 ```

* **incremental load** (just for practice):
```
sqoop import --connect jdbc:postgresql://18.134.163.221:5432/mhanna_db --username Consultants --password WelcomeItc@2022 --table career_stats_kick_return --m 1 --target-dir /tmp/DE011025/marc/sqoop/career_stats_kick_return --fields-terminated-by ',' --lines-terminated-by '\n' --null-string '\\N' --null-non-string '\\N' --as-textfile --incremental append --check-column Player Id last-value 1000 
```

**Arguments**:

--m 1: Use a single mapper for initial import.

--target-dir: HDFS location for raw data.

**HDFS File path**: `/tmp/DE011025/marc/sqoop/career_stats_kick_return`

## Python Spark Script for ETL
5) 1. Grab CSV data from temporary raw location and Convert raw data to a Pyspark Dataframe.
   2. **raw-> Bronze:** Copy from temporary raw location to **Bronze location (CSV):** _hdfs:///tmp/de011025/marc/raw/staging_career_stats_kick_return_
6) Data Cleaning / **Bronze -> Silver Transformation**:

    1. **Clean Data:** Remove duplicates, Standardize string columns (trim, uppercase). Replace missing numeric values with 0.
    2. **Enrich data** for later analysis (e.g., compute derived columns: yards_per_return and long_return_flag).
    3. Write to **Silver location (Parquet):** _/tmp/de011025/name/Silver/silver_staging_clean_

7) **Silver -> Gold Transformation:** 
   1. Perform aggregation transformations on the cleaned kick_return_stats (fact table dataframe), and player, team and year
   (dimension table dataframes) 
   2. write to **Gold location (Parquet):** _/tmp/DE011025/marc/Gold/gold_fact_kick_return_stats_

# Spark Submit Details
This section documents exactly how I used `spark-submit` the PySpark ETL script was submitted on the cluster.

The ETL SCRIPT at this file location:
`/home/Consultants/Marc_Individual_Task_ETL_Project/marc_ETL_task.py`

ran to produce an outputs directory `outputs` with test results confirming that the bronze and silver folders have the same schemas (rows and columns), silver to gold having the same number of data rows, and showing a 15 row sample of the joined fact and dimension tables total data.
## yarn local
To **produce unit test results**, I used the `spark-submit` command:

`spark-submit marc_ETL_task.py 1`

along with SparkSession set to use all cores on my local machine:

```
spark = (
    SparkSession.builder
    .appName("KickReturnsLocal")
    .master("local[*]")
    .getOrCreate()
)
```
## yarn cluster
I also ran Spark in yarn-client mode just for practice:
```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name FootballKickReturnsApp \
  --num-executors 4 \
  --executor-memory 4G \
  --executor-cores 2 \
  /home/Consultants/Marc_Individual_Task_ETL_Project/marc_ETL_task.py
```
with SparkSession pointed to HDFS:
```
spark = (
    SparkSession.builder
    .appName("KickReturnsCluster")
    .config("hive.metastore.uris", "thrift://ip-172-31-8-235.eu-west-2.compute.internal:9083")
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020")
    .config("spark.hadoop.yarn.resourcemanager.address", "ip-172-31-3-80.eu-west-2.compute.internal:8032")
    .enableHiveSupport()
    .getOrCreate()
)
```


**Explanation of Key Parameters:**

`--master yarn` → run on Hadoop YARN

`--deploy-mode cluster` → driver runs on a YARN node

`--num-executors 4` → parallel workload distribution

`--executor-memory 4G` → memory per executor

`--executor-cores 2` → number of CPU cores per executor

`/home/Consultants/Marc_Individual_Task_ETL_Project/marc_ETL_task.py` → Python ETL script path

## Star Schema Design
### Dimension Tables:

**dim_player:**

Surrogate key: player_key

Attributes: player_id, first_name, last_name, position, career_span, rookie_flag, total_yards, total_returns

**dim_team:**

* Surrogate key: 

    team_key
* Attributes: 

    team_name

**dim_year**

* Surrogate key: 

    year_key
* Attributes: 

    year

### Fact Table:

**fact_kick_return_stats**

* Foreign Keys:

    player_key, team_key, year_key
* Attributes (from original data): 

    player_key, team_key, year_key, games_played, returns, yards_returned, yards_per_return, returns_longer_20, returns_longer_40, returns_for_tds, fumbles, longest_return

* Derived attributes: 

    long_return_flag (1 if >40 yards), success_rate_20plus, success_rate_40plus, turnover_risk, weighted_return_score, cumulative_yards

### Schema Diagram:

        dim_player      dim_team       dim_year
           |                |              |
           ---------------------------------
                            |
                        fact_kick_return_stats

## Unit Testing

Location: ./unit_tests/test_kick_returns_hive_tables.py

How to run tests locally:

`pytest ./unit_tests/test_kick_returns_hive_tables.py -v --junitxml=./outputs/test_results.xml
`

Uses PySpark fixtures for Hive tables and SparkSession.

### Validates:

* Schema consistency between Bronze → Silver → Gold.
* Row counts across tables.
* Basic data integrity and derived metrics.

Logs results to ./outputs/ for review.

GitHub Actions / CI Integration:

**Tests are fully compatible with Jenkins/GitHub Actions.**

Example workflow snippet:
jobs:
  etl-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: 3.10
      - name: Install dependencies
        run: pip install pyspark pytest
      - name: Run tests
        run: pytest ./unit_tests/test_kick_returns_hive_tables.py --junitxml=results.xml

## Major Improvements / Refactoring Notes (tasks done and can be improved in the same way)
* Reorganized ETL code into reusable functions: Table creation, cleaning, Silver → Gold transformations.
* Logging and output functions for portability.
* Improved testability / portability: SparkSession setup parameterized for local vs cluster mode.
* All code outputs are logged (./marc_ETL_task_output/) and unit tests (./outputs/); can easily be integrated into CI pipelines. 
### TODOS:
* - [ ] TODO: Improve visuals of logs and outputs
* - [ ] TODO: Organize more code into functions