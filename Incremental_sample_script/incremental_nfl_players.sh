#!/bin/bash

# PostgreSQL Configuration
HOSTNAME='18.134.163.221'
DBNAME='testdb'
USERNAME='admin'
PASSWORD='admin123'
TABLE='nfl_players'

# Hadoop/HDFS Paths
TARGET_DIR="/tmp/DE011025/Joe/silver/nfl_players_output"

# Hive Configuration
HIVE_DATABASE="joepostgres"
HIVE_TABLE="nfl_players"

# HiveServer2 URL (FINAL)
HIVE_URL="jdbc:hive2://ip-172-31-3-80.eu-west-2.compute.internal:10000/${HIVE_DATABASE};"

echo "Setting HDFS permissions..."
sudo -u hdfs hdfs dfs -chmod -R 777 /tmp/DE011025/Joe/silver/

# Fetch MAX(player_id) from Hive
LAST_VALUE=$(beeline -u "${HIVE_URL}" --silent=true \
  -e "SELECT MAX(player_id) FROM ${HIVE_TABLE};" \
  | grep -o '[0-9]*' | tail -n 1)

# If empty table, use 0
if [ -z "$LAST_VALUE" ]; then
  LAST_VALUE=0
fi

echo "Last imported player_id: $LAST_VALUE"
echo "Beginning Sqoop incremental import..."

# Sqoop Import
sqoop import \
  --connect jdbc:postgresql://${HOSTNAME}:5432/${DBNAME} \
  --username ${USERNAME} \
  --password ${PASSWORD} \
  --table ${TABLE} \
  --incremental append \
  --check-column player_id \
  --last-value ${LAST_VALUE} \
  --target-dir ${TARGET_DIR} \
  --m 1 \
  --as-textfile

# Check if Sqoop succeeded
if [ $? -ne 0 ]; then
    echo "❌ Sqoop Incremental Import FAILED"
    exit 1
fi

echo "✔ Sqoop Import Successful."

# Count files inside target HDFS directory
FILE_COUNT=$(hdfs dfs -count ${TARGET_DIR} | awk '{print $2}')

if [ $FILE_COUNT -gt 0 ]; then
    echo "✔ New data detected in: ${TARGET_DIR}"
    echo "Loading data into Hive table ${HIVE_DATABASE}.${HIVE_TABLE}..."

    beeline -u "${HIVE_URL}" \
      -e "LOAD DATA INPATH '${TARGET_DIR}' INTO TABLE ${HIVE_DATABASE}.${HIVE_TABLE};"

    echo "✔ Data successfully loaded into Hive!"
else
    echo "ℹ No new data found in HDFS. Nothing to load into Hive."
fi
