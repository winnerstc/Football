#!/bin/bash

# Assign arguments to variables for clearer reference
TABLE="nfl_players"
HIVE_DATABASE="joepostgres"
HIVE_TABLE="nfl_players"

# Variables
HOSTNAME='18.134.163.221'
DBNAME='testdb'
USERNAME='admin'
PASSWORD='admin123'
TARGET_DIR="/tmp/DE011025/Joe/silver/nfl_players_output" # Modified to include user in the path
HIVE_URL="jdbc:hive2://ip-172-31-14-3.eu-west-2.compute.internal:10000/${HIVE_DATABASE};"

sudo -u hdfs hdfs dfs -chmod -R 777 /tmp/DE011025/Joe/silver/
sudo -u hdfs hdfs dfs -chmod -R 777 /tmp/DE011025/Joe/silver/*

# Fetch the maximum id value from Hive
LAST_VALUE=$(beeline -u "${HIVE_URL}" --silent=true -e "SELECT MAX(player_id) FROM ${HIVE_TABLE};" | grep -o '[0-9]*' | tail -n 1)

echo "Last recorded ID: $LAST_VALUE"
echo "Starting new import from ID greater than $LAST_VALUE"

# Perform the incremental Sqoop import
sqoop import \
    --connect jdbc:postgresql://${HOSTNAME}:5432/${DBNAME} \
    --username ${USERNAME} \
    --password ${PASSWORD} \
    --table ${TABLE} \
    --incremental append \
    --check-column Employee_ID \
    --last-value ${LAST_VALUE} \
    --target-dir ${TARGET_DIR} \
    --m 1 \
    --as-textfile

# Check if the Sqoop import was successful
if [ $? -eq 0 ]; then
    echo "Sqoop Incremental Import Successful"

    # Check if the HDFS directory has data
    HDFS_COUNT=$(hdfs dfs -count ${TARGET_DIR} | awk '{print $1}')
    if [ $HDFS_COUNT -gt 0 ]; then
        echo "New data found in HDFS directory: ${TARGET_DIR}"

        # Load new data into Hive table
        echo "Loading new data into Hive table: ${HIVE_DATABASE}.${HIVE_TABLE}"
        beeline -u "${HIVE_URL}" \
          -e "LOAD DATA INPATH '${HDFS_DIR}' INTO TABLE ${HIVE_DATABASE}.${HIVE_TABLE};"

        echo "Data successfully loaded into Hive table: ${HIVE_DATABASE}.${HIVE_TABLE}"
    else
        echo "No new data found in HDFS directory: ${TARGET_DIR}"
    fi
else
    echo "Sqoop Incremental Import Failed"
    exit 1
fi