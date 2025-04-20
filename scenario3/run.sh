#!/bin/bash
# run.sh

# Ensure Hadoop and Hive environment variables are set
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive-metastore
export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH

# Start Hive Metastore
echo "Starting Hive Metastore..."
schematool -dbType postgres -initSchema || echo "Schema already initialized or error occurred"
$HIVE_HOME/bin/hive --service metastore