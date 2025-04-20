#!/bin/bash

# A simple data collection script
LOG_DIR="/tmp/data_logs"
LOG_FILE="$LOG_DIR/data_log_$(date +%Y%m%d).txt"

# Create log directory if it doesn't exist
mkdir -p $LOG_DIR

# Append timestamp and system metrics to log file
echo "======== $(date) ========" >> $LOG_FILE
echo "CPU Usage:" >> $LOG_FILE
top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1"%"}' >> $LOG_FILE
echo "Memory Usage:" >> $LOG_FILE
free -m | grep Mem | awk '{print $3"/"$2" MB ("$3*100/$2"%)"}'  >> $LOG_FILE
echo "Disk Usage:" >> $LOG_FILE
df -h / | grep -v Filesystem | awk '{print $5 " of " $2}' >> $LOG_FILE
echo "" >> $LOG_FILE

echo "Data logged to $LOG_FILE"
