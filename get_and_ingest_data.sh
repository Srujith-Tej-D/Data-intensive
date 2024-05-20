#!/bin/bash

# Function to check if directory exists in HDFS
check_hdfs_directory() {
    hdfs dfs -test -d $1
    return $?
}

# Create HDFS directory if it doesn't exist
if ! check_hdfs_directory /data-intensive; then
    echo "Creating HDFS directory /data-intensive"
    hdfs dfs -mkdir /data-intensive
fi

# Download air quality data
wget https://data.cityofnewyork.us/api/views/c3uy-2p5r/rows.csv?accessType=DOWNLOAD -O airquality.csv

# Download water quality data
wget https://data.cityofnewyork.us/api/views/bkwf-xfky/rows.csv?accessType=DOWNLOAD -O waterquality.csv

# Move downloaded files to HDFS
echo "Moving files to HDFS"
hdfs dfs -put airquality.csv /data-intensive
hdfs dfs -put waterquality.csv /data-intensive
