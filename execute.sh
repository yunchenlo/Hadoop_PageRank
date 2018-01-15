#!/bin/bash

# Do not uncomment these lines to directly execute the script
# Modify the path to fit your need before using this script
#hdfs dfs -rm -r /user/TA/CalculateAverage/Output/
#hadoop jar CalculateAverage.jar calculateAverage.CalculateAverage /user/shared/CalculateAverage/Input /user/TA/CalculateAverage/Output
#hdfs dfs -cat /user/TA/CalculateAverage/Output/part-*

if [[ "$1" == "" ]]; then
        iter=3
else
        iter=$1
fi

INPUT_FILE=testcase/input.txt
OUTPUT_FILE=PageRank/Output
JAR=PageRank.jar

hdfs dfs -rm -r $OUTPUT_FILE
hadoop jar $JAR pagerank.PageRank $INPUT_FILE $OUTPUT_FILE
hdfs dfs -getmerge $OUTPUT_FILE PageRank.txt
