#!/usr/bin/env bash
# Starts SSH and Hadoop (HDFS). Run this once each time you reopen the laptop.
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop}"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
sudo service ssh start
start-dfs.sh
echo
echo "Running Hadoop processes (expect NameNode, DataNode, SecondaryNameNode):"
jps | grep -v Jps
echo
echo "Hadoop web page: http://localhost:9870"
