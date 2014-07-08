#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $SCRIPT_DIR
source $SCRIPT_DIR/config

mkdir -p $DIR_ITERS $DIR_CONF $DIR_LIB $NUMBERS/in
$HADOOP_BIN fs -mkdir -p $HDFS_DIR_OUT $HDFS_DIR_IN $HDFS_DIR_CONF $HDFS_DIR_WORKING $HDFS_ITER_DIR
