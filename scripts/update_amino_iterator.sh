#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config

$HADOOP_BIN fs -put -f $DIR_ITERS/*.jar $HDFS_ITER_DIR/.
