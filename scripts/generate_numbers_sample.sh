#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config

for (( i=1; i < 1000; i++ )) do echo $i >> $NUMBERS/in/numbers-1k.txt; done


$HADOOP_BIN fs -put -f $NUMBERS/in/numbers-1k.txt $HDFS_DIR_IN/.

