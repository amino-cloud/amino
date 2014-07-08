#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config

$ERB_BIN $SCRIPT_DIR/AminoDefaults.xml.erb > $DIR_CONF/AminoDefaults.xml
cat $DIR_CONF/AminoDefaults.xml

$HADOOP_BIN fs -put -f $DIR_CONF/AminoDefaults.xml $HDFS_DIR_CONF/.
