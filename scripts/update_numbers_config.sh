#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config

$ERB_BIN $SCRIPT_DIR/NumberLoader.xml.erb > $DIR_CONF/NumberLoader.xml
cat $DIR_CONF/NumberLoader.xml

$HADOOP_BIN fs -put -f $DIR_CONF/NumberLoader.xml  $HDFS_DIR_CONF/.
