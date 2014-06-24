#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config

export HADOOP_BIN=${HADOOP_BIN:-$HADOOP_HOME/bin/hadoop}


HELP_TEXT="Usage: $0 [--start-at JOB_NAME || --run-only JOB_NAME]\n
  JOB_NAME can be one of:\n
    NUMBERS_JOB\n
    DB_PREP_JOB\n
    BYBUCKET_JOB\n
    BITLOOKUP_JOB\n
    STATS_JOB\n
    HYPOTHESIS_JOB\n
    REVERSE_BITMAP_JOB\n
    REVERSE_FEATURE_LOOKUP_JOB\n
    FEATURE_METADATA_JOB\n
\n
  If you use --start-at it will run every job after the job name specified, including the job name specified.\n
  If you use --run-only it will only run the job that you specified.\n
  If you specify no arguments it will run all of the jobs including the NUMBERS_JOB\n
"


NUMBERS_JOB=1
DB_PREP_JOB=2
BYBUCKET_JOB=3
BITLOOKUP_JOB=4
STATS_JOB=5
HYPOTHESIS_JOB=6
REVERSE_BITMAP_JOB=7
REVERSE_FEATURE_LOOKUP_JOB=8
FEATURE_METADATA_JOB=9

if [ $# -gt 1 ]; then
    if [ "$1" = '--start-at' ]
    then
        START_AT=$2
        re='^[0-9]+$'
        if ! [[ ${!START_AT} =~ $re ]] ; then
           echo "error: Unknown JOB name $START_AT" >&2; exit 1
        fi
        NUMBERS_JOB=$(expr $NUMBERS_JOB - ${!START_AT} + 1)
        DB_PREP_JOB=$(expr $DB_PREP_JOB - ${!START_AT} + 1)
        BYBUCKET_JOB=$(expr $BYBUCKET_JOB - ${!START_AT} + 1)
        BITLOOKUP_JOB=$(expr $BITLOOKUP_JOB - ${!START_AT} + 1)
        STATS_JOB=$(expr $STATS_JOB - ${!START_AT} + 1)
        HYPOTHESIS_JOB=$(expr $HYPOTHESIS_JOB - ${!START_AT} + 1)
        REVERSE_BITMAP_JOB=$(expr $REVERSE_BITMAP_JOB - ${!START_AT} + 1)
        REVERSE_FEATURE_LOOKUP_JOB=$(expr $REVERSE_FEATURE_LOOKUP_JOB - ${!START_AT} + 1)
        FEATURE_METADATA_JOB=$(expr $FEATURE_METADATA_JOB - ${!START_AT} + 1)
    elif [ "$1" = '--run-only' ]
    then
        NUMBERS_JOB=0
        DB_PREP_JOB=0
        BYBUCKET_JOB=0
        BITLOOKUP_JOB=0
        STATS_JOB=0
        HYPOTHESIS_JOB=0
        REVERSE_BITMAP_JOB=0
        REVERSE_FEATURE_LOOKUP_JOB=0
        FEATURE_METADATA_JOB=0
	RUN_ONLY=$2
        printf -v $RUN_ONLY "1"
    else
       echo "Running all jobs"
    fi
elif [ $# -eq 1 ]; then
    if [ "$1" = '--help' ] || [ "$1" = "--usage" ]; then
      echo -e $HELP_TEXT
      exit
    fi
fi

([ $NUMBERS_JOB -lt 1 ] || $HADOOP_BIN jar $NUMBERS_JOB_JAR $AMINO_ROOT_PACKAGE.api.framework.FrameworkDriver --amino_default_config_path $HDFS_DIR_CONF) &&
([ $DB_PREP_JOB -lt 1 ] || $HADOOP_BIN jar $AMINO_JOB_JAR $AMINO_BITMAP_PACKAGE.DatabasePrepJob $HDFS_DIR_OUT $HDFS_DIR_CONF) &&
([ $BYBUCKET_JOB -lt 1 ] || $HADOOP_BIN jar $AMINO_JOB_JAR $AMINO_BITMAP_PACKAGE.ByBucketJob $HDFS_DIR_OUT $HDFS_DIR_CONF $HDFS_DIR_WORKING) &&
([ $BITLOOKUP_JOB -lt 1 ] || $HADOOP_BIN jar $AMINO_JOB_JAR $AMINO_BITMAP_PACKAGE.BitLookupJob $HDFS_DIR_OUT $HDFS_DIR_CONF $HDFS_DIR_WORKING) &&
([ $STATS_JOB -lt 1 ] || $HADOOP_BIN jar $AMINO_JOB_JAR $AMINO_BITMAP_PACKAGE.StatsJob $HDFS_DIR_OUT $HDFS_DIR_CONF) &&
([ $HYPOTHESIS_JOB -lt 1 ] || $HADOOP_BIN jar $AMINO_JOB_JAR $AMINO_BITMAP_PACKAGE.HypothesisJob $HDFS_DIR_OUT $HDFS_DIR_CONF $HDFS_DIR_WORKING) &&
([ $REVERSE_BITMAP_JOB -lt 1 ] || $HADOOP_BIN jar $AMINO_JOB_JAR $AMINO_BITMAP_PACKAGE.reverse.ReverseBitmapJob $HDFS_DIR_OUT $HDFS_DIR_CONF) &&
([ $REVERSE_FEATURE_LOOKUP_JOB -lt 1 ] || $HADOOP_BIN jar $AMINO_JOB_JAR $AMINO_BITMAP_PACKAGE.reverse.ReverseFeatureLookupJob $HDFS_DIR_OUT $HDFS_DIR_CONF $HDFS_DIR_WORKING) &&
([ $FEATURE_METADATA_JOB -lt 1 ] || $HADOOP_BIN jar $AMINO_JOB_JAR $AMINO_BITMAP_PACKAGE.FeatureMetadataJob $HDFS_DIR_CONF)
