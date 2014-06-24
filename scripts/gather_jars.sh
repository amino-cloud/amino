#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config
cp $AMINO/amino-impl/database/accumulo/iterators/target/amino-accumulo-iterators-*-jar-with-dependencies.jar $DEST/iterators
cp $AMINO/amino-impl/database/accumulo/common/target/amino-accumulo-common-*-SNAPSHOT-job.jar $NUMBERS/lib
cp $AMINO/amino-impl/job/number/target/number-*-SNAPSHOT-job.jar $NUMBERS/lib
