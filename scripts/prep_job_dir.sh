#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config

mkdir -p $DIR_ITERS $DIR_CONF $DIR_LIB
