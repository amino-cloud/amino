#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config

for (( i=1; i < 1000; i++ )) do echo $i >> numbers-1k.txt; done
