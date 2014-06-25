#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPT_DIR/config

echo -n "Accumulo Username: "
read accumulo_user
echo -n "Accumulo password: "
read -s accumulo_password

erb $SCRIPT_DIR/accumulo_iterator_install.erb | $ACCUMULO_BIN shell -u $accumulo_user -p $accumulo_password 
