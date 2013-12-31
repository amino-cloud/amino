#!/bin/sh

INCLUDE_DIRS="../../../../amino-common/src/main/thrift"

for i in *.thrift
do
	thrift -I $INCLUDE_DIRS -gen java $i
	[ $? -ne 0 ] && exit $?
	echo "Compiled $i"
done
cp -R gen-java/* ../java/
rm -rf gen-java
