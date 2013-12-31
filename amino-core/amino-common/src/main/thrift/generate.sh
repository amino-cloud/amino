#!/bin/sh

for i in *.thrift
do
	thrift -gen java $i
	[ $? -ne 0 ] && exit $?
	echo "Compiled $i"
done
cp -R gen-java/* ../java/
rm -rf gen-java
