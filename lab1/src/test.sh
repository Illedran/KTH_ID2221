#!/usr/bin/bash
set -e

rm -rf topten_classes
mkdir -p topten_classes
javac -cp $HADOOP_CLASSPATH -d topten_classes topten/TopTen.java
jar -cvf topten.jar -C topten_classes/ .
hadoop jar topten.jar topten.TopTen topten_input topten_output
