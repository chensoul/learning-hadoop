#!/bin/sh

CLASS_PATH=classes:`echo lib/*.jar | tr ' ' ':'`

JAVA_OPTS=$JAVA_OPTS

OUTPUT=${1:-tmp}

JAVA_CMD="java $JAVA_OPTS -cp $CLASS_PATH com.embracesource.config.ZkConfigSaver $OUTPUT"

echo Trying to download configs using below command:
echo $JAVA_CMD

eval $JAVA_CMD
