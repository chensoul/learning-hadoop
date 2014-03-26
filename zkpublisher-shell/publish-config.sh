#!/bin/sh

CLASS_PATH=classes:`echo lib/*.jar | tr ' ' ':'`

JAVA_OPTS=$JAVA_OPTS

JAVA_CMD="java $JAVA_OPTS -cp $CLASS_PATH com.embracesource.config.ZkConfigPublisher"

echo Trying to start publish zk configs using below command:
echo $JAVA_CMD

eval $JAVA_CMD
