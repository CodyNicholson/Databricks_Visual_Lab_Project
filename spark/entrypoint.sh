#!/bin/bash
# Run the Spark Connect server in the foreground so Docker sees it.
export SPARK_NO_DAEMONIZE=1
umask 0000
exec /opt/spark/sbin/start-connect-server.sh "$@"
