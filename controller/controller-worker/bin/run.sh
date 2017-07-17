#!/bin/bash

##########################################################################################
#
# Copyright 2017 Pinterest, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# 1. The default run.sh and property files are provided mainly for development and demo purpose.
#    It is highly recommended to create your own run.sh and property files for production use,
#    so that you can fine tune JVM options and customize server configs.
#
# 2. The default run.sh provide an simple but portable way of running Rocksplicator Controller
#    in background. It is highly recommended to use your favorite tools to daemonize it properly,
#    such as use start-stop-daemon, init, runsv (from runit), upstart, systemd, and etc.
#    Also, use monit, supervisor or other process monitoring tools to monitor your process.
#
# Usage example:
#
# 1. run controller-worker in foreground
# $ sh run.sh run
#
# 2. run controller-worker in background
# $ sh run.sh start
#
# 3. stop controller running in background
# $ sh run.sh stop
#
# 4. restart controller and run in background
# $ sh run.sh restart
#
##########################################################################################

# Specify all the defaults
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$(dirname ${DIR})"
TARGET_DIR="${ROOT_DIR}/target"
CP=${ROOT_DIR}/*:${ROOT_DIR}/lib/*:${TARGET_DIR}/classes:${TARGET_DIR}/lib/*
PID_FILE=${HOME}/rocksplicator-controller-worker.pid
ACTION="run"

LOG_PROPERTIES=${ROOT_DIR}/config/logback.xml
WORKER_CONFIG=${ROOT_DIR}/config/controller.worker.properties

WORKER_OPTS="-server -Xmx1024m -Xms1024m \
-verbosegc -Xloggc:/tmp/gc.log -XX:ErrorFile=/tmp/jvm_error.log \
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+UseParNewGC \
-XX:ConcGCThreads=7 -XX:ParallelGCThreads=7"
JAVA_CMD="java"
if [ "x$JAVA_HOME" != "x" ]; then JAVA_CMD=${JAVA_HOME}/bin/java; fi

display_usage() {
	echo -e "Usage: $0 [OPTIONS] [run|start|stop|restart]"
	echo -e "\nOPTIONS:"
	echo -e "  -c/--config     Config file, default is ${WORKER_CONFIG}"
	echo -e "  -i/--pid        PID file, default is ${PID_FILE}"
	echo -e "  -p/--class-path Class path, default is ${CP}"
	echo -e "  -o/--worker-opts  JVM and other options, default is ${WORKER_OPTS}"
	echo -e "\nACTION:"
	echo -e "  run     Run service in foreground. This is the default action."
	echo -e "  start   Run service in background."
	echo -e "  stop    Stop the service running in background."
	echo -e "  restart Restart the service running in background."
}

function server_start {
    echo "Starting Rocksplicator Controller Worker..."

    OPTS="${WORKER_OPTS} \
    -cp ${CP} -Dlogback.configurationFile=${LOG_PROPERTIES} -Dworker_config=${WORKER_CONFIG} \
    com.pinterest.rocksplicator.controller.WorkerService"

    if [ "$1" == "FOREGROUND" ]
    then
        ${JAVA_CMD} ${OPTS}
    else
        ${JAVA_CMD} ${OPTS} &
        echo $! > ${PID_FILE}
        echo "Rocksplicator controller worker started."
    fi
}

function server_stop {
    kill -TERM $(cat ${PID_FILE})
    rm -fr ${PID_FILE}
    echo "Rocksplicator controller worker stopped."
}

function action {
    case "$1" in

    run)
    server_start "FOREGROUND"
    ;;

    start)
    server_start "BACKGROUND"
    ;;

    stop)
    server_stop
    ;;

    restart)
    server_stop
    server_start
    ;;

    esac
}

while [[ $# > 0 ]]
do
key="$1"

case $key in
    -c|--config)
    CONFIG_FILE="$2"
    shift # past argument
    ;;
    -p|--class-path)
    CP="$2"
    shift # past argument
    ;;
    -o|--java-opts)
    WORKER_OPTS="$2"
    shift # past argument
    ;;
    -p|--pid)
    PID_FILE="$2"
    shift # past argument
    ;;
    -h|--help)
    display_usage
	exit 0
	;;
    run|start|stop|restart)
    ACTION="$1"
    ;;
    *)
    display_usage
	exit 1
    ;;
esac
shift # past argument or value
done

action ${ACTION}

exit 0