#!/bin/bash

FLINK_DIR="/home/samza/workspace/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-related/flink-testbed"

# configure parameters in flink bin
function configFlink() {
    sed 's/^\(\s*streamswitch.requirement.latency\s*:\s*\).*/\1'"$L"'/' $FLINK_DIR/conf/flink-conf.yaml > tmp
    sed 's/^\(\s*streamswitch.system.alpha\s*:\s*\).*/\1'"$ALPHA"'/' tmp > $FLINK_DIR/conf/flink-conf.yaml
    rm tmp
}

# run flink clsuter
function runFlink() {
    mkdir log
    $FLINK_DIR/bin/start-cluster.sh
}

# run applications
function runApp() {
    $FLINK_APP_DIR/submitlocal.sh 16 64 0
}

# clsoe flink clsuter
function closeFlink() {
    echo "experiment finished closing it"
    $FLINK_DIR/bin/stop-cluster.sh
    mv $FLINK_DIR/log $FLINK_DIR/log."L"$L"l"$l
    echo "close finished"
}

for L in 1000 4000 16000; do
    for l in 50 100 150 200; do
        ALPHA=`echo "$l $L" | awk '{printf "%.5f \n", $1/$2}'`
        echo run experment with L = $L, ALPHA = $ALPHA
        configFlink
        runFlink
        runApp
        python -c 'import time; time.sleep(300)'

        closeFlink
        python -c 'import time; time.sleep(30)'
    done
done