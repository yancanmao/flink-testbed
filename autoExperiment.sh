#!/bin/bash

FLINK_DIR="/home/samza/workspace/flink-extended/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-testbed"

# clean kafka related data
function cleanEnv() {
    rm -rf /tmp/flink*
    export JAVA_HOME=/home/samza/kit/jdk
    ~/samza-hello-samza/bin/grid stop kafka
    ~/samza-hello-samza/bin/grid stop zookeeper
    kill -9 $(jps | grep kafka | awk '{print $1}')
    rm -r /data/kafka/kafka-logs/
    rm -r /tmp/zookeeper/

    python -c 'import time; time.sleep(20)'

    ~/samza-hello-samza/bin/grid start zookeeper
    ~/samza-hello-samza/bin/grid start kafka

    python -c 'import time; time.sleep(5)'
}

# configure parameters in flink bin
function configFlink() {
    # set user requirement
    sed 's/^\(\s*streamswitch.requirement.latency\s*:\s*\).*/\1'"$L"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
    sed 's/^\(\s*streamswitch.system.l_low\s*:\s*\).*/\1'"$l_low"'/' tmp > tmp2
    sed 's/^\(\s*streamswitch.system.l_high\s*:\s*\).*/\1'"$l_high"'/' tmp2 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp tmp2

    # set static or streamswitch
    if [[ ${isTreat} == 1 ]]
    then
        sed 's/^\(\s*streamswitch.system.is_treat\s*:\s*\).*/\1'true'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
        mv tmp ${FLINK_DIR}/conf/flink-conf.yaml
    elif [[ ${isTreat} == 0 ]]
    then
        sed 's/^\(\s*streamswitch.system.is_treat\s*:\s*\).*/\1'false'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
        mv tmp ${FLINK_DIR}/conf/flink-conf.yaml
    fi
}

# run flink clsuter
function runFlink() {
    if [[ -d ${FLINK_DIR}/log ]]; then
        rm -rf ${FLINK_DIR}/log
    fi
    mkdir ${FLINK_DIR}/log
    ${FLINK_DIR}/bin/start-cluster.sh
}

# run applications
function runApp() {
    ${FLINK_APP_DIR}/submit-nexmark5.sh ${N} 64 ${RATE} ${CYCLE} ${BASE} ${WARMUP} ${Psource} 0
}

# clsoe flink clsuter
function closeFlink() {
    echo "experiment finished closing it"
    ${FLINK_DIR}/bin/stop-cluster.sh
    if [[ -d ${FLINK_APP_DIR}/nexmark_scripts/draw/logs/${EXP_NAME} ]]; then
        rm -rf ${FLINK_APP_DIR}/nexmark_scripts/draw/logs/${EXP_NAME}
    fi
    mv ${FLINK_DIR}/log ${FLINK_APP_DIR}/nexmark_scripts/draw/logs/${EXP_NAME}
    echo "close finished"
}

# draw figures
function draw() {
    python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/ViolationsAndUsageFromGroundTruth.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
}

# set in Flink
L=1000
l_low=20
l_high=20
isTreat=1

# only used in script
QUERY=5
RUNTIME=600

# set in Flink app
RATE=0
CYCLE=60
N=6
AVGRATE=200000
#RATE=100000
WARMUP=100
Psource=5

for RATE in 50000; do # 50000 100000
    for CYCLE in 60; do # 60 120 180 240 300
        for isTreat in 0 1; do
            BASE=`expr ${AVGRATE} - ${RATE}`
            EXP_NAME=Q${QUERY}-B${BASE}C${CYCLE}R${RATE}-Ns${Psource}-N${N}-L${L}llow${l_low}lhigh${l_high}-T${isTreat}
            echo ${EXP_NAME}

            cleanEnv
            configFlink
            runFlink
            runApp

            python -c 'import time; time.sleep('"${RUNTIME}"')'

            # draw figure
            draw
            closeFlink

        #    python -c 'import time; time.sleep(30)'
        done
    done
done
