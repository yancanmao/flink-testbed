#!/bin/bash

FLINK_DIR="/home/samza/workspace/flink-extended/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-testbed"

# clean kafka related data
function cleanEnv() {
    rm -rf /tmp/flink*
    export JAVA_HOME=/home/samza/kit/jdk
    ~/samza-hello-samza/bin/grid stop kafka
    ~/samza-hello-samza/bin/grid stop zookeeper
    kill -9 $(jps | grep Kafka | awk '{print $1}')
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
    sed 's/^\(\s*streamswitch.requirement.latency.c21234bcbf1e8eb4c61f1927190efebd\s*:\s*\).*/\1'"$L1"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp
    sed 's/^\(\s*streamswitch.requirement.latency.b71731f1c0df9c3076c4a455334d0ad6\s*:\s*\).*/\1'"$L2"'/' tmp > tmp1
    sed 's/^\(\s*streamswitch.system.l_low\s*:\s*\).*/\1'"$l_low"'/' tmp1 > tmp2
    sed 's/^\(\s*streamswitch.system.l_high\s*:\s*\).*/\1'"$l_high"'/' tmp2 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp tmp1 tmp2

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
    #${FLINK_APP_DIR}/submit-wc.sh ${N} ${N} 128 ${RATE} ${CYCLE} ${BASE} ${WARMUP} ${Psource} 1
    ${FLINK_APP_DIR}/submit-wc.sh ${N1} ${N2} 128 ${RATE} ${CYCLE} ${BASE} ${WARMUP} ${Psource} 1
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
    #python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/RateAndWindowDelay.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/ViolationsAndUsageFromGroundTruth.py ${EXP_NAME} ${WARMUP} ${RUNTIME} ${L1} ${L2}
}

# set in Flink
L=2000
L1=1000
L2=1000
l_low=100
l_high=100
isTreat=1

# only used in script
SUMRUNTIME=730

# set in Flink app
RATE=0
CYCLE=60
N1=10
N2=20
AVGRATE=3333
#RATE=100000
WARMUP=60
Psource=5
repeat=1

operator1=c21234bcbf1e8eb4c61f1927190efebd
operator2=b71731f1c0df9c3076c4a455334d0ad6

for RATE in 833 667; do # 50000 100000
#for RATE in 50000 100000 150000; do # 0 5000 10000 15000 20000 25000 30000
    for L1 in 500 1000 1500; do # 60 75 90 105 120
        for repeat in 1 2 3; do
        for CYCLE in 300 600; do # only used for repeat exps, no other usage
            L2=`expr ${L} - ${L1}`
            BASE=`expr ${AVGRATE} - ${RATE}`
            RUNTIME=`expr ${SUMRUNTIME} - ${WARMUP} - 10`
            EXP_NAME=WC-B${BASE}C${CYCLE}R${RATE}-Ns${Psource}-N1${N1}N2${N2}-L1${L1}L2${L2}-T${isTreat}-R${RUNTIME}-${repeat}
            echo ${EXP_NAME}

            cleanEnv
            configFlink
            runFlink
            runApp

            python -c 'import time; time.sleep('"${SUMRUNTIME}"')'

            # draw figure
            closeFlink
            draw

            ~/samza-hello-samza/deploy/kafka/bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic flink_metrics --from-beginning > ./nexmark_scripts/draw/logs/${EXP_NAME}/metrics &
            python -c 'import time; time.sleep(30)'
            kill -9 $(jps | grep ConsoleConsumer | awk '{print $1}')
        done
        done
    done
done

L=1000

for RATE in 833 667; do # 50000 100000
#for RATE in 50000 100000 150000; do # 0 5000 10000 15000 20000 25000 30000
    for L1 in 250 500 750; do # 60 75 90 105 120
        for repeat in 1 2 3; do
        for CYCLE in 300 600; do # only used for repeat exps, no other usage
            L2=`expr ${L} - ${L1}`
            BASE=`expr ${AVGRATE} - ${RATE}`
            RUNTIME=`expr ${SUMRUNTIME} - ${WARMUP} - 10`
            EXP_NAME=WC-B${BASE}C${CYCLE}R${RATE}-Ns${Psource}-N1${N1}N2${N2}-L1${L1}L2${L2}-T${isTreat}-R${RUNTIME}-${repeat}
            echo ${EXP_NAME}

            cleanEnv
            configFlink
            runFlink
            runApp

            python -c 'import time; time.sleep('"${SUMRUNTIME}"')'

            # draw figure
            closeFlink
            draw

            ~/samza-hello-samza/deploy/kafka/bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic flink_metrics --from-beginning > ./nexmark_scripts/draw/logs/${EXP_NAME}/metrics &
            python -c 'import time; time.sleep(30)'
            kill -9 $(jps | grep ConsoleConsumer | awk '{print $1}')
        done
        done
    done
done
