#!/bin/bash

FLINK_DIR="/home/samza/workspace/flink-related/flink-extended-heter/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-related/flink-testbed-heter"

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
    sed 's/^\(\s*streamswitch.requirement.latency\s*:\s*\).*/\1'"$L"'/' ${FLINK_DIR}/conf/flink-conf.yaml > tmp1
    sed 's/^\(\s*streamswitch.system.l_low\s*:\s*\).*/\1'"$l_low"'/' tmp1 > tmp2
    sed 's/^\(\s*streamswitch.system.l_high\s*:\s*\).*/\1'"$l_high"'/' tmp2 > tmp3
    sed 's/^\(\s*task.good.delay\s*:\s*\).*/\1'"$delayGood"'/' tmp3 > tmp4
    sed 's/^\(\s*task.bad.delay\s*:\s*\).*/\1'"$delayBad"'/' tmp4 > tmp5
    sed 's/^\(\s*task.good.ratio\s*:\s*\).*/\1'"$ratioGood"'/' tmp5 > tmp6
    sed 's/^\(\s*task.bad.ratio\s*:\s*\).*/\1'"$ratioBad"'/' tmp6 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp1 tmp2 tmp3 tmp4 tmp5 tmp6

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
    ${FLINK_APP_DIR}/submit-nexmark2.sh ${N} 128 ${RATE} ${CYCLE} ${BASE} ${WARMUP} ${TUPLESIZE} ${Psource} 1
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
    python2 ${FLINK_APP_DIR}/nexmark_scripts/draw/ViolationsAndUsageFromGroundTruth.py ${EXP_NAME} ${WARMUP} ${RUNTIME}
    java Solution ${EXP_NAME} > ./figures/${EXP_NAME}/migrationtime.txt
}

# set in Flink
L=1000
l_low=100
l_high=100
isTreat=1

delayGood=240000
delayBad=480000
ratioGood=1
ratioBad=4
ratio=5
# only used in script
QUERY=2
SUMRUNTIME=3970

# set in Flink app
RATE=2000
CYCLE=600
N=32
AVGRATE=6000
#RATE=100000
WARMUP=300
TUPLESIZE=100
Psource=5
repeat=1


#for RATE in 5000 10000; do # 50000 100000
#for RATE in 0; do # 0 5000 10000 15000 20000 25000 30000
    for RATE in 2000; do # 60 75 90 105 120
        for repeat in 2 3 4 5 6 7 8 9 10; do # only used for repeat exps, no other usage
            for ratioBad in 4 3; do # only used for repeat exps, no other usage
            ratioGood=`expr ${ratio} - ${ratioBad}`
            BASE=`expr ${AVGRATE} - ${RATE}`
            RUNTIME=`expr ${SUMRUNTIME} - ${WARMUP} - 10`
            EXP_NAME=Q${QUERY}-B${BASE}C${CYCLE}R${RATE}-N${N}-L${L}llow${l_low}lhigh${l_high}-D${delayBad}-${ratioGood}To${ratioBad}-T${isTreat}-${repeat}
            echo ${EXP_NAME}

            cleanEnv
            configFlink
            runFlink
            runApp

            python -c 'import time; time.sleep('"${SUMRUNTIME}"')'

            #draw figure
            closeFlink
            draw

            ~/samza-hello-samza/deploy/kafka/bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic flink_metrics --from-beginning > ./nexmark_scripts/draw/logs/${EXP_NAME}/metrics &
            python -c 'import time; time.sleep(30)'
            kill -9 $(jps | grep ConsoleConsumer | awk '{print $1}')
        done
    done
done
