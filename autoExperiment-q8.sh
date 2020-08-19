#!/bin/bash

FLINK_DIR="/home/samza/workspace/flink-related/flink-extended-nexmark/build-target"
FLINK_APP_DIR="/home/samza/workspace/flink-related/flink-testbed-nexmark"

# clean kafka related data
function cleanEnv() {
    rm -rf /tmp/flink*
    export JAVA_HOME=/home/samza/kit/jdk
    ~/samza-hello-samza/bin/grid stop kafka
    ~/samza-hello-samza/bin/grid stop zookeeper
    kill -9 $(jps | grep Kafka | awk '{print $1}')
    kill -9 $(jps | grep TaskManagerRunner | awk '{print $1}')
    rm -r /tmp/kafka-logs/
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
    sed 's/^\(\s*streamswitch.system.l_high\s*:\s*\).*/\1'"$l_high"'/' tmp2 > tmp3
    sed 's/^\(\s*policy.windowSize\s*:\s*\).*/\1'"$metricsIntervalNs"'/' tmp3 > tmp4
    sed 's/^\(\s*streamswitch.system.metrics_interval\s*:\s*\).*/\1'"$metricsIntervalMs"'/' tmp4 > tmp5
    sed 's/^\(\s*migration.delay\s*:\s*\).*/\1'"$delay"'/' tmp5 > ${FLINK_DIR}/conf/flink-conf.yaml
    rm tmp tmp2 tmp3 tmp4 tmp5

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
    ${FLINK_APP_DIR}/submit-nexmark${QUERY}.sh ${N} 64 ${RATE} ${CYCLE} ${BASE} ${WARMUP} ${TUPLESIZE} ${Psource} ${Window} 1
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
}

# set in Flink
L=1000

l=100
l_low=100
l_high=100

interval=100
metricsIntervalMs=100
metricsIntervalNs=100000000

isTreat=1
delay=0

# only used in script
QUERY=8
SUMRUNTIME=730

# set in Flink app
RATE=0
CYCLE=90
N=10
AVGRATE=30000
#RATE=100000
WARMUP=60
TUPLESIZE=100
Psource=1
repeat=1
Window=1
delay=0

#for RATE in 5000 10000; do # 50000 100000
for RATE in 20000 30000 10000; do # 0 5000 10000 15000 20000 25000 30000
   for CYCLE in 90 60 120; do 
        for AVGRATE in 30000; do # only used for repeat exps, no other usage
            BASE=`expr ${AVGRATE} - ${RATE}`
            RUNTIME=`expr ${SUMRUNTIME} - ${WARMUP} - 10`
            EXP_NAME=Q${QUERY}-B${BASE}C${CYCLE}R${RATE}-N${N}-L${L}llow${l_low}lhigh${l_high}-D${delay}-T${isTreat}-W${Window}-${repeat}
            #EXP_NAME=Q${QUERY}-B${BASE}C${CYCLE}R${RATE}-Ns${Psource}-N${N}-L${L}llow${l_low}lhigh${l_high}-T${isTreat}-R${RUNTIME}-W${Window}-${repeat}
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

# #for RATE in 5000 10000; do # 50000 100000
# for l in 20 50 100 200 500; do # 0 5000 10000 15000 20000 25000 30000
#    for N in 5; do 
#         for RATE in 10000; do # only used for repeat exps, no other usage
#             l_high=${l}
#             l_low=${l}
#             BASE=`expr ${AVGRATE} - ${RATE}`
#             RUNTIME=`expr ${SUMRUNTIME} - ${WARMUP} - 10`
#             EXP_NAME=Q${QUERY}-B${BASE}C${CYCLE}R${RATE}-N${N}-L${L}-l${l}-i${interval}-T${isTreat}-${repeat}
#             #EXP_NAME=Q${QUERY}-B${BASE}C${CYCLE}R${RATE}-Ns${Psource}-N${N}-L${L}llow${l_low}lhigh${l_high}-T${isTreat}-R${RUNTIME}-W${Window}-${repeat}
#             echo ${EXP_NAME}

#             cleanEnv
#             configFlink
#             runFlink
#             runApp

#             python -c 'import time; time.sleep('"${SUMRUNTIME}"')'

#             #draw figure
#             closeFlink
#             draw

#             ~/samza-hello-samza/deploy/kafka/bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic flink_metrics --from-beginning > ./nexmark_scripts/draw/logs/${EXP_NAME}/metrics &
#             python -c 'import time; time.sleep(30)'
#             kill -9 $(jps | grep ConsoleConsumer | awk '{print $1}')
#         done
#     done
# done

# reset l
# l=100

# #for RATE in 5000 10000; do # 50000 100000
# for interval in 20 50 100 200 500; do # 0 5000 10000 15000 20000 25000 30000
#    for N in 5; do 
#         for RATE in 10000; do # only used for repeat exps, no other usage
#             metricsIntervalMs=${interval}
#             metricsIntervalNs=$((interval*1000000))
#             BASE=`expr ${AVGRATE} - ${RATE}`
#             RUNTIME=`expr ${SUMRUNTIME} - ${WARMUP} - 10`
#             EXP_NAME=Q${QUERY}-B${BASE}C${CYCLE}R${RATE}-N${N}-L${L}-l${l}-i${interval}-T${isTreat}-${repeat}
#             #EXP_NAME=Q${QUERY}-B${BASE}C${CYCLE}R${RATE}-Ns${Psource}-N${N}-L${L}llow${l_low}lhigh${l_high}-T${isTreat}-R${RUNTIME}-W${Window}-${repeat}
#             echo ${EXP_NAME}

#             cleanEnv
#             configFlink
#             runFlink
#             runApp

#             python -c 'import time; time.sleep('"${SUMRUNTIME}"')'

#             #draw figure
#             closeFlink
#             draw

#             ~/samza-hello-samza/deploy/kafka/bin/kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic flink_metrics --from-beginning > ./nexmark_scripts/draw/logs/${EXP_NAME}/metrics &
#             python -c 'import time; time.sleep(30)'
#             kill -9 $(jps | grep ConsoleConsumer | awk '{print $1}')
#         done
#     done
# done
