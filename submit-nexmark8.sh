echo "compiling is: ${10}"
iscompile=${10}

if [ $iscompile == 1 ]
then
    mvn clean package
fi

KAFKA_PATH="/home/samza/samza-hello-samza/deploy/kafka/bin"


$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics
$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_keygroups_status
#$KAFKA_PATH/kafka-topics.sh --create --zookeeper camel:2181 -flink_metricsk_metrics --partitions 1 --replication-factor 1
/home/samza/workspace/flink-related/flink-extended-nexmark/build-target/bin/flink run target/testbed-1.0-SNAPSHOT.jar -p2 $1 -mp2 $2 \
 -auction-srcRate $3 -auction-srcCycle $4 -auction-srcBase $5 -person-srcRate $3 -person-srcCycle $4 -person-srcBase $5 \
  -srcWarmUp $6 -srcTupleSize $7 -p-source $8 -window-size $9 &
