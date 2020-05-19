iscompile=$9

if [ $iscompile == 1 ]
then
    mvn clean package
fi

KAFKA_PATH="/home/samza/samza-hello-samza/deploy/kafka/bin"


$KAFKA_PATH/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_metrics
$KAFKA_PATH/kafka-topics.sh --create --zookeeper localhost:2181 --topic flink_metrics --partitions 1 --replication-factor 1
/home/samza/workspace/flink-related/flink-extended-heter/build-target/bin/flink run target/testbed-1.0-SNAPSHOT.jar -p2 $1 -mp2 $2 \
 -srcRate $3 -srcCycle $4 -srcBase $5 -srcWarmUp $6 -srcTupleSize $7 -p-source $8&
