iscompile=$2

if [ $iscompile == 1 ]
then
    mvn clean package
fi

~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper camel:2181 --topic flink_metrics
/home/samza/workspace/flink-extended/build-target/bin/flink run target/testbed-1.0-SNAPSHOT.jar -p2 $1 &

python -c 'import time; time.sleep(20)'

./generate.sh camel:9092
