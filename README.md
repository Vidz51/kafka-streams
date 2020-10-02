# kafka-streams
 kafka streams examples

# favourite color counts
 ##Create topics
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic fav-color-input
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-keys-colors --config cleanup.policy=compact
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic fav-color-output --config cleanup.policy=compact
	##producer
	bin/kafka-console-producer.sh --broker-list localhost:9092 --topic fav-color-input
 ##consumer
	bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic fav-color-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
