package com.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

/*
I/o: user id, color, color should be only green,red,blue. o/p count fav colors. user,color can change

 */
public class FavouriteColourApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-java");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        // Step 1: We create the topic of users keys to colours
        KStream<String,String> lines = builder.stream("fav-color-input");

        KStream<String,String> userAndColors = lines
                // 1 - we ensure that a comma is here as we will split on it
                .filter((key,val) -> val.contains(","))
        // 2 - we select a key that will be the user id (lowercase for safety)
                .selectKey((key, val) -> val.split(",")[0].toLowerCase())
        // 3 - we get the colour from the value (lowercase for safety)
                .mapValues((values) -> values.split(",")[1].toLowerCase())
        // 4 - we filter undesired colours (could be a data sanitization step
            .filter((user,val) -> Arrays.asList("green","red","blue").contains(val));
        userAndColors.to("user-keys-colors");

        // step 7 - we read that topic as a KTable so that updates are read correctly
        KTable<String,String> userKeysAndColorsTable = builder.table("user-keys-colors");

        // step 8 - we count the occurences of colours
        KTable<String,Long> favColors = userKeysAndColorsTable
        // 9 - we group by colour within the KTable
        .groupBy((user,color) -> new KeyValue<>(color,color))
                .count(Materialized.as("CountByColors"));;
        favColors.toStream().to("fav-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
