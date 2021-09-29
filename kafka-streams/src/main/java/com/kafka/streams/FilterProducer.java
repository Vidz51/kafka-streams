package com.kafka.streams.exactlyonce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FilterProducer {
    public static void main(String[] args) {
        FilterProducer pr = new FilterProducer();
        String topic = "partial_location_queue33";
        String key = "CTKRA";
        String key1 = "CTKRA";

        String ugc1 = "";
        String pac = ""
        for(int i =0;i<10;i++){
            pr.produceKafkaMsg(topic, pac, key);
            pr.produceKafkaMsg(topic, pac, key1);
        }
        pr.closeKafkaProducer();
    }
    KafkaProducer<String, byte[]> stringKafkaProducer;
    public FilterProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.44.32:9099");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        stringKafkaProducer = new KafkaProducer<>(config);

    }
    public void produceKafkaMsg(String topic, String message, String key) {
        byte[] messageBytes = message.getBytes();
        try {
            ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, key, messageBytes);
            stringKafkaProducer.send(record);
            System.out.println("packet sedn");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void closeKafkaProducer() {
        stringKafkaProducer.close();
    }
    public void produceKafkaMsgs(String topic, ArrayList<String> kafkaPackets) {
        kafkaPackets.forEach(packet->{
            produceKafkaMsg(topic, packet, String.valueOf(packet.hashCode()));
        });
    }
    public void produceKafkaMsgs(String topic, List<String> kafkaPackets, String key) {
        kafkaPackets.forEach(packet->{
            produceKafkaMsg(topic, packet, key);
        });
        closeKafkaProducer();
    }
}

