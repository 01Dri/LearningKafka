package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {

        // First we need a kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties());

        // THe method "subscribe" of consumer receive a list of topics, how we have only one topic then the list will be singleton (only one element)
        consumer.subscribe(Collections.singletonList(Producer.MAIN_TOPIC));
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100)); // THe poll method search all registry of topic and return in 100 milliseconds
            // Showing all values of registry
            for (var record: records) {
                System.out.println("COMPRA NOVA: ");
                System.out.println(record.key());
                System.out.println(record.value());
                System.out.println(record.offset());
                System.out.println(record.partition());
            }
        }
    }
    // Properties responsible for configure the consumer
    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumo-cliente");
        return properties;
    }
}
