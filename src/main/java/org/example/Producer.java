package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    // Topic where the messages are sent
    public static final  String MAIN_TOPIC = "compras.do.cliente";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // FIrst we need to create a producer kafka responsible for producer new messages
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties());
        // Here is my class responsible for recording of the messages (TOPIC, KEY (CLIENT CONSUMER), VALUE )
        ProducerRecord<String, String> record = new ProducerRecord<>(MAIN_TOPIC, "cliente-1", "compras:50reais");
        // This callback is used to check if an error occurred in sending and see values of partition and offset
        Callback callback  = (data, error) -> {
            if (error != null) {
                error.printStackTrace();
                return;
            }
            System.out.println("mensagem publicada com sucesso");
            System.out.println(data.partition());
            System.out.println(data.offset());
        };

        // Sending the message with callback
        producer.send(record, callback).get();
    }

    // Properties responsible for configure the producer
    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        return properties;
    }
}