package ca.uwaterloo.cs451.project;

import org.apache.kafka.clients.producer.*;

/*
Apache Kafka is a distributed streaming platform that is used for building real-time data pipelines and
streaming apps.
* */

public class kafkaProducer {

    public static void main(String[] args) throws Exception{

        // Set the properties for the producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create the producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Send a simple message
        producer.send(new ProducerRecord<String, String>("some topic", "Hello, World!"));

        // Close the producer
        producer.close();
    }
}

