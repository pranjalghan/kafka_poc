package com.github.pranjal.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
       //Create the  producer properties
        Properties properties=new Properties();
        String bootstrapServer="127.0.0.1:9092";
        //NOW TO SEE WHICH PROPERTIES TO ADD ->GO TO KAFKA DOCUMENTATION
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        //Create the record
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","hello world");
        //send the data--asynchronous(happens at the background,and the data is never send)
        producer.send(record);
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
