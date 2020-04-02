package com.github.pranjal.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallback {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //WE CREATED A LOGGER FOR OUR CLASS
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //Create the  producer properties
        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        //NOW TO SEE WHICH PROPERTIES TO ADD ->GO TO KAFKA DOCUMENTATION
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 10; i++) {
            String topic="first_topic";
            String value="hello world" +Integer.toString(i);
            String key ="id :" +Integer.toString(i);
            //Create the record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value);
            logger.info("key:" +key);
           // ID:0 is going to partition 2
            //id:1 partition 2
            //id:2 partition 0
            //id :3 partition 2
            //id :4 partition 2
            //id: 5 partition 0 and so on
            //send the data--asynchronous(happens at the background,and the data is never send)
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("received new metadata \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                        //the record was successfully sent
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block send to make it synchronous
        }
            //flush data
            producer.flush();
            //flush and close producer
            producer.close();
        }
    }
