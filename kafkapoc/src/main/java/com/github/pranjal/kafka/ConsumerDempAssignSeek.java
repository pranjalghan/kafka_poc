package com.github.pranjal.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDempAssignSeek {
    public static void main(String[] args) {
        //Logger for the consumer
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());
        //properties for the consumer
        String bootstrapServer ="127.0.0.1:9092";
        String topic="first_topic";
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //create a Consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
       //assign and seek are 2 different kinds of api used to replay data or fetch a specific message
        //assign--assign to a specific partition
        TopicPartition topicPartition=new TopicPartition(topic,0);
        long offsetToReadFrom=15L;
        consumer.assign(Arrays.asList(topicPartition));
        //seek--seek from a specific offset
        consumer.seek(topicPartition,offsetToReadFrom);
        int numberOfMessagesToRead=5;
        boolean keepOnReading=true;
        int numberOfMessagesReadSoFar=0;
        //poll for new data
        while(keepOnReading)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:records) {
                numberOfMessagesReadSoFar +=1;
                logger.info("Key :" +record.key() +"\n"
                        +"Value :" +record.value() +"\n"+
                        "partition" +record.partition() +"\n"
                        +"offset :"+record.offset());
                if(numberOfMessagesReadSoFar>=numberOfMessagesToRead)
                {
                    keepOnReading=false;//to exit
                    break;//to exit the for loop
                }
                //we see all the data of the partition 0 then 1 and then 2 of first_topic.
            }
        }
        logger.info("exiting the application");
    }
}
