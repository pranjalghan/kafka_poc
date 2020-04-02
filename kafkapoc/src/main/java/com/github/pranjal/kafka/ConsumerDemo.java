package com.github.pranjal.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        //Logger for the consumer
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());
        //properties for the consumer
        String bootstrapServer ="127.0.0.1:9092";
        String groupId="my-third-application";
        String topic="first_topic";

        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create a Consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        //subscribe consumer to our topics
        //to subscribe to more than one topic we use Array.asList("","");
        consumer.subscribe(Collections.singleton(topic));
        //poll for new data
        while(true)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:records) {
                logger.info("Key :" +record.key() +"\n"
                +"Value :" +record.value() +"\n"+
                "partition" +record.partition() +"\n"
                +"offset :"+record.offset());
                //we see all the data of the partition 0 then 1 and then 2 of first_topic.
            }
        }
    }
}
