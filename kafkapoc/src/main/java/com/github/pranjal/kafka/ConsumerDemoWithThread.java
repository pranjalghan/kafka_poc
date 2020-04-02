package com.github.pranjal.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
       new ConsumerDemoWithThread().run();
    }
    private ConsumerDemoWithThread(){

    }
    private void run(){
        Logger logger=LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String bootstrapServer ="127.0.0.1:9092";
        String groupId="my-fourth-application";
        String topic="first_topic";
        //latch for dealing with multiple threads
        CountDownLatch latch=new CountDownLatch(1);
        //create a consumer runnable
        Runnable myConsumerThread=new ConsumerThread(latch,topic,bootstrapServer,groupId);
        //start the thread
        Thread myThread=new Thread(myConsumerThread);
        myThread.start();
        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interupted");
            e.printStackTrace();
        }finally {
            logger.info("Application is closing");
        }
    }
    public class ConsumerThread implements Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        //to deal with concurrency
        public ConsumerThread(CountDownLatch latch,String topic,String bootstrapServer,String groupId ){
            this.latch=latch;
            //properties for the consumer
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create a Consumer
            consumer= new KafkaConsumer<String, String>(properties);
            //subscribe consumer to our topics
            //to subscribe to more than one topic we use Array.asList("","");
            consumer.subscribe(Collections.singleton(topic));
            //poll for new data
        }
        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key :" + record.key() + "\n"
                                + "Value :" + record.value() + "\n" +
                                "partition" + record.partition() + "\n"
                                + "offset :" + record.offset());
                        //we see all the data of the partition 0 then 1 and then 2 of first_topic.
                    }
                }
            }
            catch (WakeupException e){
                logger.info("received shutdown signal");
            }
            finally {
                consumer.close();
                //tell our main code we are done with consumer
                latch.countDown();
            }
        }
        public void shutdown(){
            //the wakeup() method is a special method to interrupt consumer.poll()
            //it will throw an exception called WakeupException
            consumer.wakeup();
        }
    }
}
